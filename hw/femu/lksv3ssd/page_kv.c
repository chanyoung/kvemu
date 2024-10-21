#include "hw/femu/kvssd/lksv/lksv3_ftl.h"

static void gc_read_delay(struct ssd *ssd, struct femu_ppa *ppa)
{
    if (ssd->sp.enable_gc_delay) {
        struct nand_cmd gcr;
        gcr.type = GC_IO;
        gcr.cmd = NAND_READ;
        gcr.stime = 0;
        lksv3_ssd_advance_status(ssd, ppa, &gcr);
    }
}

static void gc_write_delay(struct ssd *ssd, struct femu_ppa *ppa)
{
    if (ssd->sp.enable_gc_delay) {
        struct nand_cmd gcw;
        gcw.type = GC_IO;
        gcw.cmd = NAND_WRITE;
        gcw.stime = 0;
        lksv3_ssd_advance_status(ssd, ppa, &gcw);
    }
}

void gc_erase_delay(struct ssd *ssd, struct femu_ppa *ppa)
{
    if (ssd->sp.enable_gc_delay) {
        struct nand_cmd gce;
        gce.type = GC_IO;
        gce.cmd = NAND_ERASE;
        gce.stime = 0;
        lksv3_ssd_advance_status(ssd, ppa, &gce);
    }
}

static void lksv3_line_erase(struct ssd *ssd, int lineid) {
    struct line *line = &ssd->lm.lines[lineid];
    struct femu_ppa ppa;
    qemu_mutex_lock(&ssd->comp_mu);

    kv_log("%d gc_data!!! (line: %d)\n", ++lksv_lsm->data_gc_cnt, line->id);
    kv_log("vpc: %d, valid bytes: %d, invalid_bytes: %d, secs_per_line: %d\n", line->vpc, line->vsc, line->isc, ssd->sp.secs_per_line);
    kv_log("free line left: %d\n", ssd->lm.data.free_line_cnt);

    ppa.ppa = 0;
    ppa.g.blk = lineid;
    for (int ch = 0; ch < ssd->sp.nchs; ch++) {
        for (int lun = 0; lun < ssd->sp.luns_per_ch; lun++) {
            qemu_mutex_unlock(&ssd->comp_mu);
            wait_pending_reads(ssd);
            qemu_mutex_lock(&ssd->comp_mu);

            ppa.g.ch = ch;
            ppa.g.lun = lun;
            ppa.g.pl = 0;
            ppa.g.sec = 0;
            ppa.g.rsv = 0;

            gc_erase_delay(ssd, &ppa);
            lksv3_mark_block_free(ssd, &ppa);
        }
    }

    int cnt = 0;
    for (int i = 0; i < 4; i++) {
        if (!per_line_data(line)->referenced_levels[i]) {
            kv_assert(!lksv_lsm->disk[i]->reference_lines[lineid]);
            continue;
        }
        kv_assert(lksv_lsm->disk[i]->reference_lines[lineid]);
        lksv_lsm->disk[i]->reference_lines[lineid] = false;
        per_line_data(line)->referenced_levels[i] = false;
        cnt++;
    }
    kv_assert(cnt <= 2);

    lksv3_mark_line_free(ssd, &ppa);
    qemu_mutex_unlock(&ssd->comp_mu);
}

// lksv_gc_data_early performs a line erase on lines that contain only values
// preceding the given key. This helps in reclaiming free log lines early
// during the process of erasing large log regions, making it easier for
// compacted values to re-write the log area.
void lksv_gc_data_early(struct ssd *ssd, int ulevel, int level, kv_key k) {
    kv_assert(level > 0);
    int planned = lksv_lsm->gc_planned;
    for (int i = 0; i < 512; i++) {
        if (lksv_lsm->gc_plan[level][i] || lksv_lsm->gc_plan[ulevel][i]) {
            struct line *line = &ssd->lm.lines[i];
            if (kv_cmp_key(per_line_data(line)->sg.ekey, k) < 0) {
                check_473(ssd);
                lksv3_line_erase(ssd, i);
                check_473(ssd);
                lksv_lsm->gc_plan[level][i] = false;
                lksv_lsm->gc_plan[ulevel][i] = false;
                lksv_lsm->gc_planned--;
            }
        }
    }
    if (planned != lksv_lsm->gc_planned) {
        kv_log("decrease gc_planned from %d to %d\n", planned, lksv_lsm->gc_planned);
    }
}

void lksv3_gc_data_femu3(struct ssd *ssd, int ulevel, int level) {
    kv_assert(level > 0);
    for (int i = 0; i < 512; i++) {
        if (lksv_lsm->gc_plan[level][i] || lksv_lsm->gc_plan[ulevel][i]) {
            check_473(ssd);
            lksv3_line_erase(ssd, i);
            check_473(ssd);
        }
    }
    memset(lksv_lsm->gc_plan[level], 0, sizeof(bool) * 512);
    memset(lksv_lsm->gc_plan[ulevel], 0, sizeof(bool) * 512);
}

static lksv3_run_t *_lksv3_gc_meta_find_run(struct ssd *ssd, struct femu_ppa *ppa)
{
    struct nand_page *pg = lksv3_get_pg(ssd, ppa);
    kv_key first_key_in_meta;
    lksv3_sst_t sst = {.meta = NULL};

    lksv3_sst_decode(&sst, pg->data);
    first_key_in_meta.len = sst.meta[0].g1.klen;
    first_key_in_meta.key = pg->data + sst.meta[0].g1.off;

    lksv3_run_t *entries = NULL;
    lksv3_run_t *target_entry = NULL;
    bool checkdone = false;

    for(int j = 0; j < LSM_LEVELN; j++) {
        entries = lksv3_find_run(lksv_lsm->disk[j], first_key_in_meta, ssd, NULL);
        if (entries == NULL) {
            continue;
        }
        // TODO: we need to use footer.g.skey.
        //if (entries->ppa.ppa == ppa->ppa && kv_test_key(entries->key, first_key_in_meta)) {
        if (entries->ppa.ppa == ppa->ppa) {
            checkdone = true;
            target_entry = entries;
            break;
        }
        if (checkdone)
            break;
    }

    if (!checkdone && lksv_lsm->c_level) {
        entries = lksv3_find_run(lksv_lsm->c_level, first_key_in_meta, ssd, NULL);
        // TODO: we need to use footer.g.skey.
        //if (entries && entries->ppa.ppa == ppa->ppa) {
        if (entries && entries->ppa.ppa == ppa->ppa) {
            checkdone = true;
            target_entry = entries;
        }
    }

    if (!checkdone) {
        for (int j = 0; j < LSM_LEVELN; j++) {
            entries = lksv3_find_run_slow(lksv_lsm->disk[j], first_key_in_meta, ssd);
            if (entries == NULL) {
                continue;
            }
            // TODO: we need to use footer.g.skey.
            //if (entries->ppa.ppa == ppa->ppa && kv_test_key(entries->key, first_key_in_meta)) {
            if (entries->ppa.ppa == ppa->ppa) {
                checkdone = true;
                target_entry = entries;
                break;
            }
            if (checkdone)
                break;
        }
    }

    if (!checkdone && lksv_lsm->c_level) {
        entries = lksv3_find_run_slow(lksv_lsm->c_level, first_key_in_meta, ssd);
        // TODO: we need to use footer.g.skey.
        //if (entries && entries->ppa.ppa == ppa->ppa && kv_test_key(entries->key, first_key_in_meta)) {
        if (entries && entries->ppa.ppa == ppa->ppa) {
            checkdone = true;
            target_entry = entries;
        }
    }

    if (!checkdone) {
        for (int j = 0; j < LSM_LEVELN; j++) {
            entries = lksv3_find_run_slow_by_ppa(lksv_lsm->disk[j], ppa, ssd);
            if (entries == NULL) {
                continue;
            }
            // TODO: we need to use footer.g.skey.
            //if (entries->ppa.ppa == ppa->ppa && kv_test_key(entries->key, first_key_in_meta)) {
            if (entries->ppa.ppa == ppa->ppa) {
                checkdone = true;
                target_entry = entries;
                break;
            }
            if (checkdone)
                break;
        }
    }

    if (!checkdone && lksv_lsm->c_level) {
        entries = lksv3_find_run_slow_by_ppa(lksv_lsm->c_level, ppa, ssd);
        // TODO: we need to use footer.g.skey.
        //if (entries && entries->ppa.ppa == ppa->ppa && kv_test_key(entries->key, first_key_in_meta)) {
        if (entries && entries->ppa.ppa == ppa->ppa) {
            checkdone = true;
            target_entry = entries;
        }
    }

    static int64_t lost_cnt = 0;
    if (!checkdone) {
        // TODO: handle this
        // Lost mapping between run and meta segment
        kv_err("Lost mapping between run (PPA: %u) and meta segment (n: %ld)\n", ppa->ppa, ++lost_cnt);
        target_entry = NULL;
        abort();
    }

    FREE(sst.meta);

    return target_entry;
}

static struct femu_ppa get_next_ppa(struct ssd *ssd, struct femu_ppa ppa)
{
    struct ssdparams *spp = &ssd->sp;
    ppa.g.ch++;
    if (ppa.g.ch == spp->nchs) {
        ppa.g.ch = 0;
        ppa.g.lun++;
        if (ppa.g.lun == spp->luns_per_ch) {
            ppa.g.lun = 0;
            ppa.g.pg++;
            if (ppa.g.pg == spp->pgs_per_blk) {
                ppa.g.pg = 0;
                ppa.ppa = UNMAPPED_PPA;
            }
        }
    }
    return ppa;
}

static void _lksv3_gc_meta_femu(struct ssd *ssd, struct femu_ppa *old_ppa)
{
    static bool initialized = false;
    lksv3_run_t *run = NULL;
    static struct femu_ppa old_pivot;
    static struct femu_ppa new_pivot;
    struct femu_ppa new_ppa;
    struct nand_page *old_pg;
    struct nand_page *new_pg;
    static int idx = 0;
    static bool lost = false;
    static struct femu_ppa old_next;
    static struct femu_ppa new_next;

    if (!initialized) {
        old_pivot.ppa = UNMAPPED_PPA;
        new_pivot.ppa = UNMAPPED_PPA;
        old_next.ppa = UNMAPPED_PPA;
        new_next.ppa = UNMAPPED_PPA;
        initialized = true;
    }

    if (!old_ppa) {
        if (old_pivot.ppa != UNMAPPED_PPA) {
            if (!lost) {
                if (idx < PG_N) {
                    new_ppa = lksv3_get_new_meta_page(ssd);
                    while (!is_pivot_ppa(ssd, new_ppa)) {
                        lksv3_mark_page_valid(ssd, &new_ppa);
                        lksv3_ssd_advance_write_pointer(ssd, &ssd->lm.meta);
                        lksv3_mark_page_invalid(ssd, &new_ppa);
                        new_ppa = lksv3_get_new_meta_page(ssd);
                        idx++;
                    }
                }
                kv_assert(idx == PG_N);
            }
        }
        old_pivot.ppa = UNMAPPED_PPA;
        new_pivot.ppa = UNMAPPED_PPA;
        return;
    }

    if (old_pivot.ppa == UNMAPPED_PPA) {
        idx = 0;

        old_pivot = *old_ppa;
        kv_assert(is_pivot_ppa(ssd, old_pivot));

        run = _lksv3_gc_meta_find_run(ssd, old_ppa);
        if (!run) {
            lost = true;
        } else {
            new_pivot = lksv3_get_new_meta_page(ssd);
            kv_assert(is_pivot_ppa(ssd, new_pivot));

            new_ppa = new_pivot;

            run->ppa = new_pivot;
            lost = false;
        }
    } else {
        kv_assert(!is_pivot_ppa(ssd, *old_ppa));
        kv_assert(old_pivot.g.pg == old_ppa->g.pg);
        kv_assert(old_pivot.g.blk == old_ppa->g.blk);

        if (!lost) {
            new_ppa = lksv3_get_new_meta_page(ssd);
            kv_assert(!is_pivot_ppa(ssd, new_ppa));
            kv_assert(new_pivot.g.pg == new_ppa.g.pg);
            kv_assert(new_pivot.g.blk == new_ppa.g.blk);
        }

        kv_assert(old_ppa->ppa == old_next.ppa);
        if (!lost) {
            kv_assert(new_ppa.ppa == new_next.ppa);
        }
    }
    kv_assert(old_pivot.ppa != UNMAPPED_PPA);

    if (lost) {
        old_pg = lksv3_get_pg(ssd, old_ppa);
        kv_assert(old_pg->status == PG_VALID);
        gc_read_delay(ssd, old_ppa);
        FREE(old_pg->data);

        idx++;

        old_next = get_next_ppa(ssd, *old_ppa);
        return;
    }

    old_next = get_next_ppa(ssd, *old_ppa);
    new_next = get_next_ppa(ssd, new_ppa);

    old_pg = lksv3_get_pg(ssd, old_ppa);
    kv_assert(old_pg->status == PG_VALID);
    gc_read_delay(ssd, old_ppa);

    new_pg = lksv3_get_pg(ssd, &new_ppa);
    kv_assert(new_pg->status == PG_FREE);

    new_pg->data = old_pg->data;
    old_pg->data = NULL;

    lksv3_mark_page_valid2(ssd, &new_ppa);
    lksv3_ssd_advance_write_pointer(ssd, &ssd->lm.meta);
    gc_write_delay(ssd, &new_ppa);

    idx++;

    if (false) {
        kv_err("Unreachable: old_next: %p, new_next: %p\n", &old_next, &new_next);
    }
}

int lksv3_gc_meta_femu(struct ssd *ssd) {
    struct line *victim_line = NULL;
    struct ssdparams *spp = &ssd->sp;
    struct femu_ppa ppa;
    int ch, lun, pg_n;

    check_linecnt(ssd);
    victim_line = lksv3_select_victim_meta_line(ssd, false);
    if (victim_line == NULL) {
        if (lksv_lsm->should_d2m <= 0) {
            lksv_lsm->should_d2m = 1;
        }
        return -1;
    }
    kv_log("%d gc_meta! (line: %d) invalid_pgs / pgs_per_line: %d / %d, vpc: %d \n", ++lksv_lsm->header_gc_cnt, victim_line->id, victim_line->ipc, ssd->sp.pgs_per_line, victim_line->vpc);

    ppa.g.blk = victim_line->id;

    /* copy back valid data */
    struct nand_page *pg;
    int group_n = 0;
    ppa.g.pl = 0;
    ppa.g.sec = 0;
    ppa.g.rsv = 0;
    for (pg_n = 0; pg_n < spp->pgs_per_blk; pg_n++) {
        ppa.g.pg = pg_n;
        for (lun = 0; lun < spp->luns_per_ch; lun++) {
            ppa.g.lun = lun;
            for (ch = 0; ch < spp->nchs; ch++) {
                ppa.g.ch = ch;

                if (group_n % ASYNC_IO_UNIT == 0) {
                    qemu_mutex_unlock(&ssd->comp_mu);
                    wait_pending_reads(ssd);
                    qemu_mutex_lock(&ssd->comp_mu);
                }

                pg = lksv3_get_pg(ssd, &ppa);
                if (pg->status == PG_INVALID) {
                    FREE(pg->data);
                } else if (pg->status == PG_VALID) {
                    if (pg->data) {
                        _lksv3_gc_meta_femu(ssd, &ppa);
                    } else {
                        pg->status = PG_INVALID;
                        printf("ERR: valid but empty page in line: %d, ch: %d, lun: %d, pg: %d\n", ppa.g.blk, ppa.g.ch, ppa.g.lun, ppa.g.pg);
                        abort();
                    }
                } else {
                    abort();
                }
                group_n++;
                if (group_n % PG_N == 0) {
                    // Reset the gc context.
                    _lksv3_gc_meta_femu(ssd, NULL);
                }
            }
        }
    }
    kv_assert(group_n == spp->pgs_per_line);

    for (ch = 0; ch < spp->nchs; ch++) {
        for (lun = 0; lun < spp->luns_per_ch; lun++) {
            ppa.g.ch = ch;
            ppa.g.lun = lun;
            ppa.g.pl = 0;
            ppa.g.sec = 0;
            ppa.g.rsv = 0;

            gc_erase_delay(ssd, &ppa);
            lksv3_mark_block_free(ssd, &ppa);
        }
    }

    lksv3_mark_line_free(ssd, &ppa);
    check_linecnt(ssd);

    return 0;
}

