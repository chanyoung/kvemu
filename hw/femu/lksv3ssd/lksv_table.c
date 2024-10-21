#include "hw/femu/kvssd/lksv/lksv3_ftl.h"
#include "hw/femu/kvssd/lksv/skiplist.h"

#ifndef OURS
static int onep = 5;
static int fivp = 25;
#endif

bool move_line_m2d(struct ssd *ssd, bool force)
{
    struct line_partition *m = &ssd->lm.meta;
    struct line_partition *d = &ssd->lm.data;
    struct line *line;

    if (m->free_line_cnt < 4)
        return false;
    if (!force && m->lines < lksv_lsm->t_meta)
        return false;
#ifndef OURS
    if (m->free_line_cnt <= onep + onep)
        return false;
    if (m->lines <= lksv_lsm->t_meta)
        return false;
    if (m->free_line_cnt - fivp - onep < 0)
        return false;
#endif

    line = lksv3_get_next_free_line(m);
    kv_assert(line->meta);
    m->lines--;

    QTAILQ_INSERT_TAIL(&d->free_line_list, line, entry);
    d->free_line_cnt++;
    d->lines++;
    line->meta = false;

    kv_log("m2d line(%d): data lines(%d), meta lines(%d)\n", line->id, d->lines, m->lines);

    return true;
}

bool move_line_d2m(struct ssd *ssd, bool force)
{
    struct line_partition *m = &ssd->lm.meta;
    struct line_partition *d = &ssd->lm.data;
    struct line *line;

    if (d->free_line_cnt < 4)
        return false;
    if (!force && d->lines < ssd->sp.tt_lines - lksv_lsm->t_meta)
        return false;
#ifndef OURS
    if (lksv3_should_data_gc_high(ssd, 0))
        return false;
#endif

    line = lksv3_get_next_free_line(d);
    kv_assert(!line->meta);
    d->lines--;

    QTAILQ_INSERT_TAIL(&m->free_line_list, line, entry);
    m->free_line_cnt++;
    m->lines++;
    line->meta = true;

    kv_log("d2m line(%d): data lines(%d), meta lines(%d)\n", line->id, d->lines, m->lines);

    return true;
}

static bool
is_fit(lksv3_sst_t *sst, lksv3_kv_pair_t *kv, const int wp)
{
    int written = LKSV3_SSTABLE_FOOTER_BLK_SIZE + wp;

    written += (LKSV3_SSTABLE_META_BLK_SIZE + LKSV3_SSTABLE_STR_IDX_SIZE) *
               sst->footer.g.n;

    return written + (kv->k.len + kv->v.len + LKSV3_SSTABLE_META_BLK_SIZE +
           LKSV3_SSTABLE_STR_IDX_SIZE) <= PAGESIZE;
}

void
lksv3_sst_encode_str_idx(lksv3_sst_t *sst, lksv3_sst_str_idx_t *block, int n)
{
    kv_assert(sst->footer.g.n == n);
    kv_assert(sst->footer.g.n > 0);
    /* Written size <= PAGESIZE */
    kv_assert(sst->meta[n-1].g1.off +
              sst->meta[n-1].g1.klen +
              sst->meta[n-1].g2.snum == 1 ? sst->meta[n-1].g2.slen : 0 +
              n * (LKSV3_SSTABLE_META_BLK_SIZE + LKSV3_SSTABLE_STR_IDX_SIZE) +
              LKSV3_SSTABLE_FOOTER_BLK_SIZE <= PAGESIZE);

    memcpy(sst->raw + (PAGESIZE - LKSV3_SSTABLE_FOOTER_BLK_SIZE -
           (n * (LKSV3_SSTABLE_META_BLK_SIZE + LKSV3_SSTABLE_STR_IDX_SIZE))),
           block, n * LKSV3_SSTABLE_STR_IDX_SIZE);
}

int lksv3_sst_encode2(lksv3_sst_t *sst, lksv3_kv_pair_t *kv, uint32_t hash, int *wp, bool sharded) {
    if (!sharded && !is_fit(sst, kv, *wp)) {
        return LKSV3_TABLE_FULL;
    }

    lksv_block_meta *m = &sst->meta[sst->footer.g.n];
    sst->footer.g.n++;

    // TODO: Handling shards.
    m->g2.sid = 0;
    if (!sharded)
        m->g2.snum = 1;
    else
        m->g2.snum = 2;
    m->g2.voff = kv->voff;

    m->g1.off = *wp;
    m->g1.klen = kv->k.len;
    m->g1.hash = hash;
    *wp = (*wp) + kv->k.len;

    // TODO: Copy value.
    m->g2.slen = kv->v.len;
    if (kv->v.len == PPA_LENGTH)
        m->g1.flag = VLOG;
    else
        m->g1.flag = VMETA;
    int v = *wp + kv->v.len;
    *wp = v;

    memcpy(sst->raw + (PAGESIZE - LKSV3_SSTABLE_FOOTER_BLK_SIZE), &sst->footer, sizeof(lksv_block_footer));
    memcpy(sst->raw + (PAGESIZE - LKSV3_SSTABLE_FOOTER_BLK_SIZE - (LKSV3_SSTABLE_META_BLK_SIZE * sst->footer.g.n)), m, sizeof(lksv_block_meta));
    memcpy(sst->raw + m->g1.off, kv->k.key, kv->k.len);
    if (kv->ppa.ppa != UNMAPPED_PPA) {
        memcpy(sst->raw + m->g1.off + kv->k.len, &kv->ppa, sizeof(struct femu_ppa));
    }
    return LKSV3_TABLE_OK;
}

int lksv3_sst_decode(lksv3_sst_t *sst, void *raw) {
    sst->footer = *(lksv_block_footer *) (raw + (PAGESIZE - LKSV3_SSTABLE_FOOTER_BLK_SIZE));
    sst->str_idx = (lksv3_sst_str_idx_t *) (raw + (PAGESIZE - LKSV3_SSTABLE_FOOTER_BLK_SIZE - (LKSV3_SSTABLE_META_BLK_SIZE * sst->footer.g.n) - (LKSV3_SSTABLE_STR_IDX_SIZE * sst->footer.g.n)));
    if (sst->meta == NULL) {
        sst->meta = calloc(sst->footer.g.n, sizeof(lksv_block_meta));
    }

    int i;
    for (i = 0; i < sst->footer.g.n; i++) {
        int offset = PAGESIZE - LKSV3_SSTABLE_FOOTER_BLK_SIZE - (LKSV3_SSTABLE_META_BLK_SIZE * (i + 1));
        sst->meta[i] = *(lksv_block_meta *) (raw + offset);
    }

    return LKSV3_TABLE_OK;
}

struct femu_ppa lksv3_sst_write(struct ssd *ssd, struct femu_ppa ppa, lksv3_sst_t *sst) {
    struct nand_page *pg = lksv3_get_pg(ssd, &ppa);
    memcpy(pg->data, sst->raw, PAGESIZE);
    return ppa;
}

void lksv3_sst_read(struct ssd *ssd, struct femu_ppa ppa, lksv3_sst_t *sst) {
    struct nand_page *pg = lksv3_get_pg(ssd, &ppa);
    kv_assert(sst->raw == NULL);
    sst->raw = calloc(1, PAGESIZE);
    memcpy(sst->raw, pg->data, PAGESIZE);
}

lksv3_compaction *new_lksv3_compaction(int max_keys) {
    lksv3_compaction *c = calloc(1, sizeof(lksv3_compaction));
    c->high = calloc(1, sizeof(lksv3_sst_t));
    c->high->meta = calloc(max_keys, sizeof(lksv_block_meta));
    c->high_ppa.ppa = UNMAPPED_PPA;
    c->low = calloc(1, sizeof(lksv3_sst_t));
    c->low->meta = calloc(max_keys, sizeof(lksv_block_meta));
    c->low_ppa.ppa = UNMAPPED_PPA;
    c->target.meta = calloc(max_keys, sizeof(lksv_block_meta));
    c->target.raw = calloc(1, PAGESIZE);
    c->max_keys = max_keys;
    return c;
}

void free_lksv3_compaction(lksv3_compaction *c) {
    if (c->high) {
        if (c->high->raw)
            FREE(c->high->raw);
        if (c->high->meta)
            FREE(c->high->meta);
        FREE(c->high);
    }
    if (c->low) {
        if (c->low->raw)
            FREE(c->low->raw);
        if (c->low->meta)
            FREE(c->low->meta);
        FREE(c->low);
    }
    if (c->target.raw)
        FREE(c->target.raw);
    if (c->target.meta)
        FREE(c->target.meta);
    FREE(c);
}

static void load_run_to_comp_entry_list(struct ssd *ssd, lksv_comp_list *list, lksv3_run_t *run, int to_level, int level) {
    kv_assert(list->n == 0);
    lksv3_sst_t sst[PG_N];
    int k = 0;

    int tt_keys[PG_N];

    memset(sst, 0, PG_N * sizeof(lksv3_sst_t));
    for (int i = 0; i < run->hash_list_n; i++) {
        struct femu_ppa ppa = get_next_write_ppa(ssd, run->ppa, i);
        struct nand_page *pg = lksv3_get_pg(ssd, &ppa);
        sst[i].raw = pg->data;
        // TODO: Test level 1, 2 (idx, 0, 1) in mem.
        if (ssd->sp.enable_comp_delay && !kv_is_cached(lksv_lsm->lsm_cache, run->cache[DATA_SEGMENT_GROUP])) {
            struct nand_cmd cpr;
            cpr.type = COMP_IO;
            cpr.cmd = NAND_READ;
            cpr.stime = 0;
            lksv3_ssd_advance_status(ssd, &ppa, &cpr);
        }
        k++;
        if (k % ASYNC_IO_UNIT == 0) {
            wait_pending_reads(ssd);
        }

        sst[i].footer = *(lksv_block_footer *) (sst[i].raw + (PAGESIZE - LKSV3_SSTABLE_FOOTER_BLK_SIZE));
        sst[i].str_idx = (lksv3_sst_str_idx_t *) (sst[i].raw + (PAGESIZE - LKSV3_SSTABLE_FOOTER_BLK_SIZE - (LKSV3_SSTABLE_META_BLK_SIZE * sst[i].footer.g.n) - (LKSV3_SSTABLE_STR_IDX_SIZE * sst[i].footer.g.n)));
        if (i == 0) {
            tt_keys[i] = 0;
        } else {
            tt_keys[i] = tt_keys[i-1] + sst[i-1].footer.g.n;
        }
    }

    int si = 0;
    lksv_comp_entry *p = NULL;
    uint32_t sidx_sst = -1;
    for (int i = 0; i < run->hash_list_n; i++) {
        for (int j = 0; j < sst[i].footer.g.n; j++) {
            p = &list->str_order_entries[si];
            sidx_sst = sst[i].str_idx[j].g1.sst;
            uint32_t sidx_off = sst[i].str_idx[j].g1.off;
            //lksv3_sst_str_idx_t sidx = sst[i].str_idx[j];

            kv_assert(p->key.key == NULL);

            p->meta = *(lksv_block_meta *) (sst[sidx_sst].raw + (PAGESIZE - LKSV3_SSTABLE_FOOTER_BLK_SIZE - (LKSV3_SSTABLE_META_BLK_SIZE * (sidx_off + 1))));
            p->key.key = sst[sidx_sst].raw + p->meta.g1.off;
            p->key.len = p->meta.g1.klen;
            list->n++;

            if (p->meta.g1.flag == VLOG) {
                struct femu_ppa *log_ppa = sst[sidx_sst].raw + p->meta.g1.off + p->meta.g1.klen;
                //lksv_lsm->disk[level]->reference_lines[log_ppa->g.blk] = false;
                struct line *line = lksv3_get_line(ssd, log_ppa);
                //per_line_data(line)->referenced_levels[level] = false;
                bool unify = should_unify(ssd, log_ppa, level, to_level);
                if (unify) {
                    struct nand_cmd srd;
                    srd.type = COMP_IO;
                    srd.cmd = NAND_READ;
                    srd.stime = 0;
                    lksv3_ssd_advance_status(ssd, log_ppa, &srd);
                    k++;
                    if (k % ASYNC_IO_UNIT == 0) {
                        wait_pending_reads(ssd);
                    }

                    struct nand_page *pg2 = lksv3_get_pg(ssd, log_ppa);
                    int offset = PAGESIZE - LKSV3_SSTABLE_FOOTER_BLK_SIZE - (LKSV3_SSTABLE_META_BLK_SIZE * (p->meta.g2.voff + 1));
                    lksv_block_meta meta = *(lksv_block_meta *) (pg2->data + offset);
                    kv_assert(meta.g1.hash == p->meta.g1.hash);
                    p->meta.g2.slen = meta.g2.slen;
                    p->meta.g1.flag = VMETA;
                    p->ppa.ppa = UNMAPPED_PPA;
                    line->vsc++;
                } else {
                    p->ppa = *log_ppa;
                    kv_assert(p->meta.g1.flag == VLOG);
                }
            } else {
                p->ppa.ppa = UNMAPPED_PPA;
                kv_assert(p->meta.g1.flag == VMETA);
            }

            //list->hash_order_pointers[si] = &list->str_order_entries[si];
            // ???
            list->hash_order_pointers[tt_keys[sidx_sst]+sidx_off] = &list->str_order_entries[si];

            // set hash order of str_keys.
            p->hash_order = tt_keys[sidx_sst] + sidx_off;
            si++;
        }
    }

    if (level == lksv_lsm->bottom_level && p != NULL && sidx_sst != -1) {
        lksv_lsm->sum_key_bytes += p->meta.g1.klen;
        if (p->meta.g1.flag == VMETA) {
            lksv_lsm->sum_value_bytes += p->meta.g2.slen;
        } else {
            struct femu_ppa *log_ppa = sst[sidx_sst].raw + p->meta.g1.off + p->meta.g1.klen;
            struct nand_cmd srd;
            srd.type = COMP_IO;
            srd.cmd = NAND_READ;
            srd.stime = 0;
            lksv3_ssd_advance_status(ssd, log_ppa, &srd);
            k++;
            if (k % ASYNC_IO_UNIT == 0) {
                wait_pending_reads(ssd);
            }

            struct nand_page *pg2 = lksv3_get_pg(ssd, log_ppa);
            int offset = PAGESIZE - LKSV3_SSTABLE_FOOTER_BLK_SIZE - (LKSV3_SSTABLE_META_BLK_SIZE * (p->meta.g2.voff + 1));
            lksv_block_meta meta = *(lksv_block_meta *) (pg2->data + offset);
            kv_assert(meta.g1.hash == p->meta.g1.hash);
            lksv_lsm->sum_value_bytes += meta.g2.slen;
        }
        if (++lksv_lsm->samples_count == 10000) {
            lksv_lsm->avg_value_bytes = lksv_lsm->sum_value_bytes / lksv_lsm->samples_count;
            lksv_lsm->avg_key_bytes = lksv_lsm->sum_key_bytes / lksv_lsm->samples_count;
            lksv_lsm->sum_value_bytes = 0;
            lksv_lsm->sum_key_bytes = 0;
            lksv_lsm->samples_count = 0;
        }
    }
}

static lksv_comp_entry *get_next_comp_entry(lksv_comp_lists_iterator *it) {
retry:
    if (it->i >= it->imax) {
        return NULL;
    }

    if (it->n >= (*it->l)[it->i].n) {
        it->n = 0;
        it->i++;
        goto retry;
    } else {
        lksv_comp_entry *ret;
        ret = &(*it->l)[it->i].str_order_entries[it->n];
        it->n++;
        return ret;
    }
}

static int compare_gc(const void *a, const void *b)
{
    struct lksv3_gc g1 = *(struct lksv3_gc *)a;
    struct lksv3_gc g2 = *(struct lksv3_gc *)b;
    if (g1.inv_ratio > g2.inv_ratio) {
        return -1;
    } else if (g1.inv_ratio < g2.inv_ratio) {
        return 1;
    } else {
        return 0;
    }
}

static void mark_reclaimable_log_lines(struct ssd *ssd, int ulevel_i, int level_i)
{
    kv_assert(level_i >= 1);

    lksv3_level *level = lksv_lsm->disk[level_i];
    lksv3_level *level_h = lksv_lsm->disk[ulevel_i];
    memset(lksv_lsm->gc_plan[level_i], 0, 512 * sizeof(bool));
    memset(lksv_lsm->gc_plan[ulevel_i], 0, 512 * sizeof(bool));

    struct lksv3_gc gcs[512];
    memset(gcs, 0, (512) * sizeof(struct lksv3_gc));

    int avg_inv_ratio = 0;
    int avg_cnt = 0;

    for (int i = 0; i < 512; i++) {
        gcs[i].lineid = i;
        gcs[i].inv_ratio = -1;
        if (is_meta_line(ssd, i)) {
            continue;
        }
        struct line *line = &ssd->lm.lines[i];
        int inv_ratio = 0;
        if (line->isc + line->vsc > 0) {
            inv_ratio = (line->isc * 100) / (line->isc + line->vsc);
            avg_inv_ratio += inv_ratio;
            avg_cnt++;
        } else {
            continue;
        }
        if (i == ssd->lm.data.wp.blk) {
            continue;
        }
        if (lksv_lsm->flush_reference_lines[i]) {
            continue;
        }
        if (lksv_lsm->flush_buffer_reference_lines[i]) {
            continue;
        }
        if (!level->reference_lines[i] && !level_h->reference_lines[i]) {
            continue;
        }
        bool referenced_by_other_levels = false;
        for (int j = 0; j < 4; j ++) {
            if (j != level_i && j != ulevel_i && per_line_data(line)->referenced_levels[j]) {
                referenced_by_other_levels = true;
                break;
            }
        }
        if (referenced_by_other_levels) {
            continue;
        }
        gcs[i].inv_ratio = inv_ratio;
    }
    if (avg_cnt > 0)
        avg_inv_ratio /= avg_cnt;
    kv_log("avg_inv_ratio: %d\n", avg_inv_ratio);

    qsort(gcs, (512), sizeof(struct lksv3_gc), compare_gc);

    struct ssdparams *spp = &ssd->sp;
    while (true) {
        struct line *victim_line = pqueue_peek(ssd->lm.meta.victim_line_pq);
        if (!victim_line) {
            break;
        }
        if (victim_line->vpc != 0) {
            break;
        }
        pqueue_pop(ssd->lm.meta.victim_line_pq);
        victim_line->pos = 0;
        ssd->lm.meta.victim_line_cnt--;

        kv_debug("%d gc_meta! (line: %d) invalid_pgs / pgs_per_line: %d / %d, vpc: %d \n", ++lksv_lsm->header_gc_cnt, victim_line->id, victim_line->ipc, ssd->sp.pgs_per_line, victim_line->vpc);

        struct femu_ppa mppa;
        mppa.g.blk = victim_line->id;
        for (int ch = 0; ch < spp->nchs; ch++) {
            for (int lun = 0; lun < spp->luns_per_ch; lun++) {
                wait_pending_reads(ssd);

                mppa.g.ch = ch;
                mppa.g.lun = lun;
                mppa.g.pl = 0;
                mppa.g.sec = 0;
                mppa.g.rsv = 0;

                gc_erase_delay(ssd, &mppa);
                lksv3_mark_block_free(ssd, &mppa);
            }
        }
        lksv3_mark_line_free(ssd, &mppa);
        check_linecnt(ssd);
    }

    lksv3_level *last = lksv_lsm->disk[LSM_LEVELN - 1];
    int threshold;
    if (last->v_num > last->n_num) {
        threshold = 100 * (last->m_num - last->v_num) / last->m_num;
    } else {
        threshold = 100 * (last->m_num - last->n_num) / last->m_num;
    }
    // TODO: fix this to calculate proper disk usage.
    if (lksv3_should_data_gc_high(ssd, 5))
        threshold = 30;
    else
        threshold = 60;
    if (lksv_lsm->force)
        threshold /= 2;

    kv_log("threshold: %d\n", threshold);
    kv_log("GC Plan for level %d,%d:", ulevel_i, level_i);

    int lt = LSM_LEVELN - 1;
    if (lksv_lsm->force) {
        lt = lksv_lsm->bottom_level;
    }
    for (int i = 0; i < 512; i++) {
        if (gcs[i].inv_ratio < 0)
            break;
        if (level_i != lt && gcs[i].inv_ratio < threshold)
            break;

        struct line *line = &ssd->lm.lines[gcs[i].lineid];
        if (per_line_data(line)->sg.scattered)
            kv_log(" scatter([%d:%d])", gcs[i].lineid, gcs[i].inv_ratio);
        else
            kv_log(" gather([%d:%d])", gcs[i].lineid, gcs[i].inv_ratio);

        lksv_lsm->gc_plan[level_i][gcs[i].lineid] = true;
        lksv_lsm->gc_plan[ulevel_i][gcs[i].lineid] = true;
        lksv_lsm->gc_planned++;
        if (lksv_lsm->gc_planned % 8 == 0)
            kv_log("\n");
    }
    kv_log("\n"); 
}

static struct femu_ppa log_write2(struct ssd *ssd, lksv_comp_entry *e)
{
    static int wp;
    static struct femu_ppa fppa = {.ppa = UNMAPPED_PPA};
    static lksv3_sst_t sst;
    const int MAX_META = 1024;
    static lksv_block_meta *meta;
    int ret;
    struct nand_page *pg;
    static int in_page_idx = 0;

    kv_key dummy_key;
    dummy_key.key = NULL;
    dummy_key.len = 0;

    static kv_key skey = { .len = 0, .key = NULL };
    static kv_key ekey = { .len = 0, .key = NULL };
    static kv_key prev_key = { .len = 0, .key = NULL };

retry:
    if (e) {
        if (fppa.ppa == UNMAPPED_PPA) {
            FREE(meta);
            memset(&sst, 0, sizeof(lksv3_sst_t));
            meta = calloc(MAX_META, sizeof(lksv_block_meta));
            sst.meta = meta;
            wp = 0;
            in_page_idx = 0;

            fppa = lksv3_get_new_data_page(ssd);
            pg = lksv3_get_pg(ssd, &fppa);
            if (pg->data == NULL) {
                pg->data = calloc(1, PAGESIZE);
            }
            sst.raw = pg->data;

            skey = kv_key_min;
            ekey = kv_key_max;
            prev_key = kv_key_min;
        } else {
            kv_assert(sst.meta == meta);
        }
    } else {
        if (wp == 0) {
            kv_assert(!meta);
            fppa.ppa = UNMAPPED_PPA;
            return fppa;
        }
        // TODO: cleanup
        lksv3_mark_page_valid(ssd, &fppa);
        if (sst.footer.g.n) {
            struct line *line = lksv3_get_line(ssd, &fppa);
            line->vsc += sst.footer.g.n;
            kv_copy_key(&ekey, &prev_key);
            update_sg(line, skey, ekey, false);
            //kv_assert(!per_line_data(line)->sg.scattered);
        }
        lksv3_ssd_advance_write_pointer(ssd, &ssd->lm.data);
        if (ssd->sp.enable_comp_delay) {
            struct nand_cmd cpw;
            cpw.type = COMP_IO;
            cpw.cmd = NAND_WRITE;
            cpw.stime = 0;
            lksv3_ssd_advance_status(ssd, &fppa, &cpw);
        }
        FREE(meta);
        sst.meta = NULL;
        fppa.ppa = UNMAPPED_PPA;
        wp = 0;

        skey = kv_key_min;
        ekey = kv_key_max;
        prev_key = kv_key_min;
        return fppa;
    }

    kv_assert(e->meta.g2.slen > PPA_LENGTH);

    lksv3_kv_pair_t kv;
    kv.k = dummy_key;
    kv.v.len = e->meta.g2.slen;
    kv.ppa.ppa = UNMAPPED_PPA;

    e->meta.g2.voff = in_page_idx;
    ret = lksv3_sst_encode2(&sst, &kv, e->meta.g1.hash, &wp, false);
    in_page_idx++;
    if (ret == LKSV3_TABLE_FULL) {
        struct line *line = lksv3_get_line(ssd, &fppa);
        line->vsc += sst.footer.g.n;
        lksv3_mark_page_valid(ssd, &fppa);
        lksv3_ssd_advance_write_pointer(ssd, &ssd->lm.data);

        if (ssd->sp.enable_comp_delay) {
            struct nand_cmd cpw;
            cpw.type = COMP_IO;
            cpw.cmd = NAND_WRITE;
            cpw.stime = 0;
            lksv3_ssd_advance_status(ssd, &fppa, &cpw);
        }
        fppa.ppa = UNMAPPED_PPA;

        kv_copy_key(&ekey, &prev_key);
        kv_assert(kv_cmp_key(skey, e->key) < 0);
        kv_assert(kv_cmp_key(ekey, e->key) < 0);
        update_sg(line, skey, ekey, false);
        skey = kv_key_min;
        ekey = kv_key_max;
        prev_key = kv_key_min;
        goto retry;
    } else {
        kv_assert(ret == LKSV3_TABLE_OK);
    }

    if (skey.key == kv_key_min.key) {
        kv_copy_key(&skey, &e->key);
    }
    kv_assert(kv_cmp_key(prev_key, e->key) < 0);
    prev_key = e->key;

    return fppa;
}

static uint64_t upcnt = 0;
static uint64_t upncnt = 0;

#ifndef OURS
static inline bool
should_written_back_into_value_log(struct ssd *ssd,
                                   lksv3_level *to,
                                   lksv3_level *target)
{
    bool   to_log;
    double threshold;

    /*
     * Once the size of the target destination level reaches a certain point,
     * the remaining values (to be merged to the data segment groups) are
     * written back into the value log. Doing so prevents any chance of a
     * compaction chain from being generated.
     */
    if (to->idx == LSM_LEVELN - 1) {
        to_log = false;
    } else {
        if (lksv_lsm->avg_value_bytes / lksv_lsm->avg_key_bytes > 20) {
            threshold = 0.00;
        } else if (lksv_lsm->avg_value_bytes / lksv_lsm->avg_key_bytes > 10) {
            threshold = 0.70;
        } else if (lksv_lsm->avg_value_bytes / lksv_lsm->avg_key_bytes > 5) {
            threshold = 0.80;
        } else if (lksv_lsm->avg_value_bytes / lksv_lsm->avg_key_bytes > 3) {
            threshold = 0.90;
        } else {
            threshold = 1.00;
        }
        to_log = target->n_num >= target->m_num * threshold;
        to_log = to_log && (ssd->lm.data.free_line_cnt > 0);
    }

    return to_log;
}
#endif

static void
_do_lksv3_compaction2(struct ssd *ssd,
                      lksv3_level *from,
                      lksv3_level *to,
                      lksv3_level *target,
                      kv_skiplist *mem)
{
    lev_iter                    *to_iter;
    lev_iter                    *from_iter;
    lksv3_run_t                 *to_run;
    lksv3_run_t                 *from_run;
    lksv_comp_list              *upper;
    lksv_comp_list              *lower;
    lksv_comp_list              merged;
    lksv_comp_lists_iterator    ui;
    lksv_comp_lists_iterator    li;
    lksv_comp_entry             *ue;
    lksv_comp_entry             *le;
    kv_snode                    *temp;
    int                         i = 0;
    int                         j = 0;
    struct lksv3_hash_sort_t    sort;
    int                         page_n;
    int                         page_size;
    int                         bound;
    int                         PG_LIMIT;

    kv_assert(target->idx == to->idx);
    kv_assert(from ? from->idx < target->idx : true);

    if (to->idx > 0)
        mark_reclaimable_log_lines(ssd, from->idx, to->idx);

    upper = calloc(from ? from->n_num : 1, sizeof(lksv_comp_list));
    lower = calloc(to->n_num, sizeof(lksv_comp_list));

    if (from) {
        from_iter = lksv3_get_iter(from, from->start, from->end);
        while ((from_run = lksv3_iter_nxt(from_iter))) {
            upper[i].str_order_entries = calloc((from_run->hash_list_n * 512), sizeof(lksv_comp_entry));
            upper[i].hash_order_pointers = calloc((from_run->hash_list_n * 512), sizeof(lksv_comp_entry *));
            upper[i].str_order_map = calloc((from_run->hash_list_n * 512), sizeof(lksv3_sst_str_idx_t));
            load_run_to_comp_entry_list(ssd, &upper[i], from_run, to->idx, from->idx);
            i++;

            j += from_run->hash_list_n;
            if (j >= ASYNC_IO_UNIT) {
                wait_pending_reads(ssd);
                j -= ASYNC_IO_UNIT;
            }
        }
    } else {
        upper[0].str_order_entries = calloc(mem->n, sizeof(lksv_comp_entry));
        upper[0].hash_order_pointers = calloc(mem->n, sizeof(lksv_comp_entry *));
        upper[0].str_order_map = calloc(mem->n, sizeof(lksv3_sst_str_idx_t));
        upper->n = mem->n;

        for_each_sk(temp, mem) {
            lksv_comp_entry *p = &upper[0].str_order_entries[i];
            // TODO: fix this.
            if (temp->value->length == PPA_LENGTH) {
                p->meta.g1.flag = VLOG;
            } else {
                p->meta.g1.flag = VMETA;
            }
            if (temp->private) {
                p->ppa = *snode_ppa(temp);
                p->meta.g2.voff = *snode_off(temp);
                p->meta.g1.hash = *snode_hash(temp);
            } else {
                p->ppa.ppa = UNMAPPED_PPA;
                p->meta.g1.hash = XXH32(temp->key.key, temp->key.len, 0);
            }
            p->key = temp->key;
            p->meta.g1.klen = temp->key.len;
            p->meta.g2.slen = temp->value->length;
            upper[0].hash_order_pointers[i] = p;
            i++;
        }
        bucket_sort(&upper[0]);

        for (j = 0; j < upper->n-1; j++) {
            kv_assert(upper[0].hash_order_pointers[j]->meta.g1.hash <= upper[0].hash_order_pointers[j+1]->meta.g1.hash);
            kv_assert(upper[0].hash_order_pointers[j]->hash_order == j);
        }
    }

    i = j = 0;
    to_iter = lksv3_get_iter(to, to->start, to->end);
    while ((to_run = lksv3_iter_nxt(to_iter))) {
        lower[i].str_order_entries = calloc((to_run->hash_list_n * 512), sizeof(lksv_comp_entry));
        lower[i].hash_order_pointers = calloc((to_run->hash_list_n * 512), sizeof(lksv_comp_entry *));
        lower[i].str_order_map = calloc(to_run->hash_list_n * 512, sizeof(lksv3_sst_str_idx_t));
        load_run_to_comp_entry_list(ssd, &lower[i], to_run, to->idx, to->idx);
        i++;

        j += to_run->hash_list_n;
        if (j >= ASYNC_IO_UNIT) {
            wait_pending_reads(ssd);
            j -= ASYNC_IO_UNIT;
        }
    }

    merged.str_order_entries = calloc((PG_N * 512), sizeof(lksv_comp_entry));
    merged.hash_order_pointers = calloc((PG_N * 512), sizeof(lksv_comp_entry*));
    merged.str_order_map = calloc((PG_N * 512), sizeof(lksv3_sst_str_idx_t));
    merged.n = 0;

    li.l = &lower;
    li.i = li.n = 0;
    li.imax = to->n_num;
    ui.l = &upper;
    ui.i = ui.n = 0;
    if (from)
        ui.imax = from->n_num;
    else
        ui.imax = 1;

    memset(&sort, 0, sizeof(struct lksv3_hash_sort_t));
    sort.u_start_i = sort.u_end_i = ui.i;
    sort.u_start_n = ui.n;
    sort.u[0] = calloc((*ui.l)[0].n, sizeof(lksv_comp_entry*));
    ue = get_next_comp_entry(&ui);

    sort.l_start_i = sort.l_end_i = li.i;
    sort.l_start_n = li.n;
    sort.l[0] = calloc((*li.l)[0].n, sizeof(lksv_comp_entry*));
    le = get_next_comp_entry(&li);

    page_n = 1;
    page_size = LKSV3_SSTABLE_FOOTER_BLK_SIZE;

    if (target->idx != LSM_LEVELN - 1)
        bound = PAGESIZE;
    else
        bound = 0;
    bound = (PG_N * PAGESIZE) - bound;
    PG_LIMIT = bound / PAGESIZE;
    kv_assert(PG_LIMIT <= PG_N);

    log_write2(ssd, NULL);

#ifndef OURS
    // We firstly move values from to tree, and then resent values to logs. So we unlimit the 1% of tree's last level room.
    int lines = lksv_lsm->t_meta < ssd->lm.meta.lines ? lksv_lsm->t_meta : ssd->lm.meta.lines;
    bool to_log = should_written_back_into_value_log(ssd, to, target);
    int cnt_for_lsm_comp_space = 0;
#endif

    int cnt = 0;
    int mcnt = 0;
    int last_log_line = -1;
#ifndef OURS
    bool print_warn_a = false;
    bool print_warn_b = false;
#endif

    while (true) {
        lksv_comp_entry *te = &merged.str_order_entries[merged.n];
        merged.hash_order_pointers[merged.n] = te;
        //te->str_order = merged.n;

        int res;
        if (ue && le) {
            res = kv_cmp_key(le->key, ue->key);
        } else if (!ue && le) {
            res = -1;
        } else if (!le && ue) {
            res = +1;
        } else {
            if (merged.n > 0) {
                //bucket_sort(&merged);
                default_merge(&sort, &merged, &ui, &li);
                lksv3_run_t *run = calloc(1, sizeof(lksv3_run_t));
                lksv3_mem_cvt2table2(ssd, &merged, run);
                lksv3_insert_run2(ssd, target, run);
                lksv3_free_run(lksv_lsm, run);
                FREE(run->key.key);
                FREE(run->end.key);
                FREE(run);
                merged.n = 0;
            }
            break;
        }

        if (res < 0) {
            te->key = le->key;
            te->meta = le->meta;
            te->hash_order = le->hash_order;

            if (le->meta.g1.flag == VMETA) {
#ifndef OURS
                if (false) {
#else
                if (to_log) {
#endif
                    upcnt++;
                    te->ppa = log_write2(ssd, te);
                    if (te->ppa.g.blk != last_log_line) {
                        last_log_line = te->ppa.g.blk;
                        cnt++;
                        if (cnt > 0) {
                            to_log = should_written_back_into_value_log(ssd, to, target);

                            int lsm_last_level_limit_for_comp = lines - fivp - onep + 1;
                            int lsm_last_level_used = ((lksv_lsm->disk[LSM_LEVELN - 1]->n_num * PG_N) / ssd->sp.pgs_per_line);
                            if (!print_warn_a && lsm_last_level_used > lsm_last_level_limit_for_comp) {
                                kv_debug("[WARN] lsm_last_level_used(%d) > limit(%d)\n", lsm_last_level_used, lsm_last_level_limit_for_comp);
                                print_warn_a = true;
                            }
                            if (!print_warn_b && ssd->lm.meta.free_line_cnt <= onep) {
                                kv_debug("[WARN] meta.free_line_cnt(%d) <= onep(%d)\n", ssd->lm.meta.free_line_cnt, onep);
                                print_warn_b = true;
                            }
                            cnt = 0;
                        }
                    }
                    kv_assert(te->meta.g2.slen > PPA_LENGTH);
                    te->meta.g2.slen = PPA_LENGTH;
                    kv_assert(te->meta.g1.flag == VMETA);
                    te->meta.g1.flag = VLOG;
                } else {
                    upncnt++;
                    te->ppa = le->ppa;
                }
            } else {
                te->ppa = le->ppa;
            }

            le = get_next_comp_entry(&li);
        } else {
            te->key = ue->key;
            te->meta = ue->meta;
            te->hash_order = ue->hash_order;
            if (ue->meta.g1.flag == VMETA) {
#ifdef OURS
                if (false) {
#else
                if (to_log) {
#endif
                    upcnt++;
                    te->ppa = log_write2(ssd, te);
                    if (te->ppa.g.blk != last_log_line) {
                        last_log_line = te->ppa.g.blk;
                        cnt++;
                        if (cnt > 0) {
                            to_log = should_written_back_into_value_log(ssd, to, target);

                            int lsm_last_level_limit_for_comp = lines - fivp - onep + 1;
                            int lsm_last_level_used = ((lksv_lsm->disk[LSM_LEVELN - 1]->n_num * PG_N) / ssd->sp.pgs_per_line);
                            if (!print_warn_a && lsm_last_level_used > lsm_last_level_limit_for_comp) {
                                kv_debug("[WARN] lsm_last_level_used(%d) > limit(%d)\n", lsm_last_level_used, lsm_last_level_limit_for_comp);
                                print_warn_a = true;
                            }
                            if (!print_warn_b && ssd->lm.meta.free_line_cnt <= onep) {
                                kv_debug("[WARN] meta.free_line_cnt(%d) <= onep(%d)\n", ssd->lm.meta.free_line_cnt, onep);
                                print_warn_b = true;
                            }
                            cnt = 0;
                        }
                    }
                    kv_assert(te->meta.g2.slen > PPA_LENGTH);
                    te->meta.g2.slen = PPA_LENGTH;
                    kv_assert(te->meta.g1.flag == VMETA);
                    te->meta.g1.flag = VLOG;
                } else {
                    upncnt++;
                    te->ppa = ue->ppa;
                }
            } else {
                te->ppa = ue->ppa;
            }

            ue = get_next_comp_entry(&ui);

            if (res == 0) {
                if (le->meta.g1.flag == VLOG) {
                    lksv3_get_line(ssd, &le->ppa)->isc++;
                    lksv3_get_line(ssd, &le->ppa)->vsc--;
                }

                le = get_next_comp_entry(&li);
                
                if (!ssd->start_ramp && !ssd->start_log) {
                    ssd->start_ramp = true;
                    ssd->ramp_start_time = qemu_clock_get_ns(QEMU_CLOCK_REALTIME);
                }
            }
        }

        if (te->meta.g1.flag == VLOG) {
            struct line *l = lksv3_get_line(ssd, &te->ppa);
            per_line_data(l)->referenced_levels[target->idx] = true;
            target->reference_lines[te->ppa.g.blk] = true;
            // Just for the logic.
            to->reference_lines[te->ppa.g.blk] = true;
            check_473(ssd);
        }

        int te_size = te->key.len + te->meta.g2.slen + LKSV3_SSTABLE_META_BLK_SIZE + LKSV3_SSTABLE_STR_IDX_SIZE;
        if (page_size + te_size > PAGESIZE) {
#ifndef OURS
            cnt_for_lsm_comp_space++;
            if (mcnt % 32 == 0) {
                to_log = should_written_back_into_value_log(ssd, to, target);

                int lsm_last_level_limit_for_comp = lines - fivp - onep + 1;
                int lsm_last_level_used = ((lksv_lsm->disk[LSM_LEVELN - 1]->n_num * PG_N) / ssd->sp.pgs_per_line);
                if (!print_warn_a && lsm_last_level_used > lsm_last_level_limit_for_comp) {
                    kv_debug("[WARN] lsm_last_level_used(%d) > limit(%d)\n", lsm_last_level_used, lsm_last_level_limit_for_comp);
                    print_warn_a = true;
                }
                if (!print_warn_b && ssd->lm.meta.free_line_cnt <= onep) {
                    kv_debug("[WARN] meta.free_line_cnt(%d) <= onep(%d)\n", ssd->lm.meta.free_line_cnt, onep);
                    print_warn_b = true;
                }
            }
#endif

            if (page_n == PG_LIMIT) {
                //bucket_sort(&merged);
                default_merge(&sort, &merged, &ui, &li);

                lksv3_run_t *run = calloc(1, sizeof(lksv3_run_t));
                lksv3_mem_cvt2table2(ssd, &merged, run);
                lksv3_insert_run2(ssd, target, run);
                lksv3_free_run(lksv_lsm, run);
                FREE(run->key.key);
                FREE(run->end.key);
                FREE(run);

                //te->str_order = 0;
                merged.str_order_entries[0] = *te;
                te = &merged.str_order_entries[0];
                merged.hash_order_pointers[0] = &merged.str_order_entries[0];
                merged.n = 1;
                page_n = 1;
                mcnt++;
            } else {
                page_n++;
                merged.n++;
            }
            page_size = LKSV3_SSTABLE_FOOTER_BLK_SIZE + te_size;

        } else {
            page_size += te_size;
            merged.n++;
        }

        if (res < 0) {
            kv_assert(sort.l_end_i-sort.l_start_i < 512);
            sort.l[sort.l_end_i - sort.l_start_i][te->hash_order] = te;
            sort.l_end_n++;
            sort.ln[sort.l_end_i - sort.l_start_i]++;
            if (li.i > sort.l_end_i) {
                sort.l_end_i = li.i;
                sort.l_end_n = 0;
                if (li.i < li.imax) {
                    kv_assert(li.i - sort.l_start_i < 512);
                    sort.l[li.i - sort.l_start_i] = calloc((*li.l)[li.i].n, sizeof(lksv_comp_entry*));
                }
            }
        } else {
            kv_assert(sort.u_end_i-sort.u_start_i < 512);
            sort.u[sort.u_end_i - sort.u_start_i][te->hash_order] = te;
            sort.u_end_n++;
            sort.un[sort.u_end_i - sort.u_start_i]++;
            if (ui.i > sort.u_end_i) {
                sort.u_end_i = ui.i;
                sort.u_end_n = 0;
                if (ui.i < ui.imax) {
                    kv_assert(ui.i - sort.u_start_i < 512);
                    sort.u[ui.i - sort.u_start_i] = calloc((*ui.l)[ui.i].n, sizeof(lksv_comp_entry*));
                }
            }
            if (res == 0) {
                if (li.i > sort.l_end_i) {
                    sort.l_end_i = li.i;
                    sort.l_end_n = 0;
                    if (li.i < li.imax) {
                        kv_assert(li.i - sort.l_start_i < 512);
                        sort.l[li.i - sort.l_start_i] = calloc((*li.l)[li.i].n, sizeof(lksv_comp_entry*));
                    }
                }
            }
        }

        if ((mcnt > ssd->sp.pgs_per_blk) && (lksv_lsm->gc_planned > 0)) {
            if (ssd->start_log)
                lksv_gc_data_early(ssd, from->idx, to->idx, te->key);
            mcnt = 0;
        }
    }

#ifndef OURS
    static uint64_t ucnt = 0;
    if (ucnt++ % 100 == 0)
        kv_log("upcnt: %lu, upncnt: %lu, upcnt ratio: %lu\n", upcnt, upncnt, 100 * upcnt / (upcnt + upncnt + 1));
#endif

    int i_end = sort.u_end_i - sort.u_start_i;
    for (int i = 0; i < i_end; i++) {
        free(sort.u[i]);
    }

    i_end = sort.l_end_i - sort.l_start_i;
    for (int i = 0; i < i_end; i++) {
        free(sort.l[i]);
    }
    memset(&sort, 0, sizeof(struct lksv3_hash_sort_t));

    // Cleanup the partial log page.
    log_write2(ssd, NULL);

    FREE(merged.str_order_entries);
    FREE(merged.hash_order_pointers);
    FREE(merged.str_order_map);

    for (i = 0; i < ui.imax; i++) {
        FREE(upper[i].str_order_entries);
        FREE(upper[i].hash_order_pointers);
        FREE(upper[i].str_order_map);
    }
    FREE(upper);

    for (i = 0; i < li.imax; i++) {
        FREE(lower[i].str_order_entries);
        FREE(lower[i].hash_order_pointers);
        FREE(lower[i].str_order_map);
    }
    FREE(lower);

    struct femu_ppa run_new_ppa = lksv3_get_new_meta_page(ssd);
    while (!is_pivot_ppa(ssd, run_new_ppa)) {
        lksv3_mark_page_valid(ssd, &run_new_ppa);
        lksv3_ssd_advance_write_pointer(ssd, &ssd->lm.meta);
        lksv3_mark_page_invalid(ssd, &run_new_ppa);
        run_new_ppa = lksv3_get_new_meta_page(ssd);
    }

    check_473(ssd);
    if (lksv_lsm->gc_planned > 0) {
        check_linecnt(ssd);
        lksv3_gc_data_femu3(ssd, from->idx, to->idx);
        check_linecnt(ssd);
        kv_log("GC finished and current free lines: %d\n", ssd->lm.data.free_line_cnt);
    }

    if (lksv_lsm->should_d2m > 0) {
        if (move_line_d2m(ssd, false)) {
            lksv_lsm->should_d2m--;
        }
        lksv_lsm->m2d = 0;
    } else {
        lksv_lsm->m2d++;
        if (lksv_lsm->m2d % (512 / 16) == 0)
            move_line_m2d(ssd, false);
    }
    lksv_lsm->gc_planned = 0;
    for (int i = 0; i < 512; i++) {
        struct femu_ppa ppa;
        ppa.g.blk = i;
        struct line *line = lksv3_get_line(ssd, &ppa);
        if (from) {
            if (from->reference_lines[i]) {
                from->reference_lines[i] = false;
                kv_assert(per_line_data(line)->referenced_levels[from->idx]);
                per_line_data(line)->referenced_levels[from->idx] = false;
            }
        } else {
            if (lksv_lsm->flush_reference_lines[i]) {
                //assert(ssd->lm.data.wp.curline->id != i);
                if (ssd->lm.data.wp.curline->id != i) {
                    lksv_lsm->flush_reference_lines[i] = false;
                    kv_assert(per_line_data(line)->referenced_flush);
                    per_line_data(line)->referenced_flush = false;
                    kv_assert(target->idx == 0);
                }
            }
        }
        if (to->reference_lines[i]) {
            if (target->reference_lines[i]) {
                kv_assert(per_line_data(line)->referenced_levels[target->idx]);
            } else {
                kv_assert(per_line_data(line)->referenced_levels[to->idx]);
                per_line_data(line)->referenced_levels[to->idx] = false;
            }
            to->reference_lines[i] = false;
        }
    }
}

void do_lksv3_compaction2(struct ssd *ssd, int high_lev, int low_lev, leveling_node *l_node, lksv3_level *target) {
    lksv3_level *l;
    lev_iter *iter;
    lksv3_run_t *now;

    if (l_node) {
        _do_lksv3_compaction2(ssd, NULL, lksv_lsm->disk[low_lev], target, l_node->mem);
    } else {
        _do_lksv3_compaction2(ssd, lksv_lsm->disk[high_lev], lksv_lsm->disk[low_lev], target, NULL);
    }

    qemu_mutex_lock(&ssd->comp_mu);
    kv_assert(lksv_lsm->c_level == NULL);
    lksv_lsm->c_level = target;
    qemu_mutex_unlock(&ssd->comp_mu);

    wait_pending_reads(ssd);
    l = lksv_lsm->disk[low_lev];
    iter = lksv3_get_iter(l, l->start, l->end);
    int j = 0;

    while((now=lksv3_iter_nxt(iter))) {
        qemu_mutex_lock(&ssd->comp_mu);
        for (int i = 0; i < PG_N; i++) {
            struct femu_ppa ppa = get_next_write_ppa(ssd, now->ppa, i);
            if (lksv3_get_pg(ssd, &ppa)->status == PG_VALID) {
                lksv3_mark_page_invalid(ssd, &ppa);
            }
        }
        j++;
        qemu_mutex_unlock(&ssd->comp_mu);
        if (j % 2 == 0)
            wait_pending_reads(ssd);
    }
    if (!l_node) {
        l = lksv_lsm->disk[high_lev];
        iter = lksv3_get_iter(l, l->start, l->end);
        while((now=lksv3_iter_nxt(iter))) {
            qemu_mutex_lock(&ssd->comp_mu);
            for (int i = 0; i < PG_N; i++) {
                struct femu_ppa ppa = get_next_write_ppa(ssd, now->ppa, i);
                if (lksv3_get_pg(ssd, &ppa)->status == PG_VALID) {
                    lksv3_mark_page_invalid(ssd, &ppa);
                }
            }
            qemu_mutex_unlock(&ssd->comp_mu);
            j++;
            if (j % 2 == 0)
                wait_pending_reads(ssd);
        }
    }

    qemu_mutex_lock(&ssd->comp_mu);

    l = target;
    iter = lksv3_get_iter(l, l->start, l->end);
    while((now=lksv3_iter_nxt(iter))) {
        if (lksv3_should_meta_gc_high(ssd)) {
            lksv3_gc_meta_femu(ssd);
        }
        if (ssd->lm.meta.free_line_cnt < 1) {
            move_line_d2m(ssd, true);
        }
        kv_assert(is_pivot_ppa(ssd, lksv3_get_new_meta_page(ssd)));
        kv_assert(now->hash_list_n > 0);
        kv_assert(now->hash_list_n <= PG_N);
        struct femu_ppa fppa;

        for (int i = 0; i < now->hash_list_n; i++) {
            target->vsize += ((lksv_block_footer *) (now->buffer[i] + (PAGESIZE - LKSV3_SSTABLE_FOOTER_BLK_SIZE)))->g.n * (lksv_lsm->avg_value_bytes + lksv_lsm->avg_key_bytes + 20);

            fppa = lksv3_compaction_meta_segment_write_femu(ssd, (char *) now->buffer[i], target->idx);
            now->buffer[i] = NULL;

            if (i == 0) {
                now->ppa = fppa;
                kv_assert(is_pivot_ppa(ssd, fppa));
            }
        }

        for (int i = now->hash_list_n; i < PG_N; i++) {
            kv_assert(i > 0);
            fppa = lksv3_get_new_meta_page(ssd);
            lksv3_mark_page_valid(ssd, &fppa);
            lksv3_ssd_advance_write_pointer(ssd, &ssd->lm.meta);
            lksv3_mark_page_invalid(ssd, &fppa);
        }

        j++;
        if (j % 2 == 0) {
            qemu_mutex_unlock(&ssd->comp_mu);
            wait_pending_reads(ssd);
            qemu_mutex_lock(&ssd->comp_mu);
        }
    }
}

