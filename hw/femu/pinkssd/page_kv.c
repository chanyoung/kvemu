#include "hw/femu/kvssd/pink/pink_ftl.h"

static void gc_read_delay(struct ssd *ssd, struct femu_ppa *ppa)
{
    if (ssd->sp.enable_gc_delay) {
        struct nand_cmd gcr;
        gcr.type = GC_IO;
        gcr.cmd = NAND_READ;
        gcr.stime = 0;
        pink_ssd_advance_status(ssd, ppa, &gcr);
    }
}

static void gc_write_delay(struct ssd *ssd, struct femu_ppa *ppa)
{
    if (ssd->sp.enable_gc_delay) {
        struct nand_cmd gcw;
        gcw.type = GC_IO;
        gcw.cmd = NAND_WRITE;
        gcw.stime = 0;
        pink_ssd_advance_status(ssd, ppa, &gcw);
    }
}

static void gc_erase_delay(struct ssd *ssd, struct femu_ppa *ppa)
{
    if (ssd->sp.enable_gc_delay) {
        struct nand_cmd gce;
        gce.type = GC_IO;
        gce.cmd = NAND_ERASE;
        gce.stime = 0;
        pink_ssd_advance_status(ssd, ppa, &gce);
    }
}

static bool is_data_valid(struct ssd *ssd, kv_key key, struct femu_ppa ppa, int idx) {
    qemu_mutex_lock(&ssd->memtable_mu);
    kv_snode *target_node = kv_skiplist_find(pink_lsm->memtable, key);
    if (target_node) {
        qemu_mutex_unlock(&ssd->memtable_mu);
        return false;
    }

    for (int z = 0; z < pink_lsm->temp_n; z++) {
        target_node = kv_skiplist_find(pink_lsm->temptable[z], key);
        if (target_node) {
            qemu_mutex_unlock(&ssd->memtable_mu);
            return false;
        }
    }
    qemu_mutex_unlock(&ssd->memtable_mu);

    pink_run_t *entries = NULL;
    pink_run_t *up_entry = NULL;
    for (int i = 0; i < LSM_LEVELN; i++) {
        if (up_entry) {
            entries = find_run_se(pink_lsm, pink_lsm->disk[i], key, up_entry, ssd, NULL);
        } else {
            entries = find_run(pink_lsm->disk[i], key, ssd, NULL);
        }

        up_entry = NULL;
        if (entries == NULL) {
            continue;
        }

        if (entries->buffer == NULL && entries->ppa.ppa == UNMAPPED_PPA) {
            entries = find_run2(pink_lsm->c_level, key, ssd, NULL);
            i = pink_lsm->c_level->idx;
            if (!entries) {
                continue;
            }
        }

        keyset *find;
        bool valid;
        if (entries->buffer) {
            find = find_keyset(entries->buffer, key);
            if (find) {
                valid = find->ppa.ppa == ppa.ppa;
                FREE(find);
                return valid;
            } else {
                up_entry = entries;
                continue;
            }
        }

        struct nand_page *pg;
        pg = get_pg(ssd, &entries->ppa);
        if (!kv_is_cached(pink_lsm->lsm_cache, entries->cache[META_SEGMENT])) {
            gc_read_delay(ssd, &entries->ppa);
            if (kv_cache_available(pink_lsm->lsm_cache, cache_level(META_SEGMENT, i))) {
               if (!kv_level_being_compacted_without_unlock(&pink_lsm->comp_ctx, i))
                   kv_cache_insert(pink_lsm->lsm_cache, &entries->cache[META_SEGMENT], PAGESIZE, cache_level(META_SEGMENT, i), KV_CACHE_WITHOUT_FLAGS);
               kv_unlock_compaction_info(&pink_lsm->comp_ctx);
            }
            pink_lsm->cache_miss++;
        } else {
#ifdef CACHE_UPDATE
            kv_cache_update(pink_lsm->lsm_cache, entries->cache[META_SEGMENT]);
#endif
            pink_lsm->cache_hit++;
            if (pink_lsm->cache_hit % 10000 == 0) {
                kv_debug("cache hit ratio: %lu\n", pink_lsm->cache_hit * 100 / (pink_lsm->cache_hit + pink_lsm->cache_miss));
            }
        }

        find = find_keyset((char *)pg->data, key);
        if (find) {
            valid = find->ppa.ppa == ppa.ppa;
            FREE(find);
            return valid;
        } else {
            up_entry = entries;
        }
    }

    // The key is deleted?
    // We don't have delete workload now. so just abort it.
    kv_debug("Key is deleted?\n");
    abort();
    return false;
}

int gc_erased;
int gc_moved;

static void gc_data_one_block(struct ssd *ssd, struct femu_ppa ppa)
{
    struct ssdparams *spp = &ssd->sp;

    struct nand_page *pg;
    for (int pg_n = 0; pg_n < spp->pgs_per_blk; pg_n++) {
        ppa.g.pg = pg_n;
        pg = get_pg(ssd, &ppa);
        gc_read_delay(ssd, &ppa);
        kv_assert(pg->status != PG_FREE);

        int nheaders = ((uint16_t *)pg->data)[0];
        for (int i = 0; i < nheaders; i++) {
            kv_key key;
            key.len = ((uint16_t *)pg->data)[(nheaders+2)+i+1];
            key.key = pg->data + ((uint16_t *)pg->data)[i+1];

            bool valid = is_data_valid(ssd, key, ppa, i);
            if (valid) {
                gc_moved++;

                key.key = (char*) calloc(key.len+1, sizeof(char));
                memcpy(key.key, pg->data + ((uint16_t *)pg->data)[i+1], key.len);

                kv_value *value = (kv_value*) calloc(1, sizeof(kv_value));
                value->length = ((uint16_t *)pg->data)[i+2] - ((uint16_t *)pg->data)[i+1] - key.len;
                value->value = (char *) calloc(value->length, sizeof(char));
                memcpy(value->value, pg->data + ((uint16_t *)pg->data)[i+1] + key.len, value->length);

                // Give our m allocated key to skiplist.
                // No need to free that.
                //compaction_check(ssd);
                qemu_mutex_lock(&ssd->memtable_mu);
                kv_skiplist_insert(pink_lsm->memtable, key, value);
                qemu_mutex_unlock(&ssd->memtable_mu);
            } else {
                gc_erased++;
            }
        }
        // Free page~!
        FREE(pg->data);
        pg->data = NULL;
        pg->status = PG_FREE;
    }
}

int gc_data_femu(struct ssd *ssd) {
    struct line *victim_line = NULL;
    struct ssdparams *spp = &ssd->sp;
    struct femu_ppa ppa;
    //int ch, lun, pg_n;
    int ch, lun;

    victim_line = select_victim_data_line(ssd, true);
    if (victim_line == NULL) {
        return -1;
    }
    kv_log("%d gc_data!!!!!!!\n", ++pink_lsm->data_gc_cnt);
    kv_log("vpc: %d, vsc: %d, isc: %d, secs_per_line: %d\n", victim_line->vpc, victim_line->vsc, victim_line->isc, spp->secs_per_line);

    gc_moved = 0;
    gc_erased = 0;

    //qemu_mutex_unlock(&ssd->comp_mu);
    //wait_delay(ssd, true);
    //qemu_mutex_lock(&ssd->comp_mu);

    ppa.g.blk = victim_line->id;
    //struct nand_page *pg;
    for (ch = 0; ch < spp->nchs; ch++) {
        for (lun = 0; lun < spp->luns_per_ch; lun++) {
            qemu_mutex_unlock(&ssd->comp_mu);
            wait_pending_reads(ssd);
            qemu_mutex_lock(&ssd->comp_mu);

            ppa.g.ch = ch;
            ppa.g.lun = lun;
            ppa.g.pl = 0;
            ppa.g.sec = 0;
            ppa.g.rsv = 0;

            gc_data_one_block(ssd, ppa);
            gc_erase_delay(ssd, &ppa);
            mark_block_free(ssd, &ppa);

            //compaction_check(ssd);
        }
    }
    mark_line_free(ssd, &ppa);

    //qemu_mutex_unlock(&ssd->comp_mu);
    //wait_delay(ssd, true);
    //qemu_mutex_lock(&ssd->comp_mu);

    kv_log("gc_data_erased: %d, gc_data_moved: %d, moved percentage: %f\n", gc_erased, gc_moved, ((float)gc_moved) / (gc_moved + gc_erased));
    return gc_erased > 0 ? 0 : -2;
}

/*
 * TODO: We need to implement the case where the level list entry is
 * stored in flash and needs to be garbage collected. Since there was
 * no noticeable difference in the experimental results when implemented
 * in a straightforward manner, so we just skipped now.
 */
int gc_meta_femu(struct ssd *ssd) {
    struct line *victim_line = NULL;
    struct ssdparams *spp = &ssd->sp;
    struct femu_ppa ppa;
    int ch, lun, pg_n;

    victim_line = select_victim_meta_line(ssd, true);
    if (victim_line == NULL) {
        return -1;
    }
    kv_log("%d gc_meta! invalid_pgs / pgs_per_line: %d / %d \n", ++pink_lsm->header_gc_cnt, victim_line->ipc, ssd->sp.pgs_per_line);

    ppa.g.blk = victim_line->id;

    int i = 0;

    /* copy back valid data */
    struct nand_page *pg;
    for (ch = 0; ch < spp->nchs; ch++) {
        for (lun = 0; lun < spp->luns_per_ch; lun++) {
            ppa.g.ch = ch;
            ppa.g.lun = lun;
            ppa.g.pl = 0;
            ppa.g.sec = 0;
            ppa.g.rsv = 0;

            for (pg_n = 0; pg_n < spp->pgs_per_blk; pg_n++) {
                if (pg_n % ASYNC_IO_UNIT == 0) {
                    qemu_mutex_unlock(&ssd->comp_mu);
                    wait_pending_reads(ssd);
                    qemu_mutex_lock(&ssd->comp_mu);
                }

                ppa.g.pg = pg_n;
                pg = get_pg(ssd, &ppa);
                kv_assert(pg->status != PG_FREE);
                if (pg->status == PG_INVALID) {
                    kv_assert(pg->data == NULL);
                    pg->status = PG_FREE;
                    continue;
                }
                gc_read_delay(ssd, &ppa);

                kv_key first_key_in_meta;
                uint16_t *bitmap = (uint16_t *) pg->data;
                char *body = (char *) pg->data;
                first_key_in_meta.len = bitmap[2] - bitmap[1] - sizeof(struct femu_ppa);
                first_key_in_meta.key = &body[bitmap[1] + sizeof(struct femu_ppa)];

                pink_run_t *entries = NULL;
                pink_run_t *target_entry = NULL;
                bool checkdone = false;
                bool shouldwrite = false;
                for(int j = 0; j < LSM_LEVELN; j++) {
                    entries = find_run(pink_lsm->disk[j], first_key_in_meta, ssd, NULL);
                    if (entries == NULL) {
                        continue;
                    }
                    if (entries->ppa.ppa == ppa.ppa && kv_test_key(entries->key, first_key_in_meta)) {
                        checkdone = true;
                        target_entry = entries;
                        shouldwrite = true;
                        break;
                    }
                    if (checkdone)
                        break;
                }

                if (!checkdone && pink_lsm->c_level) {
                    entries = find_run(pink_lsm->c_level, first_key_in_meta, ssd, NULL);
                    if (entries && entries->ppa.ppa == ppa.ppa) {
                        checkdone = true;
                        shouldwrite = true;
                        target_entry = entries;
                    }
                }

                if (!checkdone) {
                    // Lost mapping between run and meta segment
                    kv_debug("Lost mapping between run and meta segment\n");
                    abort();
                }

                if (shouldwrite) {
                    struct femu_ppa new_ppa;
                    struct nand_page *new_pg;

                    new_ppa = get_new_meta_page(ssd);
                    new_pg = get_pg(ssd, &new_ppa);
                    gc_write_delay(ssd, &new_ppa);

                    new_pg->data = pg->data;
                    pg->data = NULL;

                    target_entry->ppa = new_ppa;
                }

                kv_assert(pg->data == NULL);
                pg->status = PG_FREE;

                i++;
            }

            gc_erase_delay(ssd, &ppa);
            mark_block_free(ssd, &ppa);
            // should set gc end time.
        }
    }

    mark_line_free(ssd, &ppa);

    //qemu_mutex_unlock(&ssd->comp_mu);
    //wait_delay(ssd, true);
    //qemu_mutex_lock(&ssd->comp_mu);

    return 0;
}

