#include "hw/femu/kvssd/lksv/lksv3_ftl.h"
#include <math.h>
#include "hw/femu/kvssd/lksv/skiplist.h"

struct lksv3_lsmtree *lksv_lsm;

void lksv3_lsm_create(struct ssd *ssd)
{
    lksv3_llp *llp = &ssd->llp;

    lksv3_lsm_setup_params(ssd);

    float m_num = 1;
    uint64_t all_header_num = 0;
    lksv_lsm->disk = (lksv3_level**) malloc(sizeof(lksv3_level*) * LSM_LEVELN);
    kv_debug("|-----LSMTREE params ---------\n");
    // TODO: apply last level to different size factor
    for (int i = 0; i < LSM_LEVELN; i++) {
        int m = ceil(m_num * llp->size_factor);

        lksv_lsm->disk[i] = lksv3_level_init(m, i);
        kv_debug("| [%d] noe:%d\n", i, lksv_lsm->disk[i]->m_num);
        all_header_num += lksv_lsm->disk[i]->m_num;
        m_num *= llp->size_factor;
    }

    kv_debug("| level:%d sizefactor:%lf last:%lf\n",LSM_LEVELN, llp->size_factor, llp->last_size_factor);
    //uint64_t level_bytes = all_header_num * lsp->ONESEGMENT;
    //kv_debug("| all level size:%lu(MB), %lf(GB)", level_bytes / M, (double) level_bytes / G);
    // TODO: all level header size
    //kv_debug(" target size: %lf(GB)\n",(double) ssd->sp.tt_pgs * PAGESIZE / G);
    //kv_debug("| level cache :%luMB(%lu page)%.2f(%%)\n",lsp->pin_memory/M,lsp->pin_memory/PAGESIZE,(float)lsp->pin_memory/lsp->total_memory*100);
    //kv_debug("| entry cache :%luMB(%lu page)%.2f(%%)\n",lsp->cache_memory/M,lsp->cache_memory/PAGESIZE,(float)lsp->cache_memory/lsp->total_memory*100);
    kv_debug("| -------- algorithm_log END\n\n");

    kv_debug("SHOWINGSIZE(GB) :%lu HEADERSEG:%d DATASEG:%d\n", ((unsigned long) ssd->sp.tt_pgs * 9 / 10 * PAGESIZE) / G, ssd->sp.meta_lines, ssd->sp.data_lines);
    kv_debug("LEVELN:%d\n", LSM_LEVELN);

    lksv3_compaction_init(ssd);

    lksv_lsm->lsm_cache = kv_cache_init(lksv_lsm->opts->cache_memory_size, LSM_LEVELN * CACHE_TYPES);
    lksv_lsm->avg_value_bytes = 1024;
    lksv_lsm->avg_key_bytes = 32;
    lksv_lsm->sum_value_bytes = 0;
    lksv_lsm->sum_key_bytes = 0;
    lksv_lsm->samples_count = 0;
}

struct lksv3_sorted_string {
    int tt_keys;
    lksv3_sst_str_idx_t *str_order_map;
    struct nand_page *pg[PG_N];
    lksv3_sst_t sst[PG_N];
    int s;
    int e;
};

static struct lksv3_sorted_string *sort_run(struct ssd *ssd, lksv3_run_t *entry, struct range_lun *luns) {
    struct lksv3_sorted_string *ss = g_malloc0(sizeof(struct lksv3_sorted_string));

    for (int k = 0; k < entry->hash_list_n; k++) {
        struct femu_ppa cppa = get_next_write_ppa(ssd, entry->ppa, k);
        luns->read[cppa.g.ch][cppa.g.lun]++;

        ss->pg[k] = lksv3_get_pg(ssd, &cppa);
        lksv3_sst_decode(&ss->sst[k], ss->pg[k]->data);

        ss->tt_keys += ss->sst[k].footer.g.n;
    }
    ss->e = ss->tt_keys - 1;

    ss->str_order_map = g_malloc0(sizeof(lksv3_sst_str_idx_t) * ss->tt_keys);
    int n = 0;
    for (int k = 0; k < entry->hash_list_n; k++) {
        memcpy(ss->str_order_map + n, ss->sst[k].str_idx, sizeof(lksv3_sst_str_idx_t) * ss->sst[k].footer.g.n);
        n += ss->sst[k].footer.g.n;
    }

    return ss;
}

uint8_t lksv3_lsm_scan_run(struct ssd *ssd, kv_key key, lksv3_run_t **entry, lksv3_run_t *up_entry, keyset **found, int *level, NvmeRequest *req) {
    lksv3_run_t *entries=NULL;
    struct range_lun luns;
    memset(&luns.read, 0, sizeof(struct range_lun));

    struct kv_skiplist *skip = kv_skiplist_init();

    lksv3_run_t *level_ent[10] = {NULL, };

    for(int i = *level; i < LSM_LEVELN; i++){
        entries = lksv3_find_run(lksv_lsm->disk[i], key, ssd, req);
        if(!entries) {
            continue;
        }

        level_ent[i] = entries;
        kv_assert(level_ent[i] == &((array_body *)lksv_lsm->disk[i]->level_data)->arrs[
            entries - &(((array_body *)lksv_lsm->disk[i]->level_data)->arrs[0])
        ]);
    }

    bool *already_read = g_malloc0(ssd->sp.tt_pgs);
    int i = 10;
    while (true) {
next_level:
        if (--i < 0) {
            break;
        }
        if (!level_ent[i]) {
            continue;
        }

        bool first_in_level = true;

        struct lksv3_sorted_string *ss;
        int run_n_num;
advance_in_level:
        entries = level_ent[i];
        run_n_num = entries - &(((array_body *)lksv_lsm->disk[i]->level_data)->arrs[0]);
        kv_assert(level_ent[i] == &((array_body *)lksv_lsm->disk[i]->level_data)->arrs[run_n_num]);

        ss = sort_run(ssd, entries, &luns);

        int s = ss->s; int e = ss->e;
        int mid = (s + e) / 2;
        int res;
        if (first_in_level) {
            while (s <= e) {
                mid = (s + e) / 2;
                kv_key tkey;
                int k = ss->str_order_map[mid].g1.sst;
                int n = ss->str_order_map[mid].g1.off;
                int sst = ss->sst[k].str_idx[n].g1.sst;
                int off = ss->sst[k].str_idx[n].g1.off;
                tkey.key = ss->pg[sst]->data + ss->sst[sst].meta[off].g1.off;
                tkey.len = ss->sst[sst].meta[off].g1.klen;
                res = kv_cmp_key(tkey, key);
                if (res == 0) {
                    break;
                } else if (res < 0) {
                    s = mid + 1;
                } else {
                    e = mid - 1;
                }
            }
        } else {
            mid = ss->s;
        }
        kv_assert(mid >= 0 && mid < ss->tt_keys);
        first_in_level = false;

        while (skip->n < RQ_MAX) {
            int k = ss->str_order_map[mid].g1.sst;
            int n = ss->str_order_map[mid].g1.off;
            int sst = ss->sst[k].str_idx[n].g1.sst;
            int off = ss->sst[k].str_idx[n].g1.off;
            if (mid >= ss->e) {
                goto try_advance_in_level;
            }

            kv_key tkey;
            kv_value *v;
            kv_snode *t;

            tkey.len = ss->sst[sst].meta[off].g1.klen;
            tkey.key = calloc(1, tkey.len);
            memcpy(tkey.key, ss->pg[sst]->data + ss->sst[sst].meta[off].g1.off, tkey.len);
            v = calloc(1, sizeof(kv_value));
            v->length = ss->sst[sst].meta[off].g2.slen;
            t = lksv3_skiplist_insert(skip, tkey, v, true, ssd);
            if (ss->sst[sst].meta[off].g1.flag == VLOG) {
                snode_ppa(t)->ppa = *(uint32_t *) (ss->pg[sst]->data + ss->sst[sst].meta[off].g1.off + ss->sst[sst].meta[off].g1.klen);
            } else {
                snode_ppa(t)->ppa = UNMAPPED_PPA;
            }

            mid++;
        }

        while (mid < ss->e) {
            kv_key tkey;
            int k = ss->str_order_map[mid].g1.sst;
            int n = ss->str_order_map[mid].g1.off;
            int sst = ss->sst[k].str_idx[n].g1.sst;
            int off = ss->sst[k].str_idx[n].g1.off;
            tkey.len = ss->sst[sst].meta[off].g1.klen;
            tkey.key = calloc(1, tkey.len);
            memcpy(tkey.key, ss->pg[sst]->data + ss->sst[sst].meta[off].g1.off, tkey.len);

            res = kv_cmp_key(tkey, skip->header->back->key);
            if (res <= 0) {
                kv_value *v;
                kv_snode *t;

                v = calloc(1, sizeof(kv_value));
                v->length = ss->sst[sst].meta[off].g2.slen;
                t = lksv3_skiplist_insert(skip, tkey, v, true, ssd);
                if (ss->sst[sst].meta[off].g1.flag == VLOG) {
                    snode_ppa(t)->ppa = *(uint32_t *) (ss->pg[sst]->data + ss->sst[sst].meta[off].g1.off + ss->sst[sst].meta[off].g1.klen);
                } else {
                    snode_ppa(t)->ppa = UNMAPPED_PPA;
                }

                mid++;
                // TODO: Delete last key.
            } else {
                g_free(tkey.key);
                for (int k = 0; k < entries->hash_list_n; k++) {
                    g_free(ss->sst[k].meta);
                }
                g_free(ss->str_order_map);
                g_free(ss);
                goto next_level;
            }
        }

try_advance_in_level:
        for (int k = 0; k < entries->hash_list_n; k++) {
            g_free(ss->sst[k].meta);
        }
        g_free(ss->str_order_map);
        g_free(ss);

        if (run_n_num >= lksv_lsm->disk[i]->n_num - 2) {
            continue;
        }
        level_ent[i] = &((array_body *)lksv_lsm->disk[i]->level_data)->arrs[run_n_num + 1];

        goto advance_in_level;
    }

    kv_snode *t = NULL;
    int n_read = 0;
    for_each_sk (t, skip) {
        n_read++;

        if (snode_ppa(t)->ppa == UNMAPPED_PPA) {
            continue;
        }

        struct ssdparams *spp = &ssd->sp;
        uint32_t pgidx = snode_ppa(t)->g.ch  * spp->pgs_per_ch  + \
                         snode_ppa(t)->g.lun * spp->pgs_per_lun + \
                         snode_ppa(t)->g.pl  * spp->pgs_per_pl  + \
                         snode_ppa(t)->g.blk * spp->pgs_per_blk + \
                         snode_ppa(t)->g.pg;
        kv_assert(pgidx < ssd->sp.tt_pgs);

        if (!already_read[pgidx]) {
            luns.read[snode_ppa(t)->g.ch][snode_ppa(t)->g.lun]++;
            already_read[pgidx] = true;
        }
        if (n_read >= RQ_MAX) {
            break;
        }
    }

    stat_range_lun(&luns);

    struct nand_cmd srd;
    srd.type = USER_IO;
    srd.cmd = NAND_READ;
    srd.stime = req->etime;
    int64_t max = 0;

    struct femu_ppa ppa = { .ppa = 0 };
    for (; ppa.g.ch < 8; ppa.g.ch++) {
        for (; ppa.g.lun < 8; ppa.g.lun++) {
            while (luns.read[ppa.g.ch][ppa.g.lun] > 0) {
                req->flash_access_count++;
                uint64_t sublat = lksv3_ssd_advance_status(ssd, &ppa, &srd); 
                if (max < sublat) {
                    max = sublat;
                }
                //req->etime += sublat;
                luns.read[ppa.g.ch][ppa.g.lun]--;
            }
        }
        ppa.g.lun = 0;
    }

    req->etime += max;

    free(already_read);

    //printf("req->access_count: %d\n", req->flash_access_count);

    kv_skiplist_free(skip);

    return FOUND;
}

uint8_t lksv3_lsm_find_run(struct ssd *ssd, kv_key key, lksv3_run_t **entry, lksv3_run_t *up_entry, keyset **found, int *level, NvmeRequest *req) {
    lksv3_run_t *entries=NULL;

    uint32_t hash;
    hash = XXH32(key.key, key.len, 0);

    for(int i = *level; i < LSM_LEVELN; i++){
        /* 
         * If we have a upper level entry, then use a range pointer to reduce
         * a range to search.
         */
        if (up_entry)
            entries = lksv3_find_run_se(lksv_lsm, lksv_lsm->disk[i], key, up_entry, ssd, req);
        else
            entries = lksv3_find_run(lksv_lsm->disk[i], key, ssd, req);

        /*
         * If we failed to find a run_t in a lower layer, then we lost
         * a range pointer. Clear the up_entry info.
         */
        if(!entries) {
            up_entry = NULL;
            continue;
        }

        if (entries->ppa.ppa == UNMAPPED_PPA) {
            entries = lksv3_find_run(lksv_lsm->c_level, key, ssd, req);
            if (level) {
                *level = i = lksv_lsm->c_level->idx;
            }
            if (!entries) {
                up_entry = NULL;
                continue;
            }

            keyset *find = lksv3_find_keyset(ssd, req, entries, key, hash, i);
            if (find) {
                *found = find;
                if (level) *level = i;
                *entry = entries;
                // TODO: someone need to free(find)
                if (find->ppa.ppa != UNMAPPED_PPA) {
                    struct nand_cmd srd;
                    srd.type = USER_IO;
                    srd.cmd = NAND_READ;
                    srd.stime = req->etime;
                    req->flash_access_count++;
                    uint64_t sublat = lksv3_ssd_advance_status(ssd, &find->ppa, &srd); 
                    req->etime += sublat;

                    kv_assert(check_voffset(ssd, &find->ppa, find->voff, find->hash));
                }
                return COMP_FOUND;
            } else {
                /*
                 * Met range overlapped run_t but no key in there.
                 */
                up_entry = NULL;
                continue;
            }
        }

        keyset *find = lksv3_find_keyset(ssd, req, entries, key, hash, i);
        bool flash_access_for_caching = false;
        // We don't need to query membership to the last level's entry.
        if (i < lksv_lsm->bottom_level) {
            if (kv_is_cached(lksv_lsm->lsm_cache, entries->cache[HASH_LIST])) {
                lksv_lsm->cache_hit++;
                if (lksv_lsm->cache_hit % 1000000 == 0)
                    kv_debug("cache hit ratio: %lu\n", lksv_lsm->cache_hit * 100 / (lksv_lsm->cache_hit + lksv_lsm->cache_miss));
            } else if (kv_cache_available(lksv_lsm->lsm_cache, cache_level(HASH_LIST, i))) {
                uint32_t entry_size = (entries->n * HASH_BYTES) + 20;
                kv_cache_insert(lksv_lsm->lsm_cache, &entries->cache[HASH_LIST], entry_size, cache_level(HASH_LIST, i), KV_CACHE_WITHOUT_FLAGS);
                if (entries->cache[HASH_LIST])
                    flash_access_for_caching = true;
                lksv_lsm->cache_miss++;
            }
        }
        if (kv_cache_available(lksv_lsm->lsm_cache, cache_level(DATA_SEGMENT_GROUP, i)) && !kv_is_cached(lksv_lsm->lsm_cache, entries->cache[DATA_SEGMENT_GROUP])) {
            uint32_t entry_size = PG_N * PAGESIZE;
            kv_cache_insert(lksv_lsm->lsm_cache, &entries->cache[DATA_SEGMENT_GROUP], entry_size, cache_level(DATA_SEGMENT_GROUP, i), KV_CACHE_WITHOUT_FLAGS);
            if (entries->cache[DATA_SEGMENT_GROUP])
                flash_access_for_caching = true;
        }
        if (flash_access_for_caching) {
            for (int k = 0; k < entries->hash_list_n; k++) {
                struct femu_ppa cppa = get_next_write_ppa(ssd, entries->ppa, k);
                struct nand_cmd srd;
                srd.type = USER_IO;
                srd.cmd = NAND_READ;
                srd.stime = 0;
                lksv3_ssd_advance_status(ssd, &cppa, &srd);
            }
        }

        if (find) {
            *found = find;
            if (level) *level = i;
            // TODO: someone need to free(find)

            if (find->ppa.ppa != UNMAPPED_PPA) {
                //struct nand_page *pg2 = lksv3_get_pg(ssd, &find->ppa);
                struct nand_cmd srd;
                srd.type = USER_IO;
                srd.cmd = NAND_READ;
                srd.stime = req->etime;
                req->flash_access_count++;
                uint64_t sublat = lksv3_ssd_advance_status(ssd, &find->ppa, &srd); 
                req->etime += sublat;

                kv_assert(check_voffset(ssd, &find->ppa, find->voff, find->hash));
            }
            return FOUND;
        } else {
            /*
             * Met range overlapped run_t but no key in there.
             */
            up_entry = entries;
        }
    }
    return NOTFOUND;
}
