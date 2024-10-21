#include "hw/femu/kvssd/pink/pink_ftl.h"
#include "hw/femu/kvssd/pink/skiplist.h"
#include <math.h>

struct pink_lsmtree *pink_lsm;

void pink_lsm_adjust_level_multiplier(void)
{
    struct pink_level *floor = pink_lsm->disk[LSM_LEVELN-1];

    if (floor->n_num > floor->m_num * 9 / 10) {
        kv_debug("increase level multiplier %.2f to %.2f\n",
                pink_lsm->opts->level_multiplier,
                pink_lsm->opts->level_multiplier + 1);
        pink_lsm->opts->level_multiplier += 1;
    }
}

void pink_lsm_create(struct ssd *ssd)
{
    pink_lsm->ssd = ssd;

    float m_num = 1;
    uint64_t all_header_num = 0;
    pink_lsm->disk = (pink_level**) malloc(sizeof(pink_level*) * LSM_LEVELN);
    kv_debug("|-----LSMTREE params ---------\n");
    // TODO: apply last level to different size factor
    for (int i = 0; i < LSM_LEVELN; i++) {
        pink_lsm->disk[i] = level_init(i);
        kv_debug("| [%d] noe:%d\n", i, pink_lsm->disk[i]->m_num);
        all_header_num += pink_lsm->disk[i]->m_num;
        m_num *= pink_lsm->opts->level_multiplier;
    }

    kv_debug("| level:%d sizefactor:%lf\n",LSM_LEVELN, pink_lsm->opts->level_multiplier);
    kv_debug("| -------- algorithm_log END\n\n");

    kv_debug("SHOWINGSIZE(GB) :%lu HEADERSEG:%d DATASEG:%d\n", ((unsigned long) ssd->sp.tt_pgs * 9 / 10 * PAGESIZE) / G, ssd->sp.meta_lines, ssd->sp.data_lines);
    kv_debug("LEVELN:%d\n", LSM_LEVELN); 

    compaction_init(ssd);

    pink_lsm->lsm_cache = kv_cache_init(pink_lsm->opts->cache_memory_size, LSM_LEVELN * CACHE_TYPES);
    pink_lsm->lsm_cache->flush_callback = pink_flush_cache_when_evicted;
}

uint8_t lsm_scan_run(struct ssd *ssd, kv_key key, pink_run_t **entry, pink_run_t *up_entry, keyset **found, int *level, NvmeRequest *req) {
    pink_run_t *entries=NULL;
    struct range_lun luns;
    memset(&luns.read, 0, sizeof(struct range_lun));

    struct kv_skiplist *skip = kv_skiplist_init();

    pink_run_t *level_ent[10] = {NULL, };

    for(int i = *level; i < LSM_LEVELN; i++){
        entries = find_run(pink_lsm->disk[i], key, ssd, req);
        if(!entries) {
            continue;
        }

        level_ent[i] = entries;
        kv_assert(level_ent[i] == &((array_body *)pink_lsm->disk[i]->level_data)->arrs[
            entries - &(((array_body *)pink_lsm->disk[i]->level_data)->arrs[0])
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

        int run_idx = -1;
        int run_n_num;
advance_in_level:
        entries = level_ent[i];
        run_n_num = entries - &(((array_body *)pink_lsm->disk[i]->level_data)->arrs[0]);
        kv_assert(level_ent[i] == &((array_body *)pink_lsm->disk[i]->level_data)->arrs[run_n_num]);

        char *body = entries->buffer;
        if (!body) {
            if (entries->ppa.ppa == UNMAPPED_PPA) {
                printf("WARN!!\n");
                continue;
            } else {
                body = get_pg(ssd, &entries->ppa)->data;
            }
            luns.read[entries->ppa.g.ch][entries->ppa.g.lun]++;
        }

        uint16_t *bitmap = (uint16_t*)body;
        uint16_t num_keys = bitmap[0];
        kv_key tkey;

        if (run_idx == -1) {
            int s = 1; int e = num_keys;
            int mid = 0;
            int res = 0;
            while (s <= e) {
                mid = (s + e) / 2;
                tkey.key = &body[bitmap[mid] + sizeof(struct femu_ppa)];
                tkey.len = bitmap[mid+1] - bitmap[mid] - sizeof(struct femu_ppa);
                res = kv_cmp_key(tkey, key);
                if (res == 0) {
                    break;
                } else if (res < 0) {
                    s = mid + 1;
                } else {
                    e = mid - 1;
                }
            }
            kv_assert(mid > 0 && mid <= num_keys);
            run_idx = mid;
        } else {
            run_idx = 1;
        }

        kv_value *v;
        kv_snode *t;

        while (skip->n < RQ_MAX) {
            if (run_idx >= num_keys) {
                goto try_advance_in_level;
            }

            tkey.len = bitmap[run_idx+1] - bitmap[run_idx] - sizeof(struct femu_ppa);
            tkey.key = g_malloc0(tkey.len);
            memcpy(tkey.key, &body[bitmap[run_idx] + sizeof(struct femu_ppa)], tkey.len);
            v = g_malloc0(sizeof(kv_value));
            v->length = 0;
            t = kv_skiplist_insert(skip, tkey, v);
            memcpy(snode_ppa(t), &body[bitmap[run_idx]], sizeof(struct femu_ppa));

            run_idx++;
        }

        for (; run_idx <= num_keys; run_idx++) {
            tkey.len = bitmap[run_idx+1] - bitmap[run_idx] - sizeof(struct femu_ppa);
            tkey.key = g_malloc0(tkey.len);
            memcpy(tkey.key, &body[bitmap[run_idx] + sizeof(struct femu_ppa)], tkey.len);

            int res = kv_cmp_key(tkey, skip->header->back->key);
            if (res > 0) {
                g_free(tkey.key);
                goto next_level;
            }

            v = g_malloc0(sizeof(kv_value));
            v->length = 0;
            t = kv_skiplist_insert(skip, tkey, v);
            memcpy(snode_ppa(t), &body[bitmap[run_idx]], sizeof(struct femu_ppa));
        }

try_advance_in_level:
        if (run_n_num >= pink_lsm->disk[i]->n_num - 2) {
            continue;
        }
        level_ent[i] = &((array_body *)pink_lsm->disk[i]->level_data)->arrs[run_n_num + 1];

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
                uint64_t sublat = pink_ssd_advance_status(ssd, &ppa, &srd); 
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

    return 0;
}

uint8_t lsm_find_run(struct ssd *ssd, kv_key key, pink_run_t **entry, pink_run_t *up_entry, keyset **found, int *level, NvmeRequest *req) {
    pink_run_t *entries=NULL;

    for(int i = *level; i < LSM_LEVELN; i++){
        /* 
         * If we have a upper level entry, then use a range pointer to reduce
         * a range to search.
         */
        if (up_entry)
            entries = find_run_se(pink_lsm, pink_lsm->disk[i], key, up_entry, ssd, req);
        else
            entries = find_run(pink_lsm->disk[i], key, ssd, req);

        /*
         * Compactioning level doesn't have a range pointer yet.
         */
        up_entry = NULL;
        if(!entries) {
            continue;
        }

        if (!entries->buffer && entries->ppa.ppa == UNMAPPED_PPA) {
            entries = find_run2(pink_lsm->c_level, key, ssd, req);
            if (level) {
                *level = i = pink_lsm->c_level->idx;
            }
            if (!entries) {
                continue;
            }
            *entry = entries;
            return COMP_FOUND;
        }

        /*
         * Run is cached.
         */
        if (entries->buffer) {
            keyset *find = find_keyset(entries->buffer, key);
            if (find) {
                *found = find;
                if (level) *level = i;
                // TODO: someone need to free(find)
                return CACHING;
            }
            /*
             * Met range overlapped run_t but no key in there.
             */
            up_entry = entries;
        } else {
            /*
             * Run is founded in not pinned levels.
             */
            if (level) *level = i;
            *entry = entries;
            return FOUND;
        }

        continue;
    }
    return NOTFOUND;
}
