#include "hw/femu/kvssd/lksv/lksv3_ftl.h"

uint32_t lksv3_level_change(struct ssd *ssd, lksv3_level *from, lksv3_level *to, lksv3_level *target) {
    lksv3_level **src_ptr=NULL, **des_ptr=NULL;
    des_ptr=&lksv_lsm->disk[to->idx];

    if(from!=NULL){ 
        src_ptr=&lksv_lsm->disk[from->idx];

        int m_num = from->m_num;

        *(src_ptr)=lksv3_level_init(m_num,from->idx);
        lksv3_free_level(lksv_lsm, from);
    }

    lksv3_make_partition(lksv_lsm,target);

    (*des_ptr)=target;
    lksv3_free_level(lksv_lsm, to);

    if (lksv_lsm->bottom_level < target->idx)
        lksv_lsm->bottom_level = target->idx;

    // Double check level list entries caching
    if (target->idx == lksv_lsm->bottom_level && lksv_lsm->lsm_cache->levels[cache_level(LEVEL_LIST_ENTRY, target->idx)].n != target->n_num) {
        array_body *b = (array_body *) target->level_data;
        for (int i = 0; i < target->n_num; i++) {
            if (b->arrs[i].cache[LEVEL_LIST_ENTRY]) {
                continue;
            }
            uint32_t entry_size = b->arrs[i].key.len + (LEVELLIST_HASH_BYTES * PG_N) + 20;
            kv_cache_insert(lksv_lsm->lsm_cache, &b->arrs[i].cache[LEVEL_LIST_ENTRY], entry_size, cache_level(LEVEL_LIST_ENTRY, target->idx), KV_CACHE_WITHOUT_FLAGS);
            if (!b->arrs[i].cache[LEVEL_LIST_ENTRY]) {
                break;
            }
        }
    }
    // We don't need to query membership to the last level's entry.
    if (target->idx < lksv_lsm->bottom_level) {
        array_body *b = (array_body *) target->level_data;
        for (int i = 0; i < target->n_num - 1; i++) {
            uint32_t entry_size = (b->arrs[i].n * HASH_BYTES) + 20;
            kv_cache_insert(lksv_lsm->lsm_cache, &b->arrs[i].cache[HASH_LIST], entry_size, cache_level(HASH_LIST, target->idx), KV_CACHE_WITHOUT_FLAGS);
            if (!b->arrs[i].cache[HASH_LIST]) {
                break;
            }
        }
    }

    target->v_num = target->vsize / PAGESIZE / PG_N;
    if (target->idx > 0) {
        kv_debug("[Level: %d] n_num: %d, v_num: %d, m_num: %d\n", target->idx + 1, target->n_num, target->v_num, target->m_num);
    }

    return 1;
}

uint32_t lksv3_leveling(struct ssd *ssd, lksv3_level *from, lksv3_level *to, leveling_node *l_node){
    int m_num = to->m_num;
    lksv3_level *target = lksv3_level_init(m_num, to->idx);
    lksv3_run_t *entry = NULL;

    /*
     * If destination level is empty. (0 runs)
     */
    if (to->n_num == 0) {
        qemu_mutex_lock(&ssd->comp_mu);
        lksv_lsm->c_level = target;
        check_473(ssd);
        lksv3_compaction_empty_level(ssd, &from, l_node, &target);
        if (from == NULL) {
            if (l_node->mem->header->list[1]->value->length == PPA_LENGTH) {
                for (int i = 0; i < 512; i++) {
                    if (is_meta_line(ssd, i)) {
                        continue;
                    }
                    kv_assert(!to->reference_lines[i]);
                    if (lksv_lsm->flush_reference_lines[i]) {
                        kv_assert(per_line_data(&ssd->lm.lines[i])->referenced_flush); 
                        per_line_data(&ssd->lm.lines[i])->referenced_flush = false;
                        kv_assert(target->idx == 0);
                        per_line_data(&ssd->lm.lines[i])->referenced_levels[0] = true;
                    } else {
                        kv_assert(!per_line_data(&ssd->lm.lines[i])->referenced_flush);
                    }
                }
                memcpy(target->reference_lines, lksv_lsm->flush_reference_lines, 512 * sizeof(bool));
                memset(lksv_lsm->flush_reference_lines, 0, 512 * sizeof(bool));
            }
        }
        //check_473(ssd);
        goto last;
    }
    if (from) {
        // TODO: LEVEL_COMP_READ_DELAY
        do_lksv3_compaction2(ssd, from->idx, to->idx, NULL, target);
    } else {
        // TODO: LEVEL_COMP_READ_DELAY
        lksv3_read_run_delay_comp(ssd, to);
        do_lksv3_compaction2(ssd, -1, to->idx, l_node, target);
    }

last:
    if (entry) FREE(entry);
    uint32_t res = lksv3_level_change(ssd, from, to, target);
    check_473(ssd);
    lksv_lsm->c_level = NULL;
    qemu_mutex_unlock(&ssd->comp_mu);
    if (from == NULL) {
        kv_assert(l_node->mem == lksv_lsm->temptable);
        kv_skiplist *tmp = lksv_lsm->temptable;
        qemu_mutex_lock(&ssd->memtable_mu);
        lksv_lsm->temptable = NULL;
        qemu_mutex_unlock(&ssd->memtable_mu);
        kv_skiplist_free(tmp);
    }

    if(target->idx == LSM_LEVELN-1){
        kv_debug("last level %d/%d (n:f)\n",target->n_num,target->m_num);
    }
    return res;
}

