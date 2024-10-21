#include <pthread.h>
#include "hw/femu/kvssd/pink/pink_ftl.h"
#include "hw/femu/kvssd/pink/skiplist.h"

static void compaction_selector(struct ssd *ssd, pink_level *a, pink_level *b, leveling_node *lnode){
    if (a)
        kv_set_compaction_info(&pink_lsm->comp_ctx, a->idx, b->idx);
    else
        kv_set_compaction_info(&pink_lsm->comp_ctx, -1, b->idx);

    leveling(ssd, a, b, lnode);

    if (b->idx == LSM_LEVELN - 1)
        pink_lsm_adjust_level_multiplier();
    if (b->idx > 0) // We don't want too many calling adjust_lines().
        pink_adjust_lines(ssd);

    kv_reset_compaction_info(&pink_lsm->comp_ctx);
}

bool compaction_init(struct ssd *ssd) {
    QTAILQ_INIT(&pink_lsm->compaction_queue);
    return true;
}

static void compaction_assign(struct pink_lsmtree *LSM, compR* req){
    QTAILQ_INSERT_TAIL(&LSM->compaction_queue, req, entry);
}

void compaction_free(struct pink_lsmtree *LSM){
    while (!QTAILQ_EMPTY(&LSM->compaction_queue)) {
        compR *req = QTAILQ_FIRST(&LSM->compaction_queue);
        QTAILQ_REMOVE(&LSM->compaction_queue, req, entry);
    }
}

static void compaction_cascading(struct ssd *ssd) {
    int start_level = 0, des_level;
    while (should_compact(pink_lsm->disk[start_level])) {
        if (start_level < LSM_LEVELN - 3)
            des_level = start_level + 1;
        else
            break;
        compaction_selector(ssd, pink_lsm->disk[start_level], pink_lsm->disk[des_level], NULL);
        start_level++;
    }

    /*
       L0 - don't care.
       L1 - if L2 should be compacted, then compact after L2 compaction.
       L2 - compact only if L1 should be compacted.
       L3 - (last level).
     */
    if (should_compact(pink_lsm->disk[LSM_LEVELN - 3])) {
        if (should_compact(pink_lsm->disk[LSM_LEVELN - 2])) {
            compaction_selector(ssd, pink_lsm->disk[LSM_LEVELN - 2], pink_lsm->disk[LSM_LEVELN - 1], NULL);
        }
        compaction_selector(ssd, pink_lsm->disk[LSM_LEVELN - 3], pink_lsm->disk[LSM_LEVELN - 2], NULL);
    }
}

void pink_do_compaction(struct ssd *ssd)
{
    qemu_mutex_lock(&ssd->comp_q_mu);
    if (!QTAILQ_EMPTY(&pink_lsm->compaction_queue)) {
        compR *req = QTAILQ_FIRST(&pink_lsm->compaction_queue);
        QTAILQ_REMOVE(&pink_lsm->compaction_queue, req, entry);
        leveling_node lnode;

        qemu_mutex_unlock(&ssd->comp_q_mu);

        /*
         * from L0.
         * temptable is a cut skiplist.
         */
        if (req->fromL == -1) {
            lnode.mem = req->temptable;
            compaction_data_write(ssd, &lnode);
            compaction_selector(ssd, NULL, pink_lsm->disk[0], &lnode);
        }
        compaction_cascading(ssd);

        FREE(lnode.start.key);
        FREE(lnode.end.key);
        FREE(req);

        wait_pending_reads(ssd);
        qemu_mutex_lock(&ssd->comp_mu);
        while (pink_should_data_gc_high(ssd)) {
            int n = ssd->lm.data.lines / 10;
            if (n < 10)
                n = 10;
            for (int i = 0; i < n; i ++) {
                if (gc_data_femu(ssd) == 0) {
                    ssd->need_flush = true;
                }
            }
        }
        while (pink_should_meta_gc_high(ssd)) {
            int n = ssd->lm.meta.lines / 10;
            if (n < 2)
                n = 2;
            for (int i = 0; i < n; i ++) {
                gc_meta_femu(ssd);
            }
        }
        qemu_mutex_unlock(&ssd->comp_mu);

        if (rand() % 1000 == 0) {
            kv_debug("write_cnt %lu\n", pink_lsm->num_data_written);
            kv_debug("[META] free line cnt: %d\n", ssd->lm.meta.free_line_cnt);
            kv_debug("[META] full line cnt: %d\n", ssd->lm.meta.full_line_cnt);
            kv_debug("[META] victim line cnt: %d\n", ssd->lm.meta.victim_line_cnt);
            kv_debug("[DATA] free line cnt: %d\n", ssd->lm.data.free_line_cnt);
            kv_debug("[DATA] full line cnt: %d\n", ssd->lm.data.full_line_cnt);
            kv_debug("[DATA] victim line cnt: %d\n", ssd->lm.data.victim_line_cnt);
            print_level_summary(pink_lsm);
        }
    } else {
        qemu_mutex_unlock(&ssd->comp_q_mu);
    }
}

void compaction_check(struct ssd *ssd) {
    //qemu_mutex_lock(&ssd->memtable_mu);
    if (pink_lsm->memtable->n < FLUSHNUM || pink_lsm->temptable[0]) {
        qemu_mutex_unlock(&ssd->memtable_mu);
        return;
    }

    compR *req = (compR*)malloc(sizeof(compR));
    kv_skiplist *t1=NULL;

    t1 = pink_skiplist_cutting_header(pink_lsm->memtable, true);
    if (t1 == pink_lsm->memtable) {
        kv_debug("skiplist should be larger\n");
        abort();
    }
    /*
     * Constructs a compaction request in units of one meta-segment.
     */
    req->fromL = -1;    /* from memtable (L0) */
    req->temptable = t1;
    kv_assert(pink_lsm->temptable[0] == NULL);
    pink_lsm->temptable[0] = t1;
    kv_assert(pink_lsm->temp_n == 0);
    pink_lsm->temp_n = 1;
    qemu_mutex_unlock(&ssd->memtable_mu);

    qemu_mutex_lock(&ssd->comp_q_mu);
    compaction_assign(pink_lsm, req);
    qemu_mutex_unlock(&ssd->comp_q_mu);
}

void compaction_subprocessing(struct ssd *ssd, struct kv_skiplist *top, struct pink_run** src, struct pink_run** org, struct pink_level *des){
    merger(ssd, top,src,org,des);

    kv_key key,end;
    pink_run_t* target=NULL;

    int run_idx = 0;
    array_body *b = (array_body*)des->level_data;
    while((target=cutter(pink_lsm,top,des,&key,&end))){
        insert_run(ssd,des,target);
        run_idx++;
        free_run(pink_lsm, target);
        FREE(target);
    }

    qemu_mutex_lock(&ssd->comp_mu);
    kv_assert(pink_lsm->c_level == NULL);
    pink_lsm->c_level = des;
    qemu_mutex_unlock(&ssd->comp_mu);

    // Critical section - level data will be changed.
    int j = 0;
    qemu_mutex_lock(&ssd->comp_mu);
    if (src) {
        for(int i=0; src[i]!=NULL; i++){
            pink_run_t *temp=src[i];
            meta_segment_read_postproc(ssd, temp);

            if (j % ASYNC_IO_UNIT == 0) {
                qemu_mutex_unlock(&ssd->comp_mu);
                wait_pending_reads(ssd);
                qemu_mutex_lock(&ssd->comp_mu);
            }
            j++;
        }
        FREE(src);
    }
    for(int i=0; org[i]!=NULL; i++){
        pink_run_t *temp=org[i];
        meta_segment_read_postproc(ssd, temp);

        if (j % ASYNC_IO_UNIT == 0) {
            qemu_mutex_unlock(&ssd->comp_mu);
            wait_pending_reads(ssd);
            qemu_mutex_lock(&ssd->comp_mu);
        }
        j++;
    }
    FREE(org);

    bool cache_full = false;
    for (int i = 0; i < des->n_num; i++) {
        pink_run_t *temp = &b->arrs[i];

        if (!cache_full) {
            kv_cache_insert(pink_lsm->lsm_cache, &temp->cache[META_SEGMENT], PAGESIZE, cache_level(META_SEGMENT, des->idx), KV_CACHE_FLUSH_EVICTED);
            if (!temp->cache[META_SEGMENT]) {
                cache_full = true;
            } else {
                continue;
            }
        }

        if (i % ASYNC_IO_UNIT == 0) {
            qemu_mutex_unlock(&ssd->comp_mu);
            wait_pending_reads(ssd);
            qemu_mutex_lock(&ssd->comp_mu);
        }

        if (pink_should_meta_gc_high(ssd)) {
            gc_meta_femu(ssd);
        }

        kv_assert(temp->ppa.ppa == UNMAPPED_PPA);
        temp->ppa = compaction_meta_segment_write_femu(ssd, (char *) temp->buffer);
        temp->buffer = NULL;
    }

    //wait_delay(ssd, true);
}

bool meta_segment_read_preproc(pink_run_t *r){
    if (r->buffer) {
        return true;
    }
    return false;
}

void meta_segment_read_postproc(struct ssd *ssd, pink_run_t *r){
    if (r->ppa.ppa != UNMAPPED_PPA) {
        // data will be freed when marking page invalid.
        kv_assert(get_pg(ssd, &r->ppa)->data == r->buffer);
        mark_page_invalid(ssd, &r->ppa);
        r->buffer = NULL;
        r->ppa.ppa = UNMAPPED_PPA;
    }
}

uint32_t compaction_empty_level(struct ssd *ssd, pink_level **from, leveling_node *lnode, pink_level **des){
    if (!(*from)) {
        bool cache_full = false;
        while (true) {
            kv_skiplist *mem;
            mem = pink_skiplist_cutting_header(lnode->mem, false);

            /*
             * From memtable; L0->L1 (flush)
             */
            struct femu_ppa unmapped_ppa;
            unmapped_ppa.ppa = UNMAPPED_PPA;
            kv_key start;
            kv_key end;
            kv_skiplist_get_start_end_key(mem, &start, &end);
            pink_run_t *entry = make_run(start, end, unmapped_ppa);
            FREE(entry->key.key);
            FREE(entry->end.key);
            mem_cvt2table(ssd, mem, entry);

            if (!cache_full) {
                kv_cache_insert(pink_lsm->lsm_cache, &entry->cache[META_SEGMENT], PAGESIZE, cache_level(META_SEGMENT, (*des)->idx), KV_CACHE_FLUSH_EVICTED);
                if (!entry->cache[META_SEGMENT]) {
                    cache_full = true;
                    goto cache_full;
                }
            } else {
cache_full:
                entry->ppa = compaction_meta_segment_write_femu(ssd, (char *) entry->buffer);
                entry->buffer = NULL;
            }
            insert_run(ssd,(*des), entry);
            free_run(pink_lsm, entry);
            FREE(entry);

            if (mem == lnode->mem) {
                break;
            } else {
                qemu_mutex_lock(&ssd->memtable_mu);
                pink_lsm->temptable[pink_lsm->temp_n] = mem;
                pink_lsm->temp_n++;
                qemu_mutex_unlock(&ssd->memtable_mu);
                //kv_skiplist_free(mem);
            }
        }
    } else {
        copy_level(ssd,*des,*from);
    }
    return 1;
}
