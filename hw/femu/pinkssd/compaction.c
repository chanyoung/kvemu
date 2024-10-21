#include "hw/femu/kvssd/pink/pink_ftl.h"

uint32_t level_change(struct pink_lsmtree *LSM, pink_level *from, pink_level *to, pink_level *target) {
    pink_level **src_ptr=NULL, **des_ptr=NULL;
    des_ptr=&LSM->disk[to->idx];

    if(from!=NULL){
        src_ptr=&LSM->disk[from->idx];
        *(src_ptr)=level_init(from->idx);
        free_level(LSM, from);
    }

    make_partition(LSM,target);

    (*des_ptr)=target;
    free_level(LSM, to);

    return 1;
}

uint32_t leveling(struct ssd *ssd, pink_level *from, pink_level *to, leveling_node *l_node){
    pink_level *target_origin = to;
    pink_level *target = level_init(to->idx);
    pink_run_t *entry = NULL;

    // TODO: LEVEL_COMP_READ_DELAY
    read_run_delay_comp(ssd, to);
    /*
     * If destination level is empty. (0 runs)
     */
    if (to->n_num == 0) {
        qemu_mutex_lock(&ssd->comp_mu);
        pink_lsm->c_level = target;
        compaction_empty_level(ssd, &from, l_node, &target);
        goto last;
    }
    partial_leveling(ssd, target,target_origin,l_node,from);

last:
    if (entry) FREE(entry);
    uint32_t res = level_change(pink_lsm, from, to, target);
    pink_lsm->c_level = NULL;
    if (from == NULL) {
        kv_assert(l_node->mem == pink_lsm->temptable[0]);
        qemu_mutex_lock(&ssd->memtable_mu);
        for (int z = 0; z < pink_lsm->temp_n; z++) {
            kv_skiplist_free(pink_lsm->temptable[z]);
            pink_lsm->temptable[z] = NULL;
        }
        pink_lsm->temp_n = 0;
        qemu_mutex_unlock(&ssd->memtable_mu);
    }
    qemu_mutex_unlock(&ssd->comp_mu);

    if(target->idx == LSM_LEVELN-1){
        kv_debug("last level %d/%d (n:f)\n",target->n_num,target->m_num);
    }
    return res;
}

uint32_t partial_leveling(struct ssd *ssd, pink_level* t, pink_level *origin, leveling_node *lnode, pink_level* upper){
    kv_key start=kv_key_min;
    kv_key end=kv_key_max;
    pink_run_t **target_s=NULL;
    pink_run_t **data=NULL;
    kv_skiplist *skip=lnode?lnode->mem:kv_skiplist_init();

    if(!upper){
        range_find_compaction(origin,start,end,&target_s);

        int i = 0;
        for(int j=0; target_s[j]!=NULL; j++){
            if (compaction_meta_segment_read_femu(ssd, target_s[j])) {
                if (i % ASYNC_IO_UNIT == 0) {
                    wait_pending_reads(ssd);
                }
                i++;
            }
        }

        compaction_subprocessing(ssd, skip,NULL,target_s,t);
    } else {
        int src_num, des_num; //for stream compaction
        des_num=range_find_compaction(origin,start,end,&target_s);//for stream compaction
        src_num=range_find_compaction(upper,start,end,&data);
        if(src_num && des_num == 0 ){
            kv_debug("can't be\n");
            abort();
        }

        int j = 0;
        for(int i=0; target_s[i]!=NULL; i++){
            pink_run_t *temp=target_s[i];
            if (compaction_meta_segment_read_femu(ssd, temp)) {
                if (j % ASYNC_IO_UNIT == 0) {
                    wait_pending_reads(ssd);
                }
                j++;
            }
        }

        for(int i=0; data[i]!=NULL; i++){
            pink_run_t *temp=data[i];
            if (compaction_meta_segment_read_femu(ssd, temp)) {
                if (j % ASYNC_IO_UNIT == 0) {
                    wait_pending_reads(ssd);
                }
                j++;
            }
        }
        //wait_delay(ssd, true);
        compaction_subprocessing(ssd, NULL,data,target_s,t);
    }
    if(!lnode) kv_skiplist_free(skip);
    return 1;
}

