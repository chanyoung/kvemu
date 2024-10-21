#include "hw/femu/kvssd/lksv/lksv3_ftl.h"
#include "hw/femu/kvssd/lksv/skiplist.h"

void lksv3_array_range_update(lksv3_level *lev, lksv3_run_t* r, kv_key key)
{
    if(kv_cmp_key(lev->start, key) > 0)
        lev->start = key;
    if(kv_cmp_key(lev->end,key) < 0)
        lev->end = key;
}

lksv3_level* lksv3_level_init(int size, int idx)
{
    int x_num = size * 200;

    lksv3_level *res = (lksv3_level*) calloc(1, sizeof(lksv3_level));
    array_body *b = (array_body*) calloc(1, sizeof(array_body));
    b->pr_arrs = (pr_node*) calloc(x_num, sizeof(pr_node));
    b->arrs = (lksv3_run_t*) calloc(x_num, sizeof(lksv3_run_t));

    res->idx=idx;
    res->m_num=size;
    res->x_num=x_num;  // temporary buffer for exceed m_num;
    res->n_num=0;
    res->start=kv_key_max;
    res->end=kv_key_min;
    res->level_data=(void*)b;

    return res;
}

void lksv3_free_run(struct lksv3_lsmtree *LSM, lksv3_run_t *e) {
    kv_cache_delete_entry(LSM->lsm_cache, e->cache[LEVEL_LIST_ENTRY]);
    kv_cache_delete_entry(LSM->lsm_cache, e->cache[HASH_LIST]);
    kv_cache_delete_entry(LSM->lsm_cache, e->cache[DATA_SEGMENT_GROUP]);
    if (e->key.key) {
        FREE(e->key.key);
    }
    if (e->end.key) {
        FREE(e->end.key);
    }
    for (int i = 0; i < PG_N; i++) {
        if (e->buffer[i]) {
            e->buffer[i] = NULL;
        }
        if (e->hash_lists[i].hashes) {
            FREE(e->hash_lists[i].hashes);
        }
    }
}

static void array_body_free(struct lksv3_lsmtree *LSM, lksv3_run_t *runs, int size){
    for(int i=0; i<size; i++){
        lksv3_free_run(LSM, &runs[i]);
    }
    FREE(runs);
}

void lksv3_free_level(struct lksv3_lsmtree *LSM, lksv3_level* lev) {
    array_body *b = (array_body*)lev->level_data;
    FREE(b->p_nodes);
    FREE(b->pr_arrs);
    array_body_free(LSM, b->arrs, lev->n_num);
    FREE(b);
    FREE(lev);
}

static void array_run_cpy_to(struct ssd *ssd, lksv3_run_t *input, lksv3_run_t *res, int idx){
    kv_copy_key(&res->key,&input->key);
    kv_copy_key(&res->end,&input->end);

    res->ppa=input->ppa;

    pthread_spin_lock(&lksv_lsm->lsm_cache->lock);
    if (input->cache[HASH_LIST]) {
        res->cache[HASH_LIST]=input->cache[HASH_LIST];
        res->cache[HASH_LIST]->entry = &res->cache[HASH_LIST];
        input->cache[HASH_LIST]=NULL;
    } else {
        res->cache[HASH_LIST]=NULL;
    }
    if (input->cache[LEVEL_LIST_ENTRY]) {
        res->cache[LEVEL_LIST_ENTRY]=input->cache[LEVEL_LIST_ENTRY];
        res->cache[LEVEL_LIST_ENTRY]->entry=&res->cache[LEVEL_LIST_ENTRY];
        input->cache[LEVEL_LIST_ENTRY]=NULL;
    } else {
        res->cache[LEVEL_LIST_ENTRY]=NULL;
    }
    if (input->cache[DATA_SEGMENT_GROUP]) {
        res->cache[DATA_SEGMENT_GROUP]=input->cache[DATA_SEGMENT_GROUP];
        res->cache[DATA_SEGMENT_GROUP]->entry=&res->cache[DATA_SEGMENT_GROUP];
        input->cache[DATA_SEGMENT_GROUP]=NULL;
    } else {
        res->cache[DATA_SEGMENT_GROUP]=NULL;
    }
    pthread_spin_unlock(&lksv_lsm->lsm_cache->lock);

    for (int i = 0; i < PG_N; i++) {
        res->hash_lists[i].hashes = calloc(input->hash_lists[i].n, sizeof(lksv_hash));
        memcpy(res->hash_lists[i].hashes, input->hash_lists[i].hashes, input->hash_lists[i].n * sizeof(lksv_hash));
        res->hash_lists[i].n = input->hash_lists[i].n;
        res->pg_start_hashes[i] = input->pg_start_hashes[i];
    }
    res->hash_list_n = input->hash_list_n;
    res->n = input->n;
}

void lksv3_copy_level(struct ssd *ssd, lksv3_level *des, lksv3_level *src){
    des->start=kv_key_max;
    des->end=kv_key_min;
    des->n_num=src->n_num;

    des->vsize=src->vsize;
    des->v_num=src->v_num;

    array_body *db=(array_body*)des->level_data;
    array_body *sb=(array_body*)src->level_data;
    memcpy(db->pr_arrs,sb->pr_arrs,sizeof(pr_node)*src->n_num);
    for(int i=0; i<src->n_num; i++){
        array_run_cpy_to(ssd, &sb->arrs[i],&db->arrs[i],src->idx);
        lksv3_array_range_update(des, NULL, db->arrs[i].key);
        lksv3_array_range_update(des, NULL, db->arrs[i].end);
    }
}

void lksv3_read_run_delay_comp(struct ssd *ssd, lksv3_level *lev) {
    int p = 0;
    int end = lev->n_num / RUNINPAGE + 1;
    array_body *b = (array_body*)lev->level_data;
    int last_read_run_idx = INT32_MAX;
    while (p < end) {
        // TODO: LEVEL_READ_DELAY
        if (kv_is_cached(lksv_lsm->lsm_cache, b->arrs[p].cache[LEVEL_LIST_ENTRY])) {
            kv_cache_delete_entry(lksv_lsm->lsm_cache, b->arrs[p].cache[LEVEL_LIST_ENTRY]);
        } else if (last_read_run_idx != p / LEVEL_LIST_ENTRY_PER_PAGE) {
            last_read_run_idx = p / LEVEL_LIST_ENTRY_PER_PAGE;

            struct nand_cmd srd;
            srd.type = COMP_IO;
            srd.cmd = NAND_READ;
            srd.stime = 0;

            struct femu_ppa fake_ppa;
            fake_ppa.ppa = 0;
            fake_ppa.g.blk = last_read_run_idx % ssd->sp.blks_per_pl;

            lksv3_ssd_advance_status(ssd, &fake_ppa, &srd); 
        }
        p++;
    }
}

lksv3_run_t* lksv3_insert_run2(struct ssd *ssd, lksv3_level *lev, lksv3_run_t* r) {
    if(lev->m_num <= lev->n_num) {
        kv_assert(lev->x_num >= lev->n_num);
        kv_debug("WARNING: n_num(%d) exceeds m_num(%d) at level %d\n", lev->n_num, lev->m_num, lev->idx);
    }
    kv_assert(!kv_is_cached(lksv_lsm->lsm_cache, r->cache[LEVEL_LIST_ENTRY]));
    // TODO: We need to implement writing the level list entry to Flash.
    // Currently, we are only modeling the write latency, and we are not
    // modeling the case where those pages undergo garbage collecting.
    kv_cache_insert(lksv_lsm->lsm_cache, &r->cache[LEVEL_LIST_ENTRY], r->key.len + (LEVELLIST_HASH_BYTES * PG_N) + 20, cache_level(LEVEL_LIST_ENTRY, lev->idx), KV_CACHE_WITHOUT_FLAGS);

    array_body *b = (array_body*)lev->level_data;
    lksv3_run_t *arrs = b->arrs;
    lksv3_run_t *target = &arrs[lev->n_num];
    array_run_cpy_to(ssd, r, target, lev->idx);
    for (int i = 0; i < PG_N; i++) {
        target->buffer[i] = r->buffer[i];
        r->buffer[i] = NULL;
    }

    memcpy(b->pr_arrs[lev->n_num].pr_key,r->key.key,PREFIXCHECK);

    lksv3_array_range_update(lev, NULL, target->key);
    lksv3_array_range_update(lev, NULL, target->end);

    lev->n_num++;
    return target;
}

lksv3_run_t* lksv3_insert_run(struct ssd *ssd, lksv3_level *lev, lksv3_run_t* r) {
    if(lev->m_num <= lev->n_num) {
        kv_assert(lev->x_num >= lev->n_num);
        kv_debug("WARNING: n_num(%d) exceeds m_num(%d) at level %d\n", lev->n_num, lev->m_num, lev->idx);
    }
    kv_assert(!kv_is_cached(lksv_lsm->lsm_cache, r->cache[LEVEL_LIST_ENTRY]));
    // TODO: We need to implement writing the level list entry to Flash.
    // Currently, we are only modeling the write latency, and we are not
    // modeling the case where those pages undergo garbage collecting.
    kv_cache_insert(lksv_lsm->lsm_cache, &r->cache[LEVEL_LIST_ENTRY], r->key.len + (LEVELLIST_HASH_BYTES * PG_N) + 20, cache_level(LEVEL_LIST_ENTRY, lev->idx), KV_CACHE_WITHOUT_FLAGS);

    array_body *b = (array_body*)lev->level_data;
    lksv3_run_t *arrs = b->arrs;
    lksv3_run_t *target = &arrs[lev->n_num];
    array_run_cpy_to(ssd, r, target, lev->idx);

    memcpy(b->pr_arrs[lev->n_num].pr_key,r->key.key,PREFIXCHECK);

    lksv3_array_range_update(lev, NULL, target->key);
    lksv3_array_range_update(lev, NULL, target->end);

    lev->n_num++;
    return target;
}

static int lksv3_find_sht_idx(lksv3_run_t *run, uint16_t hash)
{
    int end = run->hash_list_n-1;
    for (int i = 0; i < end; i++)
        if (run->pg_start_hashes[i+1] >= hash)
            return i;
    return run->hash_list_n-1;
}

static int lksv3_key_maybe_exist(lksv_hash_list *sht, uint32_t hash, int nbit) {
    int start = 0;
    int end = sht->n - 1;
    int mid=0;
    int shift = 32 - nbit;
    uint32_t h = hash >> shift;

    while(start<end) {
        mid=(start+end)/2;

        if ((sht->hashes[mid].hash >> shift) > h) {
            end = mid - 1;
        } else if ((sht->hashes[mid].hash >> shift) < h) {
            start = mid + 1;
        } else {
            goto find_same_hash;
        }
    }
    mid = (start+end)/2;
    if ((sht->hashes[mid].hash >> shift) == h) {
        goto find_same_hash;
    }

    return -1;

find_same_hash:
    while (mid > 0) {
        if ((sht->hashes[mid - 1].hash >> shift) != (sht->hashes[mid].hash >> shift)) {
            break;
        }
        mid--;
    }
    return mid;
}

keyset* lksv3_find_keyset(struct ssd *ssd, NvmeRequest *req, lksv3_run_t *run, kv_key lpa, uint32_t hash, int level) {
    bool hash_list_cached = kv_is_cached(lksv_lsm->lsm_cache, run->cache[HASH_LIST]);
    int page_idx = lksv3_find_sht_idx(run, hash >> LEVELLIST_HASH_SHIFTS);
    int in_page_idx;

find_keyset:
    in_page_idx = lksv3_key_maybe_exist(&run->hash_lists[page_idx], hash, 32);
    if (hash_list_cached) {
        if (in_page_idx == -1)
            return NULL;
    }

    // Read a data segement.
    char *buffer;
    if (run->ppa.ppa != UNMAPPED_PPA) {
        struct femu_ppa ppa = get_next_write_ppa(ssd, run->ppa, page_idx);
        if (!kv_is_cached(lksv_lsm->lsm_cache, run->cache[DATA_SEGMENT_GROUP])) {
            struct nand_cmd srd;
            srd.type = USER_IO;
            srd.cmd = NAND_READ;
            if (req) {
                srd.stime = req->etime;
                req->flash_access_count++;
            } else {
                srd.stime = 0;
            }
            uint64_t sublat = lksv3_ssd_advance_status(ssd, &ppa, &srd);
            if (req) {
                req->etime += sublat;
            }
        }
        struct nand_page *pg = lksv3_get_pg(ssd, &ppa);
        kv_assert(pg->data);
        buffer = pg->data;
    } else if (run->buffer[page_idx]) {
        buffer = run->buffer[page_idx];
    } else {
        abort();
    }

    // Find the keyset.
    keyset *ks = NULL;
    lksv_block_footer *f = (lksv_block_footer *) (buffer + (PAGESIZE - LKSV3_SSTABLE_FOOTER_BLK_SIZE));
    while (in_page_idx < f->g.n) {
        lksv_block_meta *meta = (lksv_block_meta *) (buffer + (PAGESIZE - LKSV3_SSTABLE_FOOTER_BLK_SIZE - (LKSV3_SSTABLE_META_BLK_SIZE * (in_page_idx + 1))));
        if (meta->g1.hash > hash) {
            break;
        } else if (meta->g1.hash < hash) {
            in_page_idx++;
            continue;
        }

        kv_key target;
        target.key = buffer + meta->g1.off;
        target.len = meta->g1.klen;
        int res = kv_cmp_key(target, lpa);
        if (res == 0) {
            ks = (keyset *) calloc(1, sizeof(keyset));
            ks->lpa.key = target.key;
            ks->lpa.len = target.len;
            ks->value_len = meta->g2.slen;
            ks->voff = meta->g2.voff;
            ks->hash = meta->g1.hash;
            if (meta->g1.flag == VLOG) {
                ks->ppa = *((struct femu_ppa *) (target.key + target.len));
            } else {
                ks->ppa.ppa = UNMAPPED_PPA;
            }
            // Handling shards.
            if (meta->g2.snum > 1 && run->ppa.ppa != UNMAPPED_PPA) {
                struct femu_ppa ppa = get_next_write_ppa(ssd, run->ppa, page_idx+1);
                if (!kv_is_cached(lksv_lsm->lsm_cache, run->cache[DATA_SEGMENT_GROUP])) {
                    struct nand_cmd srd;
                    srd.type = USER_IO;
                    srd.cmd = NAND_READ;
                    if (req) {
                        srd.stime = req->etime;
                        req->flash_access_count++;
                    } else {
                        srd.stime = 0;
                    }
                    uint64_t sublat = lksv3_ssd_advance_status(ssd, &ppa, &srd);
                    if (req) {
                        req->etime += sublat;
                    }
                }
            }
            break;
        } else {
            in_page_idx++;
        }
    }

    if (ks == NULL) {
        qatomic_inc(&ssd->n->hash_collision_cnt);
        if (page_idx+1 < run->hash_list_n && (hash >> LEVELLIST_HASH_SHIFTS) == run->pg_start_hashes[page_idx+1]) {
            page_idx++;
            goto find_keyset;
        }
    }
    return ks;
}

static int array_bound_search(lksv3_run_t *body, uint32_t max_t, kv_key lpa, bool islower){
    int start=0;
    int end=max_t-1;
    int mid=0;

    int res1=0, res2=0; //1:compare with start, 2:compare with end
    while(start==end ||start<end){
        mid=(start+end)/2;
        res1=kv_cmp_key(body[mid].key,lpa);
        res2=kv_cmp_key(body[mid].end,lpa);
        if(res1<=0 && res2>=0){
            if(islower)return mid;
            else return mid+1;
        }
        if(res1>0) end=mid-1;
        else if(res2<0) start=mid+1;
    }

    if(res1>0) return mid;
    else if (res2<0 && mid < (int)max_t-1) return mid+1;
    else return -1;
}

uint32_t lksv3_range_find_compaction(lksv3_level *lev, kv_key s, kv_key e, lksv3_run_t ***rc){
    array_body *b=(array_body*)lev->level_data;
    lksv3_run_t *arrs=b->arrs;
    int res=0;
    lksv3_run_t *ptr;
    lksv3_run_t **r=(lksv3_run_t**)calloc(1, sizeof(lksv3_run_t*)*(lev->n_num+1));
    int target_idx=array_bound_search(arrs,lev->n_num,s,true);
    if(target_idx==-1) target_idx=0;
    for(int i=target_idx;i<lev->n_num; i++){
        ptr=(lksv3_run_t*)&arrs[i];
        r[res++]=ptr;
    }
    r[res]=NULL;
    *rc=r;
    return res;
}

lev_iter* lksv3_get_iter(lksv3_level *lev, kv_key start, kv_key end){
    array_body *b=(array_body*)lev->level_data;
    lev_iter *it=(lev_iter*)calloc(1, sizeof(lev_iter));
    it->from=start;
    it->to=end;
    a_iter *iter=(a_iter*)calloc(1, sizeof(a_iter));

    if(kv_cmp_key(start,lev->start)==0 && kv_cmp_key(end,lev->end)==0){
        iter->ispartial=false;
        iter->max=lev->n_num;
        iter->now=0;
    }   
    else{
        abort();
        //  kv_debug("should do somthing!\n");
        iter->now=array_bound_search(b->arrs,lev->n_num,start,true);
        iter->max=array_bound_search(b->arrs,lev->n_num,end,true);
        iter->ispartial=true;
    }
    iter->arrs=b->arrs;

    it->iter_data=(void*)iter;
    it->lev_idx=lev->idx;
    return it;
}

lksv3_run_t *lksv3_iter_nxt(lev_iter* in){
    a_iter *iter=(a_iter*)in->iter_data;
    if(iter->now==iter->max){
        FREE(iter);
        FREE(in);
        return NULL;
    }else{   
        if(iter->ispartial){
            return &iter->arrs[iter->now++];
        }else{
            return &iter->arrs[iter->now++];
        }
    }
    return NULL;
}

static int find_end_partition(lksv3_level *lev, int start_idx, kv_key lpa){
    array_body *b=(array_body*)lev->level_data;
    lksv3_run_t *arrs=b->arrs;
    int end=lev->n_num-1;
    int start=start_idx;

    int mid=(start+end)/2,res;
    while(1){
        res=kv_cmp_key(arrs[mid].key,lpa);
        if(res>0) end=mid-1;
        else if(res<0) start=mid+1;
        else {
            return mid;
        }
        mid=(start+end)/2;
        if(start>end){
            return mid;
        }
    }
    return lev->n_num-1;
}

static void partition_set(struct lksv3_lsmtree* LSM, pt_node *target, kv_key lpa_end, int end, int n_lev_idx){
    target->start=end;
    target->end=find_end_partition(LSM->disk[n_lev_idx], end, lpa_end);
}

void lksv3_make_partition(struct lksv3_lsmtree *LSM, lksv3_level *lev){
    if(lev->idx==LSM_LEVELN-1) return;
    array_body *b=(array_body*)lev->level_data;
    lksv3_run_t *arrs=b->arrs;
    b->p_nodes=(pt_node*)calloc(1, sizeof(pt_node)*lev->n_num);
    pt_node *p_nodes=b->p_nodes;

    for(int i=0; i<lev->n_num-1; i++){
        partition_set(LSM, &p_nodes[i],arrs[i+1].key,i==0?0:p_nodes[i-1].end,lev->idx+1);
    }
    if(lev->n_num==1){
        p_nodes[0].start=0;
        p_nodes[0].end=LSM->disk[lev->idx+1]->n_num-1;
    }
    else{
        p_nodes[lev->n_num-1].start=p_nodes[lev->n_num-2].end;
        p_nodes[lev->n_num-1].end=LSM->disk[lev->idx+1]->n_num-1;
    }
}

lksv3_run_t *lksv3_find_run_se(struct lksv3_lsmtree *LSM, lksv3_level *lev, kv_key lpa, lksv3_run_t *up_ent, struct ssd *ssd, NvmeRequest *req){
    array_body *b=(array_body*)lev->level_data;
    lksv3_run_t *arrs=b->arrs;
    pr_node *parrs=b->pr_arrs;

    array_body *bup=(array_body*)LSM->disk[lev->idx-1]->level_data;
    if(!arrs || lev->n_num==0 || !bup) return NULL;
    int up_idx=up_ent-bup->arrs;

    int start=bup->p_nodes[up_idx].start;
    int end=bup->p_nodes[up_idx].end;
    int mid=(start+end)/2, res;

    int last_read_run_idx = INT32_MAX;

    while(1){
        // TODO: LEVEL_READ_DELAY
        if (!arrs[mid].cache[LEVEL_LIST_ENTRY] &&
            last_read_run_idx != mid / LEVEL_LIST_ENTRY_PER_PAGE) {
            last_read_run_idx = mid / LEVEL_LIST_ENTRY_PER_PAGE;
            struct nand_cmd srd;
            srd.type = USER_IO;
            srd.cmd = NAND_READ;
            if (req) {
                srd.stime = req->etime;
            } else {
                srd.stime = 0;
            }
            struct femu_ppa fake_ppa;
            fake_ppa.ppa = 0;
            fake_ppa.g.blk = last_read_run_idx % ssd->sp.blks_per_pl;
            uint64_t sublat = lksv3_ssd_advance_status(ssd, &fake_ppa, &srd); 
            if (req) {
                req->etime += sublat;
                req->flash_access_count++;
            }
        }

        res=memcmp(parrs[mid].pr_key,lpa.key,PREFIXCHECK);
        if(res>0) end=mid-1;
        else if(res<0) start=mid+1;
        else{
            break;
        }
        mid=(start+end)/2;
        if(start>end) {
            return &arrs[mid];
        }
    }

    while(1){
        // TODO: LEVEL_READ_DELAY
        if (!arrs[mid].cache[LEVEL_LIST_ENTRY] &&
            last_read_run_idx != mid / LEVEL_LIST_ENTRY_PER_PAGE) {
            last_read_run_idx = mid / LEVEL_LIST_ENTRY_PER_PAGE;
            struct nand_cmd srd;
            srd.type = USER_IO;
            srd.cmd = NAND_READ;
            if (req) {
                srd.stime = req->etime;
            } else {
                srd.stime = 0;
            }
            struct femu_ppa fake_ppa;
            fake_ppa.ppa = 0;
            fake_ppa.g.blk = last_read_run_idx % ssd->sp.blks_per_pl;
            uint64_t sublat = lksv3_ssd_advance_status(ssd, &fake_ppa, &srd); 
            if (req) {
                req->etime += sublat;
                req->flash_access_count++;
            }
        }

        res=kv_cmp_key(arrs[mid].key,lpa);
        if(res>0) end=mid-1;
        else if(res<0) start=mid+1;
        else {
            return &arrs[mid];
        }
        mid=(start+end)/2;
        if(start>end){
            return &arrs[mid];
        }
    }
    return &arrs[mid];
}

lksv3_run_t *lksv3_find_run(lksv3_level* lev, kv_key lpa, struct ssd *ssd, NvmeRequest *req){
    array_body *b=(array_body*)lev->level_data;
    lksv3_run_t *arrs=b->arrs;
    pr_node *parrs=b->pr_arrs;
    if(!arrs || lev->n_num==0) return NULL;
    int end=lev->n_num-1;
    int start=0;
    int mid;

    int res1; //1:compare with start, 2:compare with end
    mid=(start+end)/2;

    int last_read_run_idx = INT32_MAX;
    while(1){
        // TODO: LEVEL_READ_DELAY
        if (!arrs[mid].cache[LEVEL_LIST_ENTRY] &&
            last_read_run_idx != mid / LEVEL_LIST_ENTRY_PER_PAGE) {
            last_read_run_idx = mid / LEVEL_LIST_ENTRY_PER_PAGE;
            struct nand_cmd srd;
            srd.type = USER_IO;
            srd.cmd = NAND_READ;
            if (req) {
                srd.stime = req->etime;
            } else {
                srd.stime = 0;
            }
            struct femu_ppa fake_ppa;
            fake_ppa.ppa = 0;
            fake_ppa.g.blk = last_read_run_idx % ssd->sp.blks_per_pl;
            uint64_t sublat = lksv3_ssd_advance_status(ssd, &fake_ppa, &srd); 
            if (req) {
                req->etime += sublat;
                req->flash_access_count++;
            }
        }

        res1=memcmp(parrs[mid].pr_key,lpa.key,PREFIXCHECK);
        if(res1>0) end=mid-1;
        else if(res1<0) start=mid+1;
        else{
            break;
        }
        mid=(start+end)/2;
        if(start>end) break;
    }

    while(1){
        // TODO: LEVEL_READ_DELAY
        if (!arrs[mid].cache[LEVEL_LIST_ENTRY] &&
            last_read_run_idx != mid / LEVEL_LIST_ENTRY_PER_PAGE) {
            last_read_run_idx = mid / LEVEL_LIST_ENTRY_PER_PAGE;
            struct nand_cmd srd;
            srd.type = USER_IO;
            srd.cmd = NAND_READ;
            if (req) {
                srd.stime = req->etime;
            } else {
                srd.stime = 0;
            }
            struct femu_ppa fake_ppa;
            fake_ppa.ppa = 0;
            fake_ppa.g.blk = last_read_run_idx % ssd->sp.blks_per_pl;
            uint64_t sublat = lksv3_ssd_advance_status(ssd, &fake_ppa, &srd); 
            if (req) {
                req->etime += sublat;
                req->flash_access_count++;
            }
        }

        res1=kv_cmp_key(arrs[mid].key,lpa);
        if(res1>0) end=mid-1;
        else if(res1<0) start=mid+1;
        else {
            return &arrs[mid];
        }
        mid=(start+end)/2;
        if(start>end){
            return &arrs[mid];
        }
    }
    return NULL;
}

lksv3_run_t *lksv3_find_run_slow(lksv3_level* lev, kv_key lpa, struct ssd *ssd){
    array_body *b=(array_body*)lev->level_data;
    lksv3_run_t *arrs=b->arrs;
    if(!arrs || lev->n_num==0) return NULL;
    int res1; //1:compare with start, 2:compare with end
    for (int i = 0; i < lev->n_num; i++) {
        res1=kv_cmp_key(arrs[i].key,lpa);
        if (res1 == 0) {
            return &arrs[i];
        }
    }
    return NULL;
}

lksv3_run_t *lksv3_find_run_slow_by_ppa(lksv3_level* lev, struct femu_ppa *ppa, struct ssd *ssd){
    array_body *b=(array_body*)lev->level_data;
    lksv3_run_t *arrs=b->arrs;
    if(!arrs || lev->n_num==0) return NULL;
    for (int i = 0; i < lev->n_num; i++) {
        if (arrs[i].ppa.ppa == ppa->ppa) {
            return &arrs[i];
        }
    }
    return NULL;
}

lksv3_run_t *lksv3_make_run(kv_key start, kv_key end, struct femu_ppa ppa){
    lksv3_run_t * res=(lksv3_run_t*) calloc(1, sizeof(lksv3_run_t));
    kv_copy_key(&res->key,&start);
    kv_copy_key(&res->end,&end);
    res->ppa = ppa;
    return res;
}

void lksv3_print_level_summary(struct lksv3_lsmtree *LSM) {
    for(int i=0; i<LSM_LEVELN; i++){
        if(LSM->disk[i]->n_num==0){
            kv_log("[%d] n_num:%d m_num:%d\n",i+1,LSM->disk[i]->n_num,LSM->disk[i]->m_num);
        }
        else {
            kv_log("[%d (%.*s ~ %.*s)] n_num:%d m_num:%d %.*s ~ %.*s\n",i+1,KEYFORMAT(LSM->disk[i]->start),KEYFORMAT(LSM->disk[i]->end),LSM->disk[i]->n_num,LSM->disk[i]->m_num,KEYFORMAT(LSM->disk[i]->start),KEYFORMAT(LSM->disk[i]->end));
        }
    }
}

static void _lksv3_mem_cvt2table(struct ssd *ssd, lksv_comp_entry **mem, int n, lksv3_sst_t *sst, bool sharded)
{
    int wp = 0;

    memset(sst, 0, sizeof(lksv3_sst_t));
    sst->raw = calloc(1, PAGESIZE);
    sst->meta = calloc(2048, sizeof(lksv_block_meta));

    lksv_comp_entry *t;
    for (int i = 0; i < n; i++) {
        t = mem[i];

        kv_assert(t->meta.g2.slen > 0);

        lksv3_kv_pair_t kv;
        kv.k = t->key;
        kv.v.len = t->meta.g2.slen;
        kv.voff = t->meta.g2.voff;
        if (t->meta.g1.flag == VLOG) {
            kv.ppa = t->ppa;
            kv_assert(kv.ppa.ppa != UNMAPPED_PPA);
        } else {
            kv.ppa.ppa = UNMAPPED_PPA;
            kv_assert(t->meta.g2.slen != PPA_LENGTH);
        }

        int ret = lksv3_sst_encode2(sst, &kv, t->meta.g1.hash, &wp, sharded && i == n-1);
        if (ret != LKSV3_TABLE_OK) {
            kv_err("panic");
        }
    }
}

char *lksv3_mem_cvt2table2(struct ssd *ssd, struct lksv_comp_list *list, lksv3_run_t *input)
{
    kv_copy_key(&input->key, &list->str_order_entries[0].key);
    kv_copy_key(&input->end, &list->str_order_entries[list->n - 1].key);

    int from, n, idx;
    idx = 0;
    lksv3_sst_t sst[PG_N];
    int shard_left_size = 0;
    for (int i = 0; i < list->n; i += n) {
        kv_assert(idx < PG_N);
        from = i;
        int page_size = LKSV3_SSTABLE_FOOTER_BLK_SIZE + shard_left_size;
        shard_left_size = 0;
        int k;
        for (k = i; k < list->n; k++) {
            int entry_size = LKSV3_SSTABLE_META_BLK_SIZE + LKSV3_SSTABLE_STR_IDX_SIZE;
            entry_size += list->hash_order_pointers[k]->key.len;
            if (page_size + entry_size + PPA_LENGTH > PAGESIZE) {
                break;
            }
            // shardable
            entry_size += list->hash_order_pointers[k]->meta.g2.slen;
            if (page_size + entry_size > PAGESIZE) {
                shard_left_size = page_size + entry_size - PAGESIZE +
                                  LKSV3_SSTABLE_META_BLK_SIZE;
                page_size = PAGESIZE;
                k++;
                break;
            }
            page_size += entry_size;
        }
        n = k - i;
        if (from + n > list->n) {
            n = list->n - from;
        }
        //qsort(ces + from, n, sizeof(struct lksv3_comp_entry), key_compare);

        _lksv3_mem_cvt2table(ssd, list->hash_order_pointers + from, n, &sst[idx], shard_left_size > 0);
        input->pg_start_hashes[idx] = list->hash_order_pointers[from]->meta.g1.hash >> LEVELLIST_HASH_SHIFTS;
        input->hash_lists[idx].hashes = calloc(n, sizeof(lksv_hash));
        input->hash_lists[idx].n = n;

        uint32_t str_order;
        for (int j = 0; j < n; j++) {
            input->hash_lists[idx].hashes[j].hash = list->hash_order_pointers[from + j]->meta.g1.hash;
            //str_order = list->hash_order_pointers[from + j]->str_order;
            str_order = ((int64_t) list->hash_order_pointers[from + j] - (int64_t) list->str_order_entries) / sizeof(lksv_comp_entry);
            //assert(str_order == list->hash_order_pointers[from + j]->str_order);

            //list->str_order_map[str_order].g1.sst = idx;
            //list->str_order_map[str_order].g1.off = j;
            //assert(((j << 8) | idx) == list->str_order_map[str_order].i);
            list->str_order_map[str_order].i = ((j << 8) | idx);
        }
        input->buffer[idx] = sst[idx].raw;

        idx++;
    }

    int nidx = idx;
    from = 0;
    for (idx = 0; idx < nidx; idx++) {
        lksv3_sst_encode_str_idx(&sst[idx], &list->str_order_map[from], sst[idx].footer.g.n);
        from += sst[idx].footer.g.n;
        FREE(sst[idx].meta);
    }
    input->hash_list_n = idx;
    input->n = list->n;

    return NULL;
}

char *lksv3_mem_cvt2table(struct ssd *ssd, kv_skiplist *mem, lksv3_run_t *input)
{
#ifdef LK_OH
    static uint64_t alloc = 0;
    static uint64_t loop = 0;
    static uint64_t qst = 0;
    static uint64_t get_sekey = 0;
    static uint64_t make = 0;
    static uint64_t stat_count = 0;
#endif

    kv_assert(mem->header->list[1]->value->length == PPA_LENGTH);

#ifdef LK_OH
    uint64_t now = qemu_clock_get_ns(QEMU_CLOCK_REALTIME);
#endif

    struct lksv_comp_list list;
    list.str_order_entries = calloc(mem->n, sizeof(lksv_comp_entry));
    list.hash_order_pointers = calloc(mem->n, sizeof(lksv_comp_entry *));
    list.str_order_map = calloc(mem->n, sizeof(lksv3_sst_str_idx_t));

#ifdef LK_OH
    uint64_t end = qemu_clock_get_ns(QEMU_CLOCK_REALTIME);
    alloc += end - now;
    now = end;
#endif

    int i = 0;
    list.n = mem->n;
    kv_snode *temp;
    for_each_sk(temp, mem) {
        kv_assert(snode_ppa(temp)->ppa != UNMAPPED_PPA);
        list.str_order_entries[i].meta.g1.flag = VLOG;
        list.str_order_entries[i].meta.g1.hash = *snode_hash(temp);
        list.str_order_entries[i].ppa = *snode_ppa(temp);
        list.str_order_entries[i].key = temp->key;
        list.str_order_entries[i].meta.g2.slen = PPA_LENGTH;
        list.str_order_entries[i].meta.g2.voff = *snode_off(temp);
        //list.str_order_entries[i].str_order = i;
        list.hash_order_pointers[i] = &list.str_order_entries[i];
        i++;
    }

#ifdef LK_OH
    end = qemu_clock_get_ns(QEMU_CLOCK_REALTIME);
    loop += end - now;
    now = end;
#endif

    bucket_sort(&list);

#ifdef LK_OH
    end = qemu_clock_get_ns(QEMU_CLOCK_REALTIME);
    qst += end - now;
    now = end;
#endif

    kv_skiplist_get_start_end_key(mem, &input->key, &input->end);

#ifdef LK_OH
    end = qemu_clock_get_ns(QEMU_CLOCK_REALTIME);
    get_sekey += end - now;
    now = end;
#endif

    int from, n, idx;
    idx = 0;
    lksv3_sst_t sst[PG_N];
    for (i = 0; i < mem->n; i += n) {
        from = i;
        int left_size = PAGESIZE - LKSV3_SSTABLE_FOOTER_BLK_SIZE;
        int max_keys_in_a_sht = 0;
        for (int j = from; j < mem->n; j++) {
            left_size -= list.hash_order_pointers[j]->key.len + PPA_LENGTH + LKSV3_SSTABLE_META_BLK_SIZE + LKSV3_SSTABLE_STR_IDX_SIZE;
            if (left_size >= 0)
                max_keys_in_a_sht++;
            else
                break;
        }
        n = max_keys_in_a_sht;
        if (from + n > mem->n) {
            n = mem->n - from;
        }

        _lksv3_mem_cvt2table(ssd, list.hash_order_pointers + from, n, &sst[idx], false);
        input->pg_start_hashes[idx] = list.hash_order_pointers[from]->meta.g1.hash >> LEVELLIST_HASH_SHIFTS;
        input->hash_lists[idx].hashes = calloc(n, sizeof(lksv_hash));
        input->hash_lists[idx].n = n;

        uint32_t str_order;
        for (int j = 0; j < n; j++) {
            input->hash_lists[idx].hashes[j].hash = list.hash_order_pointers[from+j]->meta.g1.hash;
            //str_order = list.hash_order_pointers[from + j]->str_order;
            str_order = ((int64_t) list.hash_order_pointers[from + j] - (int64_t) list.str_order_entries) / sizeof(lksv_comp_entry);
            //assert(str_order == list.hash_order_pointers[from + j]->str_order);

            //list.str_order_map[str_order].g1.sst = idx;
            //list.str_order_map[str_order].g1.off = j;
            //assert(((j << 8) | idx) == list.str_order_map[str_order].i);
            list.str_order_map[str_order].i = ((j << 8) | idx);
        }
        input->buffer[idx] = sst[idx].raw;

        idx++;
    }

    int nidx = idx;
    from = 0;
    for (idx = 0; idx < nidx; idx++) {
        lksv3_sst_encode_str_idx(&sst[idx], &list.str_order_map[from], sst[idx].footer.g.n);
        from += sst[idx].footer.g.n;
        FREE(sst[idx].meta);
    }
    input->hash_list_n = idx;
    input->n = mem->n;

#ifdef LK_OH
    end = qemu_clock_get_ns(QEMU_CLOCK_REALTIME);
    make += end - now;

    stat_count++;
    if (stat_count % 100 == 0) {
        double total = alloc + loop + qst + get_sekey + make;
        printf("[Overhead analysis]\n");
        printf("allocation: %lu (%.2lf)\n", alloc / stat_count, alloc / total);
        printf("for each sk loop: %lu (%.2lf)\n", loop / stat_count, loop / total);
        printf("qst: %lu (%.2lf)\n", qst / stat_count, qst / total);
        printf("get_sekey: %lu (%.2lf)\n", get_sekey / stat_count, get_sekey / total);
        printf("make: %lu (%.2lf)\n", make / stat_count, make / total);
    }
#endif

    FREE(list.hash_order_pointers);
    FREE(list.str_order_entries);
    FREE(list.str_order_map);
    return NULL;
}

