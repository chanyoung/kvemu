#include "hw/femu/kvssd/pink/pink_ftl.h"
#include "hw/femu/kvssd/pink/skiplist.h"

#define PBODY_PADDING 2

static void array_range_update(pink_level *lev, pink_level_list_entry* r, kv_key key)
{
    if(kv_cmp_key(lev->start, key) > 0)
        lev->start = key;
    if(kv_cmp_key(lev->end,key) < 0)
        lev->end = key;
}

pink_level* level_init(int idx)
{
    int size = (int) pink_lsm->opts->level_multiplier;
    
    for (int i = 0; i < idx; i++)
        size *= (int) pink_lsm->opts->level_multiplier;

    pink_level *res = (pink_level*) calloc(1, sizeof(pink_level));
    res->level_data = (pink_level_list_entry**)calloc(size, sizeof(pink_level_list_entry *));

    res->idx=idx;
    res->m_num=size;
    res->n_num=0;
    res->start=kv_key_max;
    res->end=kv_key_min;

    return res;
}

void free_run(struct pink_lsmtree *LSM, pink_level_list_entry *e) {
    kv_cache_delete_entry(LSM->lsm_cache, e->cache[LEVEL_LIST_ENTRY]);
    // Meta segment cache must be freed during the compaction process.
    kv_assert(!kv_is_cached(pink_lsm->lsm_cache, e->cache[META_SEGMENT]));
    if (e->buffer) {
        FREE(e->buffer);
    }
    if (e->smallest.key) {
        FREE(e->smallest.key);
    }
    if (e->largest.key) {
        FREE(e->largest.key);
    }
    pink_lput(e);
}

static void array_body_free(struct pink_lsmtree *LSM, pink_level_list_entry **runs, int size){
    for(int i=0; i<size; i++){
        free_run(LSM, runs[i]);
    }
    FREE(runs);
}

void free_level(struct pink_lsmtree *LSM, pink_level* lev) {
    array_body_free(LSM, lev->level_data, lev->n_num);
    FREE(lev);
}

static void array_run_cpy_to(pink_level_list_entry *input, pink_level_list_entry *res, int idx){
    kv_copy_key(&res->smallest, &input->smallest);
    kv_copy_key(&res->largest, &input->largest);

    res->ppa=input->ppa;

    pthread_spin_lock(&pink_lsm->lsm_cache->lock);
    if (input->cache[META_SEGMENT]) {
        res->cache[META_SEGMENT]=input->cache[META_SEGMENT];
        res->cache[META_SEGMENT]->entry=&res->cache[META_SEGMENT];
        input->cache[META_SEGMENT]=NULL;
    } else {
        res->cache[META_SEGMENT]=NULL;
    }
    if (input->cache[LEVEL_LIST_ENTRY]) {
        res->cache[LEVEL_LIST_ENTRY]=input->cache[LEVEL_LIST_ENTRY];
        res->cache[LEVEL_LIST_ENTRY]->entry=&res->cache[LEVEL_LIST_ENTRY];
        input->cache[LEVEL_LIST_ENTRY]=NULL;
    } else {
        res->cache[LEVEL_LIST_ENTRY]=NULL;
    }
    pthread_spin_unlock(&pink_lsm->lsm_cache->lock);

    if (input->buffer) {
        res->buffer = input->buffer;
        input->buffer = NULL;
    }
}

void read_run_delay_comp(pink_level *lev) {
    int p = 0;
    int end = lev->n_num;
    int last_read_run_idx = INT32_MAX;
    while (p < end) {
        // TODO: LEVEL_READ_DELAY
        if (kv_is_cached(pink_lsm->lsm_cache, lev->level_data[p]->cache[LEVEL_LIST_ENTRY])) {
            kv_cache_delete_entry(pink_lsm->lsm_cache, lev->level_data[p]->cache[LEVEL_LIST_ENTRY]);
        } else if (last_read_run_idx != p / LEVEL_LIST_ENTRY_PER_PAGE) {
            last_read_run_idx = p / LEVEL_LIST_ENTRY_PER_PAGE;

            struct nand_cmd srd;
            srd.type = COMP_IO;
            srd.cmd = NAND_READ;
            srd.stime = 0;

            struct femu_ppa fake_ppa;
            fake_ppa.ppa = 0;
            fake_ppa.g.blk = last_read_run_idx % pink_ssd->sp.blks_per_pl;

            pink_ssd_advance_status(&fake_ppa, &srd); 
        }
        p++;
    }
}

pink_level_list_entry* insert_run(pink_level *lev, pink_level_list_entry* r) {
    if(lev->m_num <= lev->n_num) {
        abort();
    }
    kv_assert(!kv_is_cached(pink_lsm->lsm_cache, r->cache[LEVEL_LIST_ENTRY]));
    kv_cache_insert(pink_lsm->lsm_cache, &r->cache[LEVEL_LIST_ENTRY], r->smallest.len + PPA_LENGTH, cache_level(LEVEL_LIST_ENTRY, lev->idx), KV_CACHE_WITHOUT_FLAGS);

    pink_level_list_entry **arrs = lev->level_data;
    arrs[lev->n_num] = pink_lnew();
    pink_level_list_entry *target = arrs[lev->n_num];
    array_run_cpy_to(r, target, lev->idx);

    array_range_update(lev, NULL, target->smallest);
    array_range_update(lev, NULL, target->largest);

    lev->n_num++;
    return target;
}

keyset* find_keyset(char *data, kv_key lpa) {
    char *body = data;
    uint16_t *bitmap = (uint16_t*)body;
    uint16_t *vbitmap = (uint16_t*)(body + KEYBITMAP);
    int s = 1, e = bitmap[0];
    kv_key target;
    while (s <= e) {
        int mid = (s + e) / 2;
        target.key = &body[bitmap[mid] + sizeof(struct femu_ppa)];
        target.len = bitmap[mid+1] - bitmap[mid] - sizeof(struct femu_ppa);
        int res = kv_cmp_key(target, lpa);
        if (res == 0) {
            keyset *res = (keyset *) malloc (sizeof(keyset));
            memcpy(&res->ppa, &body[bitmap[mid]], sizeof(struct femu_ppa));
            kv_copy_key(&res->lpa.k, &target);
            res->lpa.line_age.age = vbitmap[mid];
            return res;
        } else if(res < 0) {
            s = mid + 1;
        } else {
            e = mid - 1;
        }
    }
    return NULL;
}

pink_level_list_entry *find_run(pink_level* lev, kv_key lpa, NvmeRequest *req){
    pink_level_list_entry **arrs=lev->level_data;
    if(!arrs || lev->n_num==0) return NULL;
    int end=lev->n_num-1;
    int start=0;
    int mid;

    int res1; //1:compare with start, 2:compare with end
    mid=(start+end)/2;

    int last_read_run_idx = INT32_MAX;
    while(1){
        // TODO: LEVEL_READ_DELAY
        if (!arrs[mid]->cache[LEVEL_LIST_ENTRY] &&
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
            fake_ppa.g.blk = last_read_run_idx % pink_ssd->sp.blks_per_pl;
            uint64_t sublat = pink_ssd_advance_status(&fake_ppa, &srd); 
            if (req) {
                req->etime += sublat;
                req->flash_access_count++;
            }
        }

        res1=kv_cmp_key(arrs[mid]->smallest,lpa);
        if(res1>0) end=mid-1;
        else if(res1<0) start=mid+1;
        else {
            return arrs[mid];
        }
        mid=(start+end)/2;
        if(start>end){
            return arrs[mid];
        }
    }
    return NULL;
}

pink_level_list_entry *find_run2(pink_level* lev, kv_key lpa, NvmeRequest *req){
    pink_level_list_entry **arrs=lev->level_data;
    if(!arrs || lev->n_num==0) return NULL;
    int end=lev->n_num-1;
    int start=0;
    int mid;

    int res1; //1:compare with start, 2:compare with end
    mid=(start+end)/2;

    while(1){
        res1=kv_cmp_key(arrs[mid]->smallest,lpa);
        if(res1>0) end=mid-1;
        else if(res1<0) start=mid+1;
        else {
            return arrs[mid];
        }
        mid=(start+end)/2;
        if(start>end){
            return arrs[mid];
        }
    }
    return NULL;
}

pink_level_list_entry *make_run(kv_key start, kv_key end, struct femu_ppa ppa){
    pink_level_list_entry * res=(pink_level_list_entry*)calloc(1, sizeof(pink_level_list_entry));
    kv_copy_key(&res->smallest,&start);
    kv_copy_key(&res->largest,&end);
    res->ppa = ppa;
    res->cache[META_SEGMENT]=NULL;
    return res;
}

void print_level_summary(struct pink_lsmtree *LSM) {
    for(int i=0; i<LSM_LEVELN; i++){
        if(LSM->disk[i]->n_num==0){
            kv_log("[%d] n_num:%d m_num:%d\n",i+1,LSM->disk[i]->n_num,LSM->disk[i]->m_num);
        }
        else {
            kv_log("[%d (%.*s ~ %.*s)] n_num:%d m_num:%d %.*s ~ %.*s\n",i+1,KEYFORMAT(LSM->disk[i]->start),KEYFORMAT(LSM->disk[i]->end),LSM->disk[i]->n_num,LSM->disk[i]->m_num,KEYFORMAT(LSM->disk[i]->start),KEYFORMAT(LSM->disk[i]->end));
        }
    }
}

uint32_t range_find_compaction(pink_level *lev, kv_key s, kv_key e, pink_level_list_entry ***rc){
    pink_level_list_entry **arrs=lev->level_data;
    pink_level_list_entry **r=(pink_level_list_entry**)malloc(sizeof(pink_level_list_entry*)*(lev->n_num+1));

    // TODO: find range.
    for(int i = 0; i < lev->n_num; i++)
        r[i] = arrs[i];

    r[lev->n_num] = NULL;
    *rc = r;
    return lev->n_num;
}

static p_body *pbody_init(char **data,uint32_t size, pl_run *pl_datas, bool read_from_run){
    p_body *res=(p_body*)calloc(1, sizeof(p_body));
    res->data_ptr=data;
    res->max_page=size;
    res->read_from_run=read_from_run;
    res->pl_datas=pl_datas;
    return res;
}

static void new_page_set(p_body *p, bool iswrite){
    if(p->read_from_run){
        p->now_page=data_from_run(p->pl_datas[p->pidx].r);
    }
    else{
        if(p->pidx>=p->max_page){
            kv_debug("%d %d \n", p->pidx, p->max_page);
        }
        p->now_page=p->data_ptr[p->pidx];

    }
    if(iswrite && !p->now_page){
        p->now_page=(char*)malloc(PAGESIZE);
    }
    p->bitmap_ptr=(uint16_t *)p->now_page;
    p->vbitmap_ptr=(uint16_t *)(p->now_page + KEYBITMAP);
    p->kidx=1;
    p->max_key=p->bitmap_ptr[0];
    p->length=KEYBITMAP + VERSIONBITMAP;
    p->pidx++;
}

static PINK_KEYAGET pbody_get_next_key(p_body *p, struct femu_ppa *ppa){
    if(!p->now_page || (p->pidx<p->max_page && p->kidx>p->max_key)){
        new_page_set(p,false);
    }

    PINK_KEYAGET res;
    res.k.len = 0;
    res.k.key = NULL;
    if(p->pidx>=p->max_page && p->kidx>p->max_key){
        res.k.len=-1;
        return res;
    }

    memcpy(ppa,&p->now_page[p->bitmap_ptr[p->kidx]],sizeof(struct femu_ppa));
    res.k.len=p->bitmap_ptr[p->kidx+1]-p->bitmap_ptr[p->kidx]-sizeof(struct femu_ppa);
    res.k.key=&p->now_page[p->bitmap_ptr[p->kidx]+sizeof(struct femu_ppa)];
    res.line_age.age = p->vbitmap_ptr[p->kidx];
    p->kidx++;
    return res;
}

static char *pbody_insert_new_key(p_body *p, PINK_KEYAGET key, struct femu_ppa ppa, bool flush)
{
    char *res=NULL;
    if((flush && p->kidx>1) || !p->now_page || p->kidx>=(KEYBITMAP)/sizeof(uint16_t)-2 || p->length+(key.k.len+sizeof(struct femu_ppa))>PAGESIZE){
        if(p->now_page){
            p->bitmap_ptr[0]=p->kidx-1;
            p->bitmap_ptr[p->kidx]=p->length;
            p->vbitmap_ptr[0]=p->kidx-1;
            p->data_ptr[p->pidx-1]=p->now_page;
            res=p->now_page;
        }
        if(flush){
            return res;
        }
        new_page_set(p,true);
    }

    char *target_idx=&p->now_page[p->length];
    memcpy(target_idx,&ppa,sizeof(struct femu_ppa));
    memcpy(&target_idx[sizeof(struct femu_ppa)],key.k.key,key.k.len);

    p->vbitmap_ptr[p->kidx]=key.line_age.age;
    p->bitmap_ptr[p->kidx++]=p->length;
    p->length+=sizeof(struct femu_ppa)+key.k.len;

    return res;
}

static char *pbody_get_data(p_body *p, bool init)
{
    if(init){
        p->max_page=p->pidx;
        p->pidx=0;
    }

    if(p->pidx<p->max_page){
        return p->data_ptr[p->pidx++];
    }
    else{
        return NULL;
    }
}

static char *pbody_clear(p_body *p){
    FREE(p);
    return NULL;
}

static char *array_skip_cvt2_data(kv_skiplist *mem){
    char *res=(char*)malloc(PAGESIZE);
    uint16_t *bitmap=(uint16_t *)res;
    uint32_t idx=1;
    uint16_t data_start=KEYBITMAP+VERSIONBITMAP;
    uint16_t *vbitmap=(uint16_t *)(res + KEYBITMAP);
    kv_snode *temp;

    for_each_sk(temp,mem){
        memcpy(&res[data_start],snode_ppa(temp),sizeof(struct femu_ppa));
        memcpy(&res[data_start+sizeof(struct femu_ppa)],temp->key.key,temp->key.len);
        bitmap[idx]=data_start;
        struct line_age age;
        age.g.in_page_idx = *snode_off(temp);
        age.g.line_age = (get_line(snode_ppa(temp))->age % LINE_AGE_MAX);
        vbitmap[idx] = age.age;

        data_start+=temp->key.len+sizeof(struct femu_ppa);
        idx++;
    }
    bitmap[0]=idx-1;
    vbitmap[0]=idx-1;
    bitmap[idx]=data_start;
    return res;
}

void merger(struct kv_skiplist* mem, pink_level_list_entry** s, pink_level_list_entry** o, struct pink_level* d){
    pink_lsm->cutter_start=true;
    int o_num=0; int u_num=0;
    char **u_data;
    if(mem){
        kv_skiplist *skip = mem;
        u_num = 1;
        u_data=(char**)malloc(sizeof(char*)*u_num);
        u_data[0]=array_skip_cvt2_data(skip);
    }
    else{
        for(int i=0; s[i]!=NULL; i++) u_num++;
        u_data=(char**)malloc(sizeof(char*)*u_num);
        for(int i=0; i<u_num; i++) {
            u_data[i]=data_from_run(s[i]);
            if(!u_data[i]) abort();
        }
    }

    for(int i=0;o[i]!=NULL ;i++) o_num++;
    char **o_data=(char**)malloc(sizeof(char*)*o_num);
    for(int i=0; o[i]!=NULL; i++){
        o_data[i]=data_from_run(o[i]);
        if(!o_data[i]) {
            abort();
        }
    }

    pink_lsm->r_data=(char**)calloc(o_num + u_num + PBODY_PADDING, sizeof(char*));
    p_body *lp, *hp;
    lp=pbody_init(o_data,o_num,NULL,false);
    hp=pbody_init(u_data,u_num,NULL,false);
    pink_lsm->rp=pbody_init(pink_lsm->r_data, o_num + u_num + PBODY_PADDING, NULL, false);

    struct femu_ppa lppa, hppa, rppa;
    PINK_KEYAGET lp_key;
    if (o_num == 0)
    {
        lp_key.k.len = 0;
        lp_key.k.key = NULL;
        lp_key.k.len=-1;
    }
    else
    {
        lp_key = pbody_get_next_key(lp, &lppa);
    }
    PINK_KEYAGET hp_key=pbody_get_next_key(hp,&hppa);
    PINK_KEYAGET insert_key;
    memset(&insert_key, 0, sizeof(PINK_KEYAGET));
    int next_pop=0;
    int result_cnt=0;
    while(!(lp_key.k.len==UINT8_MAX && hp_key.k.len==UINT8_MAX)){
        if(lp_key.k.len==UINT8_MAX){
            insert_key=hp_key;
            rppa=hppa;
            next_pop=1;
        }
        else if(hp_key.k.len==UINT8_MAX){
            insert_key=lp_key;
            rppa=lppa;
            next_pop=-1;
        }
        else{
            next_pop=kv_cmp_key(lp_key.k,hp_key.k);
            if(next_pop<0){
                insert_key=lp_key;
                rppa=lppa;
            }
            else if(next_pop>0){
                insert_key=hp_key;
                rppa=hppa;
            }
            else{
                // Same key from upper level.
                // Invalidate the lowwer level section.
                struct line *line = get_line(&lppa);
                if (line->vsc > 0 && line->age % LINE_AGE_MAX == lp_key.line_age.g.line_age) {
                    // TODO: We avoid increasing invalid sector counter on the erased block.
                    // We do this approximately by bypassing the page when the line is free status, but this is not always be true.
                    // For example, the case if the erased block is assigned to other block, this approximation is not working.
                    //kv_debug("same key from upper\n");
                    mark_sector_invalid(&lppa);
                }
                rppa=hppa;
                insert_key=hp_key;
            }
        }
        if((pbody_insert_new_key(pink_lsm->rp,insert_key,rppa,false)))
        {
            result_cnt++;
        }

        if(next_pop<0) {
            lp_key=pbody_get_next_key(lp,&lppa);
        } else if(next_pop>0) {
            hp_key=pbody_get_next_key(hp,&hppa);
        } else {
            lp_key=pbody_get_next_key(lp,&lppa);
            hp_key=pbody_get_next_key(hp,&hppa);
        }
    }
    if((pbody_insert_new_key(pink_lsm->rp,insert_key,rppa,true)))
    {
        result_cnt++;
    }

    if(mem) {
        for (int i = 0; i < u_num; i++) {
            FREE(u_data[i]);
        }
    }
    FREE(o_data);
    FREE(u_data);
    pbody_clear(lp);
    pbody_clear(hp);
}

static pink_level_list_entry *array_pipe_make_run(struct pink_lsmtree *LSM, char *data,uint32_t level_idx)
{
    kv_key start,end;
    uint16_t *body=(uint16_t*)data;
    uint32_t num=body[0];

    start.len=body[2]-body[1]-sizeof(struct femu_ppa);
    start.key=&data[body[1]+sizeof(struct femu_ppa)];

    end.len=body[num+1]-body[num]-sizeof(struct femu_ppa);
    end.key=&data[body[num]+sizeof(struct femu_ppa)];

    struct femu_ppa unmapped_ppa;
    unmapped_ppa.ppa = UNMAPPED_PPA;
    pink_level_list_entry *r=make_run(start,end, unmapped_ppa);
    r->buffer = data;
    return r;
}

pink_level_list_entry *cutter(struct pink_lsmtree *LSM, struct kv_skiplist* mem, struct pink_level* d, kv_key* _start, kv_key *_end){
    char *data;
    if(LSM->cutter_start){
        LSM->cutter_start=false;
        data=pbody_get_data(LSM->rp,true);
    }
    else{
        data=pbody_get_data(LSM->rp,false);
    }
    if(!data) {
        FREE(LSM->r_data);
        pbody_clear(LSM->rp);
        return NULL;
    }

    return array_pipe_make_run(LSM, data,d->idx);
}

