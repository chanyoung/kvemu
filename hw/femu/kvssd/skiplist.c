#include "hw/femu/kvssd/skiplist.h"
#include "hw/femu/kvssd/utils.h"

kv_skiplist *kv_skiplist_init(void)
{
    kv_skiplist *skl = (kv_skiplist *) malloc(sizeof(kv_skiplist));
    skl->header = (kv_snode *) malloc(sizeof(kv_snode));
    skl->header->list = (kv_snode **) malloc(sizeof(kv_snode*)*(MAX_L+1));
    skl->level = 1;
    for(int i = 0; i < MAX_L; i++)
        skl->header->list[i] = skl->header;
    skl->header->back = skl->header;
    skl->header->key = kv_key_max;
    skl->header->value = NULL;
    skl->n = 0;
    return skl;
}

kv_snode *kv_skiplist_find(kv_skiplist *list, kv_key key)
{
    kv_snode *x;
    int i;
    if (!list) return NULL;
    if (list->n==0) return NULL;
    x = list->header;
    for (i = list->level; i >= 1; i--) {
        while (kv_cmp_key(x->list[i]->key, key) < 0)
            x = x->list[i];
    }
    if (kv_test_key(x->list[1]->key,key))
        return x->list[1];
    return NULL;
}

static inline int get_level(void)
{
    int level = 1;
    int temp = rand();
    while (temp % PROB == 1) {
        temp = rand();
        level++;
        if (level+1 >= MAX_L)
            break;
    }
    return level;
}

kv_snode *kv_skiplist_insert(kv_skiplist *list, kv_key key, kv_value* value)
{
    kv_snode *update[MAX_L+1];
    kv_snode *x=list->header;

    for(int i=list->level; i>=1; i--){
        while(kv_cmp_key(x->list[i]->key,key)<0)
            x=x->list[i];
        update[i]=x;
    }
    x=x->list[1];
    if(kv_test_key(key,x->key)) {
        list->val_size -= x->value->length;
        list->val_size += value->length;
        if(x->value) {
            FREE(x->value->value);
        }
        FREE(x->value);
        FREE(x->key.key);
        x->key = key;
        x->value=value;
        return x;
    } else {
        int level=get_level();
        if(level>list->level){
            for(int i=list->level+1; i<=level; i++){
                update[i]=list->header;
            }
            list->level=level;
        }
        x=(kv_snode*)calloc(1, sizeof(kv_snode));
        x->list=(kv_snode**)calloc(level+1, sizeof(kv_snode*));

        x->key=key;
        x->value=value;

        for(int i=1; i<=level; i++){
            x->list[i]=update[i]->list[i];
            update[i]->list[i]=x;
        }

        //new back
        x->back=x->list[1]->back;
        x->list[1]->back=x;
        kv_assert(x->back->list[1] == x);

        x->level=level;
        list->n++;
        list->key_size += key.len;
        list->val_size += value->length;
    }

    return x;
}

void kv_skiplist_get_start_end_key(kv_skiplist *sl, kv_key *start, kv_key *end)
{
    kv_assert(sl->n > 0);
    kv_copy_key(start, &sl->header->list[1]->key);
    kv_copy_key(end, &sl->header->back->key);
}

static void kv_skiplist_clear(kv_skiplist *list){
    kv_snode *now=list->header->list[1];
    kv_snode *next=now->list[1];
    while(now!=list->header){
        if(now->value){
            if (now->value->value) {
                FREE(now->value->value);
            }
            FREE(now->value);
        }
        if (now->private) {
            FREE(now->private);
        }
        FREE(now->key.key);
        FREE(now->list);
        FREE(now);
        now=next;
        next=now->list[1];
    }
    list->n=0;
    list->level=0;
    for(int i=0; i<MAX_L; i++)
        list->header->list[i]=list->header;
    list->header->key=kv_key_max;
}

void kv_skiplist_free (kv_skiplist *list) {
    if(list==NULL) return;
    kv_skiplist_clear(list);
    FREE(list->header->list);
    FREE(list->header);
    FREE(list);
    return;
}

kv_skiplist *kv_skiplist_divide(kv_skiplist *in, kv_snode *target, int num, int key_size, int val_size) {
    kv_skiplist *res = kv_skiplist_init();
    if (target == in->header) {
        kv_skiplist swap;
        memcpy(&swap, in, sizeof(kv_skiplist));
        memcpy(in, res, sizeof(kv_skiplist));
        memcpy(res, &swap, sizeof(kv_skiplist));
        return res;
    }

    uint32_t origin_level = in->level;
    res->level = in->level;

    kv_snode *x = in->header;
    for (uint32_t i = res->level; i >= 1; i--) {
        if (i >= target->level) {
            while(kv_cmp_key(x->list[i]->key, target->key)<=0)
                x = x->list[i];
        } else {
            kv_assert(x == target);
        }

        if (x == in->header) {
            // Nothing
            if (res->level == i) {
                res->level--;
            }
        } else if (x->list[i] == in->header) {
            // All
            if (in->level == i) {
                in->level--;
            }
            res->header->list[i] = in->header->list[i];
            x->list[i] = res->header;
            in->header->list[i] = in->header;
        } else {
            // Middle
            res->header->list[i] = in->header->list[i];
            in->header->list[i] = x->list[i];
            x->list[i] = res->header;
        }
    }
    res->header->back = target;
    res->header->list[1]->back = res->header;
    in->header->list[1]->back = in->header;

    res->n = num;
    res->key_size = key_size;
    res->val_size = val_size;
    in->n -= num;
    in->key_size -= key_size;
    in->val_size -= val_size;

    if (origin_level != in->level && origin_level != res->level) {
        printf("origin_level:%d in->level:%d\n", origin_level, in->level);
        printf("skiplist_divide error!\n");
        abort();
    }
    if (in->level == 0)
        in->level = 1;

    return res;
}

