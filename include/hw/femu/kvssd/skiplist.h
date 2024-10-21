#ifndef __FEMU_KVSSD_KV_SKIPLIST_H
#define __FEMU_KVSSD_KV_SKIPLIST_H

#include "hw/femu/kvssd/kv_types.h"
#include "hw/femu/kvssd/settings.h"  // TODO: cleanup

#define MAX_L 30 // max skiplist level
#define PROB 4   // the probaility of level increasing : 1/PROB => 1/4
#define for_each_sk(node, skip) \
    for (node = skip->header->list[1]; \
         node != skip->header; \
         node = node->list[1])

#define for_each_reverse_sk(node, skip) \
    for (node = skip->header->back; \
         node != skip->header; \
         node = node->back)

typedef struct kv_snode {
    kv_key          key;
    kv_value        *value;
    uint8_t         level;
    struct kv_snode **list;
    struct kv_snode *back;
    void            *private;
} kv_snode;

typedef struct kv_skiplist {
    kv_snode *header;
    uint8_t  level;
    uint32_t key_size;
    uint32_t val_size;
    uint32_t n;
} kv_skiplist;

kv_skiplist *kv_skiplist_init(void);
kv_snode *kv_skiplist_find(kv_skiplist *list, kv_key key);
kv_snode *kv_skiplist_insert(kv_skiplist *list, kv_key key, kv_value* value);
void kv_skiplist_get_start_end_key(kv_skiplist *sl, kv_key *start, kv_key *end);
kv_skiplist *kv_skiplist_divide(kv_skiplist *in, kv_snode *target, int num, int key_size, int val_size);
void kv_skiplist_free(kv_skiplist *list);

#endif
