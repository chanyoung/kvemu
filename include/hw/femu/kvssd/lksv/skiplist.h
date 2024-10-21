#ifndef __FEMU_LKSV_SKIPLIST_H
#define __FEMU_LKSV_SKIPLIST_H

#include "hw/femu/kvssd/skiplist.h"

#define snode_ppa(node) \
    (&((lksv_per_snode_data*)(node->private))->ppa)
#define snode_off(node) \
    (&((lksv_per_snode_data*)(node->private))->off)
#define snode_hash(node) \
    (&((lksv_per_snode_data*)(node->private))->hash)

typedef struct lksv_per_snode_data {
    struct femu_ppa ppa;
    uint32_t        hash;
    int             off;
} lksv_per_snode_data;

kv_snode *lksv3_skiplist_insert(kv_skiplist *list, kv_key key, kv_value* value, bool deletef, struct ssd *ssd);

#endif
