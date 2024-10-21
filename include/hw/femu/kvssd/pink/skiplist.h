#ifndef __FEMU_PINK_SKIPLIST_H
#define __FEMU_PINK_SKIPLIST_H

#include "hw/femu/kvssd/skiplist.h"

#define snode_ppa(node) \
    (&((pink_per_snode_data*)(node->private))->ppa)
#define snode_off(node) \
    (&((pink_per_snode_data*)(node->private))->off)

typedef struct pink_per_snode_data {
    struct femu_ppa ppa;
    int             off;
} pink_per_snode_data;

typedef struct pink_length_bucket {
    kv_snode **bucket[MAXVALUESIZE+1];
    struct pink_gc_node **gc_bucket[MAXVALUESIZE+1];
    uint32_t indices[MAXVALUESIZE+1];
    kv_value** contents;
    int contents_num;
} pink_l_bucket;

pink_l_bucket *pink_skiplist_make_length_bucket(kv_skiplist *sl);
kv_skiplist *pink_skiplist_cutting_header(kv_skiplist *in, bool align_data_segment);

#endif
