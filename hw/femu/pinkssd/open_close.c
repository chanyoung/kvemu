#include "hw/femu/kvssd/pink/pink_ftl.h"

void pink_open(struct kv_lsm_options *opts)
{
    pink_lsm = calloc(1, sizeof(struct pink_lsmtree));
    pink_lsm->opts = opts;

    kv_init_min_max_key();
    pink_lsm->memtable = kv_skiplist_init();
}
