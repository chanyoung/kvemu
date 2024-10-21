#include "hw/femu/kvssd/lksv/lksv3_ftl.h"

void lksv_open(struct kv_lsm_options *opts)
{
    lksv_lsm = calloc(1, sizeof(struct lksv3_lsmtree));
    lksv_lsm->opts = opts;

    kv_init_min_max_key();
    lksv_lsm->memtable = kv_skiplist_init();
}
