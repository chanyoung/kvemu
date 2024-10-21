#include "hw/femu/kvssd/pink/pink_ftl.h"

bool should_compact(pink_level *l) {
    if (l->idx == 0)
        return l->n_num >= l->m_num - 2 - BULK_FLUSH_MARGIN;
    else
        return l->n_num >= l->m_num - pink_lsm->disk[l->idx-1]->m_num;
}

const struct kv_lsm_operations pink_lsm_operations = {
    .open = pink_open,
};
