#include "hw/femu/kvssd/lksv/lksv3_ftl.h"

bool lksv3_should_compact(lksv3_level *l) {
    if (l->idx == 0)
        return l->n_num >= l->m_num - 2;
    else
        return l->n_num >= l->m_num - lksv_lsm->disk[l->idx-1]->m_num;
}

const struct kv_lsm_operations lksv_lsm_operations = {
    .open = lksv_open,
};
