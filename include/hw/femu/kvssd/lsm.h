#ifndef __FEMU_KVSSD_LSM_H
#define __FEMU_KVSSD_LSM_H

#include <stdint.h>

// This is fixed value. Do not change the LSM-tree level.
#define LSM_LEVELN 4

struct kv_lsm_options {
    float    level_multiplier;
    uint64_t cache_memory_size;
    void     *private;
};

struct kv_lsm_operations {
    void (*open) (struct kv_lsm_options *opts);
};

enum lsm_type {
    PINK, LKSV,
};

struct kv_lsm_options *kv_lsm_default_opts(void);
void kv_lsm_setup_db(const struct kv_lsm_operations **lops, enum lsm_type type);
float calc_level_multiplier(int floor_n);

#endif
