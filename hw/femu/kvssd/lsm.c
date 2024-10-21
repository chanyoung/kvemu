#include "hw/femu/kvssd/lsm.h"
#include "hw/femu/kvssd/debug.h"
#include "hw/femu/kvssd/pink/lsm.h"
#include "hw/femu/kvssd/lksv/lsm.h"

#include <math.h>

// Note that STARTING_LEVEL_MULTIPLIER is just the initial value of
// level multiplier. We fixed the LSM-tree level at 4, and dynamically
// change the level multiplier depending on the state of the DB.
#define STARTING_LEVEL_MULTIPLIER 20.0f

struct kv_lsm_options *kv_lsm_default_opts(void)
{
    struct kv_lsm_options *opts;

    opts = (struct kv_lsm_options *) calloc(1, sizeof(struct kv_lsm_options));
    opts->level_multiplier = STARTING_LEVEL_MULTIPLIER;

    return opts;
}

float calc_level_multiplier(int floor_n)
{
    float f = ceil(pow(10, log10(floor_n) / LSM_LEVELN));
    while (pow(f, LSM_LEVELN) > floor_n)
        f -= 0.05f;
    return f;
}

void kv_lsm_setup_db(const struct kv_lsm_operations **lops, enum lsm_type type)
{
    switch (type) {
        case PINK:
            *lops = &pink_lsm_operations;
            break;
        case LKSV:
            *lops = &lksv_lsm_operations;
            break;
        default:
            kv_err("unknown lsm type: %d", type);
            break;
    }
}
