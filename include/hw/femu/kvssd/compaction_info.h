#ifndef __FEMU_KVSSD_KV_COMPACTION_INFO_H
#define __FEMU_KVSSD_KV_COMPACTION_INFO_H

#include <stdbool.h>
#include <pthread.h>
#include "hw/femu/kvssd/kv_types.h"

// Context of in-progress compactions.
typedef struct kv_compaction_info {
    pthread_spinlock_t    lock;
    bool                  in_progress;
    char                  input_level;
    char                  output_level;
    kv_key                smallest;
    kv_key                largest;
} kv_compaction_info;

void kv_set_compaction_info(kv_compaction_info *ctx, char input_level, char output_level);
void kv_reset_compaction_info(kv_compaction_info *ctx);
bool kv_level_being_compacted_without_unlock(kv_compaction_info *ctx, char level);
void kv_unlock_compaction_info(kv_compaction_info *ctx);
void kv_init_compaction_info(kv_compaction_info *ctx);
#endif
