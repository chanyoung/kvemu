#include "hw/femu/kvssd/debug.h"
#include "hw/femu/kvssd/compaction_info.h"

void kv_set_compaction_info(kv_compaction_info *ctx, char input_level, char output_level)
{
    pthread_spin_lock(&ctx->lock);
    kv_assert(!ctx->in_progress);
    ctx->in_progress = true;
    ctx->input_level = input_level;
    ctx->output_level = output_level;
    pthread_spin_unlock(&ctx->lock);
}

void kv_reset_compaction_info(kv_compaction_info *ctx)
{
    pthread_spin_lock(&ctx->lock);
    kv_assert(ctx->in_progress);
    ctx->in_progress = false;
    pthread_spin_unlock(&ctx->lock);
}

bool kv_level_being_compacted_without_unlock(kv_compaction_info *ctx, char level)
{
    bool being_compacted = false;
    pthread_spin_lock(&ctx->lock);
    if (ctx->in_progress) {
        if (level == ctx->input_level || level == ctx->output_level)
            being_compacted = true;
    }
    return being_compacted;
}

void kv_unlock_compaction_info(kv_compaction_info *ctx)
{
    pthread_spin_unlock(&ctx->lock);
}

// Must be called by parent thread of FTL and COMP threads,
// and before they are created.
void kv_init_compaction_info(kv_compaction_info *ctx)
{
    pthread_spin_init(&ctx->lock, PTHREAD_PROCESS_PRIVATE);
}
