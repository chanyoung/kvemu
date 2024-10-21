#ifndef __FEMU_KVSSD_KV_CACHE_H
#define __FEMU_KVSSD_KV_CACHE_H

#include <stdint.h>
#include <stdbool.h>
#include <pthread.h>

// kv_cache_entry->flags.
#define KV_CACHE_WITHOUT_FLAGS (0)
#define KV_CACHE_FLUSH_EVICTED (1U << 0)  // Flush when evicted

#define KV_CACHE_HAS_FLAG(ent, flag) ((ent)->flags & (flag))

typedef struct kv_cache_entry {
    struct kv_cache_entry *up;
    struct kv_cache_entry *down;
    struct kv_cache_entry **entry;
    uint32_t              size;
    uint16_t              level;
    uint16_t              flags;
} kv_cache_entry;

typedef struct kv_cache_level {
    kv_cache_entry *top;
    kv_cache_entry *bottom;
    int            n;
} kv_cache_level;

typedef struct kv_cache {
    kv_cache_level        *levels;
    uint32_t              max_bytes;
    uint32_t              free_bytes;
    uint32_t              *evictable_bytes;
    pthread_spinlock_t    lock;
    uint32_t              level_n;
    void                  (*flush_callback) (kv_cache_entry *ent);
} kv_cache;

kv_cache *kv_cache_init(uint32_t n, uint32_t level);
void kv_cache_insert(kv_cache *, kv_cache_entry **c_ent, uint32_t size, uint16_t level, uint16_t flags);
void kv_cache_delete_entry(kv_cache *c, kv_cache_entry *c_ent);
void kv_cache_update(kv_cache *, kv_cache_entry *);
void kv_cache_free(kv_cache *);
bool kv_is_cached(kv_cache *, kv_cache_entry *);
bool kv_cache_available(kv_cache *, int level);

#endif
