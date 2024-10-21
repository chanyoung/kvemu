#include "hw/femu/kvssd/cache.h"
#include "hw/femu/kvssd/debug.h"
#include <stdlib.h>

kv_cache *kv_cache_init(uint32_t noe, uint32_t level) {
    kv_cache *c = (kv_cache *) calloc(1, sizeof(kv_cache));
    c->levels = (kv_cache_level *) calloc(level, sizeof(kv_cache_level));
    c->evictable_bytes = (uint32_t *) calloc(level, sizeof(uint32_t));
    c->level_n = level;
    c->max_bytes = c->free_bytes = noe;
    pthread_spin_init(&c->lock, PTHREAD_PROCESS_SHARED);
    return c;
}

static inline kv_cache_entry* _cache_get_lru(kv_cache_level *level) {
    kv_assert(level->bottom != NULL);
    kv_assert(*(level->bottom->entry) != NULL);
    return level->bottom;
}

static kv_cache_entry* cache_get_lru(kv_cache *c, uint32_t level) {
    kv_cache_entry *e = NULL;
    int i;

    if (c->free_bytes == c->max_bytes) {
        kv_assert(c->top == NULL);
        kv_assert(c->bottom == NULL);
        return NULL;
    }

    kv_assert(level <= c->level_n-1 && level >= 0);
    i = c->level_n - 1;
    for (i = c->level_n - 1; i > level; i--) {
        if (c->levels[i].n == 0)
            continue;
        e = _cache_get_lru(&c->levels[i]);
        if (e)
            break;
    }
    return e;
}

static void _kv_cache_delete_entry(kv_cache *c, kv_cache_entry *c_ent) {
    kv_cache_level *l = &c->levels[c_ent->level];
    kv_assert(c->free_bytes < c->max_bytes);
    kv_assert(*(c_ent->entry) == c_ent);
    if (c_ent == l->top) {
        l->top = c_ent->down;
    }
    if (c_ent == l->bottom) {
        l->bottom = c_ent->up;
    }
    if (c_ent->up) {
        c_ent->up->down = c_ent->down;
    }
    if (c_ent->down) {
        c_ent->down->up = c_ent->up;
    }
    c->free_bytes += c_ent->size;
    for (int i = 0; i < c_ent->level; i++) {
        c->evictable_bytes[i] -= c_ent->size;
    }
    *c_ent->entry = NULL;
    free(c_ent);
    l->n--;
}

void kv_cache_delete_entry(kv_cache *c, kv_cache_entry *c_ent) {
    pthread_spin_lock(&c->lock);
    if (c_ent == NULL) {
        pthread_spin_unlock(&c->lock);
        return;
    }
    _kv_cache_delete_entry(c, c_ent);
    pthread_spin_unlock(&c->lock);
}

void kv_cache_insert(kv_cache *c, kv_cache_entry **c_ent, uint32_t size, uint16_t level, uint16_t flags) {
    kv_cache_level *l;

    pthread_spin_lock(&c->lock);
    if (*c_ent || c->max_bytes < size) {
        pthread_spin_unlock(&c->lock);
        return;
    }
    while (c->free_bytes < size) {
        kv_cache_entry *victim = cache_get_lru(c, level);
        if (victim) {
            if (KV_CACHE_HAS_FLAG(victim, KV_CACHE_FLUSH_EVICTED))
                c->flush_callback(victim);
            _kv_cache_delete_entry(c, victim);
        } else {
            pthread_spin_unlock(&c->lock);
            return;
        }
    }
    *c_ent = (kv_cache_entry *) calloc(1, sizeof(kv_cache_entry));
    (*c_ent)->size = size;
    (*c_ent)->level = level;
    (*c_ent)->flags = flags;

    l = &c->levels[level];
    if (l->top) {
        l->top->up = *c_ent;
        (*c_ent)->down = l->top;
        l->top = *c_ent;
    } else {
        l->top = l->bottom = *c_ent;
    }
    c->free_bytes -= size;
    (*c_ent)->entry = c_ent;
    l->n++;
    for (int i = 0; i < level; i++) {
        c->evictable_bytes[i] += size;
    }
    pthread_spin_unlock(&c->lock);
}

void kv_cache_update(kv_cache *c, kv_cache_entry *c_ent) {
    kv_cache_level *l;

    pthread_spin_lock(&c->lock);
    l = &c->levels[c_ent->level];
    kv_assert(c->free_bytes > 0);
    if (l->top != c_ent) {
        c_ent->up->down = c_ent->down;
        if (l->bottom == c_ent) {
            l->bottom = l->bottom->up;
        } else {
            c_ent->down->up = c_ent->up;
        }
        l->top->up = c_ent;
        c_ent->down = l->top;
        l->top = c_ent;
        c_ent->up = NULL;
    }
    pthread_spin_unlock(&c->lock);
}

bool kv_is_cached(kv_cache *c, kv_cache_entry *c_ent) {
    bool cached;
    pthread_spin_lock(&c->lock);
    cached = c_ent ? true : false;
    pthread_spin_unlock(&c->lock);
    return cached;
}

void kv_cache_free (kv_cache *c) {
    pthread_spin_lock(&c->lock);
    while(c->free_bytes < c->max_bytes)
        _kv_cache_delete_entry(c, cache_get_lru(c, -1));
    pthread_spin_unlock(&c->lock);
    free(c->levels);
    free(c->evictable_bytes);
    free(c);
}

bool kv_cache_available(kv_cache *c, int level) {
    bool available;
    pthread_spin_lock(&c->lock);
    available = c->free_bytes + c->evictable_bytes[level] > 262144; // 256K
    pthread_spin_unlock(&c->lock);
    return available;
}

