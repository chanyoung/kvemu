#ifndef __FEMU_LKSV3_CACHE_H
#define __FEMU_LKSV3_CACHE_H

#define CACHE_TYPES 3

// Predefined metadata type for cache.
// This types are also used for multiplier. Do not edit.
typedef enum {
    LEVEL_LIST_ENTRY   = 0,
    HASH_LIST          = 1,
    DATA_SEGMENT_GROUP = 2,
} cache_type;

static inline uint32_t cache_level(cache_type type, uint32_t level)
{
    // TODO: change hard coded level.
    return (type * 4) + level;
}

#endif
