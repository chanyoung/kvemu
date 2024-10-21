#ifndef __FEMU_KVSSD_KV_TYPES_FTL_H
#define __FEMU_KVSSD_KV_TYPES_FTL_H

#include "hw/femu/kvssd/utils.h"
#include "hw/femu/nvme.h"
#include <stdint.h>
#include <stdbool.h>

#define KV_MIN_KEY_LEN 4
#define KV_MAX_KEY_LEN 255

typedef struct {
    char    *key;
    uint8_t len;
} kv_key;

typedef struct {
    char     *value;
    uint32_t length;
    uint32_t offset;
} kv_value;

extern kv_key kv_key_min, kv_key_max;
void kv_init_min_max_key(void);

// TODO: Do we really need to care the 0 length cases?
// It would be nice 0 length case is immediately returned w/ invalid code
// at 'nvme cmd' to 'kv cmd' translate layer, and ignore 0 length case.
static inline int kv_cmp_key(kv_key a, kv_key b)
{
    if (!a.len && !b.len) return 0;
    else if (a.len == 0) return -1;
    else if (b.len == 0) return 1;

    int r = memcmp(a.key, b.key, a.len > b.len ? b.len : a.len);
    if (r != 0 || a.len == b.len) {
        return r;
    }
    return a.len < b.len ? -1 : 1;
}

static inline char kv_test_key(kv_key a, kv_key b)
{
    if (a.len != b.len) return 0;
    return memcmp(a.key, b.key, a.len) ? 0 : 1;
}

static inline void kv_copy_key(kv_key *des, kv_key *key){
    des->key=(char*)malloc(sizeof(char)*key->len);
    des->len=key->len;
    memcpy(des->key,key->key,key->len);
}

static inline void kv_free_key(kv_key *des){
    FREE(des->key);
    FREE(des);
}

#endif
