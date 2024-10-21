#include "hw/femu/kvssd/kv_types.h"

kv_key kv_key_min, kv_key_max;
char key_min_buf[KV_MIN_KEY_LEN];
char key_max_buf[KV_MAX_KEY_LEN];

/*
 * kv_init_min_max_key sets the minimum and maximum keys in the lexical order
 * allowed by the KV interface.
 */
void kv_init_min_max_key(void) {
    kv_key_min.len = KV_MIN_KEY_LEN;
    kv_key_min.key = key_min_buf;
    memset(kv_key_min.key, 0, sizeof(char) * KV_MIN_KEY_LEN);
    kv_key_max.len = KV_MAX_KEY_LEN;
    kv_key_max.key = key_max_buf;
    memset(kv_key_max.key, -1, sizeof(char) * KV_MAX_KEY_LEN);
}

