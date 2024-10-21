#include "hw/femu/kvssd/lksv/lksv3_ftl.h"
#include "hw/femu/kvssd/lksv/skiplist.h"

kv_snode *lksv3_skiplist_insert(kv_skiplist *list, kv_key key, kv_value* value, bool deletef, struct ssd *ssd){
    kv_snode *t = kv_skiplist_insert(list, key, value);
    if (t->private) {
        ssd->lm.lines[snode_ppa(t)->g.blk].isc++;
        ssd->lm.lines[snode_ppa(t)->g.blk].vsc--;
    }
    return t;
}

static kv_skiplist *_lksv_skiplist_cutting_header(kv_skiplist *skl, const uint32_t ms_pg_n, const uint32_t ms_pg_size_limit, bool with_value, bool left) {
    const uint32_t per_key_data = LKSV3_SSTABLE_META_BLK_SIZE
                                  + LKSV3_SSTABLE_STR_IDX_SIZE;
    int ms_pg_cnt = 1;
    int ms_pg_used = 0;
    int ms_key_num = 0;
    int ms_meta_size = 0;

    int cutting_num = 0;
    int cutting_key_size = 0;
    int cutting_val_size = 0;

    kv_snode *node;

    // Fastpath: If skl is fit to a single page, then return immediately.
    if (skl->key_size + (skl->n * per_key_data) < ms_pg_size_limit) {
        return skl;
    }

    if (left) {
        for_each_sk(node, skl) {
            ms_meta_size = node->key.len + per_key_data
                           + (with_value ? node->value->length : PPA_LENGTH);
            ms_pg_used += ms_meta_size;
            ms_key_num++;
            if (ms_pg_used > ms_pg_size_limit) {
                if (ms_pg_cnt >= ms_pg_n) {
                    node = node->back;
                    break;
                } else {
                    ms_pg_cnt++;
                    ms_pg_used = ms_meta_size;
                }
            }
            cutting_num++;
            cutting_key_size += node->key.len;
            cutting_val_size += node->value->length;
        }
    } else {
        for_each_reverse_sk(node, skl) {
            ms_meta_size = node->key.len + per_key_data
                           + (with_value ? node->value->length : PPA_LENGTH);
            ms_pg_used += ms_meta_size;
            ms_key_num++;
            if (ms_pg_used > ms_pg_size_limit) {
                if (ms_pg_cnt >= ms_pg_n) {
                    break;
                } else {
                    ms_pg_cnt++;
                    ms_pg_used = ms_meta_size;
                }
            }
            cutting_num++;
            cutting_key_size += node->key.len;
            cutting_val_size += node->value->length;
        }
        cutting_num = skl->n - cutting_num;
        cutting_key_size = skl->key_size - cutting_key_size;
        cutting_val_size = skl->val_size - cutting_val_size;
    }
    return (node == skl->header) ? skl : kv_skiplist_divide(skl, node, cutting_num, cutting_key_size, cutting_val_size);
}

kv_skiplist *lksv_skiplist_cutting_header(kv_skiplist *skl, bool before_log_write, bool with_value, bool left) {
    uint32_t ms_pg_n, ms_pg_size_limit;
    if (before_log_write) {
        ms_pg_n = 1; // Just for consistency with PinK
        ms_pg_size_limit = PAGESIZE - LKSV3_SSTABLE_FOOTER_BLK_SIZE;
    } else {
        ms_pg_n = PG_N;
        ms_pg_size_limit = PAGESIZE - LKSV3_SSTABLE_FOOTER_BLK_SIZE;
    }
    return _lksv_skiplist_cutting_header(skl, ms_pg_n, ms_pg_size_limit, with_value, left);
}

lksv3_l_bucket *lksv3_skiplist_make_length_bucket(kv_skiplist *sl)
{
    lksv3_l_bucket *b = (lksv3_l_bucket *) calloc(1, sizeof(lksv3_l_bucket));
    kv_snode *target;

    /*
     * Packing values into the bucket by their data length (unit of PIECE)
     */
    for_each_sk (target, sl) {
        if (target->value == NULL || target->value->length == 0)
            abort();

        int vsize = target->value->length;
        if (b->bucket[vsize] == NULL)
            b->bucket[vsize] = (kv_snode**)calloc(1, sizeof(kv_snode*) * (sl->n + 1));
        b->bucket[vsize][b->indices[vsize]++] = target;
    }

    return b;
}

