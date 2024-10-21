#include "hw/femu/kvssd/pink/pink_ftl.h"
#include "hw/femu/kvssd/pink/skiplist.h"

kv_skiplist *pink_skiplist_cutting_header(kv_skiplist *skl, bool align_data_segment) {
    const uint32_t ms_pg_n_limit = KEYBITMAP / sizeof(uint16_t) - 2;
    const uint32_t ms_pg_size_limit = PAGESIZE - KEYBITMAP - VERSIONBITMAP;
    const int target_ds_aligned_pcent = 95;

    int ds_pg_cnt = 1;
    int ds_pg_used = 0;
    int ds_data_size = 0;
    int ds_aligned_pcent = 0;

    int ms_pg_cnt = 1;
    int ms_pg_used = 0;
    int ms_key_num = 0;
    int ms_meta_size = 0;

    int cutting_num = 0;
    int cutting_key_size = 0;
    int cutting_val_size = 0;

    kv_snode *node;

    // Fastpath: If skl is fit to a single page, then return immediately.
    if (!align_data_segment &&
        skl->key_size + (skl->n * PPA_LENGTH) < ms_pg_size_limit) {
        return skl;
    }

    for_each_sk(node, skl) {
        ms_meta_size = node->key.len + PPA_LENGTH;
        ms_pg_used += ms_meta_size;
        ms_key_num++;

        if (ms_pg_used > ms_pg_size_limit || ms_key_num > ms_pg_n_limit) {
            if (align_data_segment) {
                ds_aligned_pcent = 100 * (((ds_pg_cnt - 1) * PAGESIZE) + ds_pg_used) / (ds_pg_cnt * PAGESIZE);
                if (ds_aligned_pcent > target_ds_aligned_pcent) {
                    kv_assert(ds_aligned_pcent < 100);
                    // kv_debug("meta_pg_cnt: %d, data_pg_cnt: %d, space usage: %d\n", ms_pg_cnt, ds_pg_cnt, ds_aligned_pcent);
                    node = node->back;
                    break;
                }
                ms_pg_cnt++;
                ms_pg_used = ms_meta_size;
                ms_key_num = 1;
            } else {
                node = node->back;
                break;
            }
        }

        if (align_data_segment) {
            ds_data_size = node->value->length + node->key.len + 8;
            ds_pg_used += ds_data_size;
            if (ds_pg_used > PAGESIZE) {
                ds_pg_cnt++;
                ds_pg_used = ds_data_size;
            }
        }

        cutting_num++;
        cutting_key_size += node->key.len;
        cutting_val_size += node->value->length;
    }
    return (node == skl->header) ? skl : kv_skiplist_divide(skl, node, cutting_num, cutting_key_size, cutting_val_size);
}

pink_l_bucket *pink_skiplist_make_length_bucket(kv_skiplist *sl)
{
    pink_l_bucket *b = (pink_l_bucket *) calloc(1, sizeof(pink_l_bucket));
    kv_snode *target;

    /*
     * Packing values into the bucket by their data length (unit of PIECE)
     */
    for_each_sk (target, sl) {
        if (target->value == NULL || target->value->length == 0)
            abort();

        int vsize = target->value->length;
        if (b->bucket[vsize] == NULL)
            b->bucket[vsize] = (kv_snode**)malloc(sizeof(kv_snode*) * (sl->n + 1));
        b->bucket[vsize][b->indices[vsize]++] = target;
    }

    return b;
}

