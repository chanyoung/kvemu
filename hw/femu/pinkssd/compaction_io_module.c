#include "hw/femu/kvssd/pink/pink_ftl.h"
#include "hw/femu/kvssd/pink/skiplist.h"

static void comp_read_delay(struct femu_ppa *ppa)
{
    if (pink_ssd->sp.enable_comp_delay) {
        struct nand_cmd cpr;
        cpr.type = COMP_IO;
        cpr.cmd = NAND_READ;
        cpr.stime = 0;
        pink_ssd_advance_status(ppa, &cpr);
    }
}

static void comp_write_delay(struct femu_ppa *ppa)
{
    if (pink_ssd->sp.enable_comp_delay) {
        struct nand_cmd cpw;
        cpw.type = COMP_IO;
        cpw.cmd = NAND_WRITE;
        cpw.stime = 0;
        pink_ssd_advance_status(ppa, &cpw);
    }
}

static void log_cvt2table(void **data, kv_snode **targets, int n)
{
    kv_assert(!(*data));
    *data = (char *)calloc(1, PAGESIZE);
    char *ptr = *data;
    uint16_t *offset_map = (uint16_t *)ptr;
    uint16_t *keylen_map  = (uint16_t *)(ptr + ((n+2) * sizeof(uint16_t)));
    uint16_t data_start = 2 * (n+2) * sizeof(uint16_t);

    offset_map[0] = n;
    keylen_map[0] = n;

    int i;
    for (i = 0; i < n; i++) {
        memcpy(&ptr[data_start], targets[i]->key.key, targets[i]->key.len);
        memcpy(&ptr[data_start+targets[i]->key.len], targets[i]->value->value, targets[i]->value->length);
        offset_map[i+1] = data_start;
        keylen_map[i+1] = targets[i]->key.len;
        data_start += keylen_map[i+1] + targets[i]->value->length;

        // To prevent double free: key is reused in key_only_mem.
        targets[i]->key.key = NULL;
    }
    offset_map[n+1] = data_start;
    keylen_map[n+1] = -1;
}

/*
 * Write data segments to the data partition.
 * This is only called when the skiplist is flushed from L0 to L1.
 */
void compaction_data_write(kv_skiplist *skl) {
    pink_l_bucket *lb = pink_skiplist_make_length_bucket(skl);
    int max_vsize = MAXVALUESIZE;
    int min_vsize = 0;

    int n_vals = 0;
    for (int vsize = 0; vsize <= MAXVALUESIZE; vsize++) {
        if (lb->indices[vsize] > 0) {
            if (n_vals == 0) {
                max_vsize = vsize;
                min_vsize = vsize;
            } else {
                max_vsize = MAXVALUESIZE;
                min_vsize = 0;
            }
        }
        n_vals += lb->indices[vsize];
    }

    kv_snode *target;
    struct femu_ppa ppa;
    struct nand_page *pg = NULL;
    int in_page_idx;
    int written_data;
    kv_snode *targets[256];
    while (n_vals > 0) {
        /* Get the new page from the write pointer of data segment partition manager */
        ppa = get_new_data_page();
        pg = get_pg(&ppa);
        comp_write_delay(&ppa);
        in_page_idx = 0;
        written_data = 0;

        int n_value = 0;
        memset(targets, 0, 256 * sizeof(kv_snode *));
        for (int vsize = max_vsize; vsize >= min_vsize; vsize--) {
            int sidx = lb->indices[vsize] - 1;
            if (sidx < 0)
                continue;
            while (lb->indices[vsize] > 0) {
                /*
                 * This piece isn't fit the left space of allocated page.
                 * Go to the next piece to find a fit one.
                 */
                int to_write = vsize + lb->bucket[vsize][lb->indices[vsize]-1]->key.len + 8;
                if (written_data + to_write > PAGESIZE) {
                    //kv_debug("written kv pairs in a page: %d\n", pg->nheaders);
                    break;
                }
                written_data += to_write;

                int i = --lb->indices[vsize];
                target = lb->bucket[vsize][i];

                kv_snode *t2;
                kv_value *v2 = calloc(1, sizeof(kv_value));
                v2->length = PPA_LENGTH;
                t2 = kv_skiplist_insert(pink_lsm->key_only_mem, target->key, v2);
                if (t2->private == NULL)
                    t2->private = calloc(1, sizeof(pink_per_snode_data));
                *snode_ppa(t2) = ppa;
                *snode_off(t2) = in_page_idx;
                targets[in_page_idx] = target;
                in_page_idx++;
                n_vals--;
                n_value++;
            }
        }
        log_cvt2table(&pg->data, targets, n_value);
    }

    for(int vsize = 0; vsize <= MAXVALUESIZE; vsize++) {
        if(lb->bucket[vsize])
            FREE(lb->bucket[vsize]);
    }
    FREE(lb);
}

struct femu_ppa compaction_meta_segment_write_femu(char *data) {
    struct femu_ppa fppa;
    struct nand_page *pg;

    fppa = get_new_meta_page();
    pg = get_pg(&fppa);
    comp_write_delay(&fppa);

    kv_assert(pg->data == NULL);
    kv_assert(data != NULL);
    pg->data = data;

    return fppa;
}

// Return true if read from flash.
bool compaction_meta_segment_read_femu(pink_level_list_entry *ent) {
    struct nand_page *pg;
    bool cached = kv_is_cached(pink_lsm->lsm_cache, ent->cache[META_SEGMENT]);

    if (cached) {
        kv_cache_delete_entry(pink_lsm->lsm_cache, ent->cache[META_SEGMENT]);
        if (ent->buffer) {
            kv_assert(ent->ppa.ppa == UNMAPPED_PPA);
            goto entry_was_cached;
        }
    } else {
        comp_read_delay(&ent->ppa);
    }

    // Not cached or cached but backed by flash.
    kv_assert(ent->ppa.ppa != UNMAPPED_PPA);
    pg = get_pg(&ent->ppa);
    kv_assert(pg->data != NULL);
    ent->buffer = pg->data;

entry_was_cached:
    return !cached;
}

void pink_flush_cache_when_evicted(kv_cache_entry *ent)
{
    struct pink_level_list_entry *r = container_of(ent->entry, struct pink_level_list_entry, cache[META_SEGMENT]);

    if (r->ppa.ppa != UNMAPPED_PPA) {
        kv_assert(r->buffer == NULL);
        return;
    }

    r->ppa = compaction_meta_segment_write_femu((char *) r->buffer);
    r->buffer = NULL;
}
