#include "hw/femu/kvssd/lksv/lksv3_ftl.h"

static void comp_write_delay(struct ssd *ssd, struct femu_ppa *ppa)
{
    if (ssd->sp.enable_comp_delay) {
        struct nand_cmd cpw;
        cpw.type = COMP_IO;
        cpw.cmd = NAND_WRITE;
        cpw.stime = 0;
        lksv3_ssd_advance_status(ssd, ppa, &cpw);
    }
}

/*
 * Write data segments to the data partition.
 * This is only called when the skiplist is flushed from L0 to L1.
 */
void lksv3_compaction_data_write(struct ssd *ssd, leveling_node* lnode) {
    kv_skiplist_get_start_end_key(lnode->mem, &lnode->start, &lnode->end);
}

struct femu_ppa lksv3_compaction_meta_segment_write_femu(struct ssd *ssd, char *data, int level) {
    struct femu_ppa fppa;
    struct nand_page *pg;

    fppa = lksv3_get_new_meta_page(ssd);
    pg = lksv3_get_pg(ssd, &fppa);
    comp_write_delay(ssd, &fppa);

    lksv3_mark_page_valid(ssd, &fppa);
    lksv3_ssd_advance_write_pointer(ssd, &ssd->lm.meta);

    FREE(pg->data);
    pg->data = data;

    return fppa;
}

struct femu_ppa lksv3_compaction_meta_segment_write_insert_femu(struct ssd *ssd, lksv3_level *target, lksv3_run_t *entry)
{
    struct femu_ppa fppa;

    kv_assert(entry->hash_list_n > 0);
    kv_assert(entry->hash_list_n <= PG_N);
    for (int i = 0; i < entry->hash_list_n; i++) {
        fppa = lksv3_compaction_meta_segment_write_femu(ssd, (char *) entry->buffer[i], target->idx);
        entry->buffer[i] = NULL;

        if (i == 0) {
            entry->ppa = fppa;
            kv_assert(fppa.g.ch == 0);
        }
    }

    for (int i = entry->hash_list_n; i < PG_N; i++) {
        kv_assert(i > 0);
        fppa = lksv3_get_new_meta_page(ssd);
        lksv3_mark_page_valid(ssd, &fppa);
        lksv3_ssd_advance_write_pointer(ssd, &ssd->lm.meta);
        lksv3_mark_page_invalid(ssd, &fppa);
    }

    lksv3_insert_run(ssd, target, entry);
    lksv3_free_run(lksv_lsm, entry);

    return fppa;
}

