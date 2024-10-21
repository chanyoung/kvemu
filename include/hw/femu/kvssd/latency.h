#ifndef __FEMU_KVSSD_LATENCY_H
#define __FEMU_KVSSD_LAYENCY_H

#include <stdint.h>
#include <stdlib.h>
#include "hw/femu/nand/nand.h"

struct kvssd_latency {
    /* Nand Flash Type: SLC/MLC/TLC/QLC/PLC */
    uint8_t         flash_type;
    int mlc_tbl[MAX_SUPPORTED_PAGES_PER_BLOCK];
    int tlc_tbl[MAX_SUPPORTED_PAGES_PER_BLOCK];
    int qlc_tbl[MAX_SUPPORTED_PAGES_PER_BLOCK];
    NandFlashTiming timing;
};

void kvssd_init_latency(struct kvssd_latency *n, int flash_type);

static inline int kvssd_get_page_read_latency(struct kvssd_latency *nm, int page_type)
{
    return nm->timing.pg_rd_lat[nm->flash_type][page_type];
}

static inline int kvssd_get_page_write_latency(struct kvssd_latency *nm, int page_type)
{
    return nm->timing.pg_wr_lat[nm->flash_type][page_type];
}

static inline int kvssd_get_blk_erase_latency(struct kvssd_latency *nm)
{
    return nm->timing.blk_er_lat[nm->flash_type];
}

static inline uint8_t kvssd_get_page_type(struct kvssd_latency *nm, int pg)
{
    switch (nm->flash_type) {
        case SLC:
            return SLC_PAGE;
        case MLC:
            return nm->mlc_tbl[pg];
        case TLC:
            return nm->tlc_tbl[pg];
        case QLC:
            return nm->qlc_tbl[pg];
        default:
            abort();
    }
}

#endif

