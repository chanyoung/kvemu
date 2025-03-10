#include "hw/femu/kvssd/latency.h"

/*
 * Lower/Upper page pairing in one block
 * Shadow page programming sequence to reduce cell-to-cell interference
 */
static void kvssd_init_mlc_page_pairing(struct kvssd_latency *nm)
{
    int i;
    int lowp[] = {0, 1, 2, 3, 4, 5, 7, 8, 502, 503, 506, 507, 509, 510};
    int uppp[] = {6, 9, 504, 505, 508, 511};
    int lpflag = MLC_LOWER_PAGE;

    for (i = 0; i < sizeof(lowp)/sizeof(lowp[0]); i++)
        nm->mlc_tbl[lowp[i]] = MLC_LOWER_PAGE;

    for (i = 0; i < sizeof(uppp)/sizeof(uppp[0]); i++)
        nm->mlc_tbl[uppp[i]] = MLC_UPPER_PAGE;

    for (i = 10; i <= MAX_SUPPORTED_PAGES_PER_BLOCK - 12; i += 2) {
        nm->mlc_tbl[i] = mlc_tbl[i+1] = lpflag;
        lpflag = (lpflag == MLC_LOWER_PAGE) ? MLC_UPPER_PAGE : MLC_LOWER_PAGE;
    }
}

/*
 * Assign cell types in a simpler way: 0 for lower, 1 for center, 2 for upper
 */
static void kvssd_init_tlc_page_pairing(struct kvssd_latency *nm)
{
    int i;

    for (i = 0; i < MAX_SUPPORTED_PAGES_PER_BLOCK; i++) {
        nm->tlc_tbl[i] = i % 3;
    }
}

/* QLC-NAND Flash Mapping with Shadow-Page programming Sequence */
static void kvssd_init_qlc_page_pairing(struct kvssd_latency *nm)
{
    int i, j;
    int rows = (MAX_SUPPORTED_PAGES_PER_BLOCK + 7) / 8;
    int lpflag = QLC_LOWER_PAGE;
    int page_per_row=8;

    int lowp[] = {0, 1, 2, 3, 4, 5, 8, 9};
    int centerlp[] = {6, 7, 10, 11};
    int centerup[] = {12, 13};

    for (i = 0; i < sizeof(lowp)/sizeof(lowp[0]); i++)
        nm->qlc_tbl[lowp[i]] = QLC_LOWER_PAGE;

    for (i = 0; i < sizeof(centerlp)/sizeof(centerlp[0]); i++)
        nm->qlc_tbl[centerlp[i]] = QLC_LOWER_CENTER_PAGE;

    for (i = 0; i < sizeof(centerup)/sizeof(centerup[0]); i++)
        nm->qlc_tbl[centerup[i]] = QLC_UPPER_CENTER_PAGE;

    for (i = 0; i < rows - 3; i++) {
        for (j = 0; j < page_per_row; j += 2) {
            int idx = 8 + (i * page_per_row) + j;
            nm->qlc_tbl[idx] = qlc_tbl[idx+1] = lpflag;
            lpflag = (lpflag == QLC_UPPER_PAGE) ? QLC_LOWER_PAGE : lpflag + 1;
        }
    }
}

static void kvssd_init_nand_flash_timing(struct kvssd_latency *nm)
{
    nm->timing = (NandFlashTiming) {
        /* From low pages to high/upper pages */

        /* Nand Page Read */
        .pg_rd_lat[SLC] = {
            [SLC_PAGE] = SLC_PAGE_READ_LATENCY_NS,
        },
            .pg_rd_lat[MLC] = {
                [MLC_LOWER_PAGE] = MLC_LOWER_PAGE_READ_LATENCY_NS,
                [MLC_UPPER_PAGE] = MLC_UPPER_PAGE_READ_LATENCY_NS
            },
            .pg_rd_lat[TLC] = {
                [TLC_LOWER_PAGE]  = TLC_LOWER_PAGE_READ_LATENCY_NS,
                [TLC_CENTER_PAGE] = TLC_CENTER_PAGE_READ_LATENCY_NS,
                [TLC_UPPER_PAGE]  = TLC_UPPER_PAGE_READ_LATENCY_NS,
            },
            .pg_rd_lat[QLC] = {
                [QLC_LOWER_PAGE]        = QLC_LOWER_PAGE_READ_LATENCY_NS,
                [QLC_LOWER_CENTER_PAGE] = QLC_CENTER_LOWER_PAGE_READ_LATENCY_NS,
                [QLC_UPPER_CENTER_PAGE] = QLC_CENTER_UPPER_PAGE_READ_LATENCY_NS,
                [QLC_UPPER_PAGE]        = QLC_UPPER_PAGE_READ_LATENCY_NS,
            },

            /* Nand Page Write */
            .pg_wr_lat[SLC] = {
                [SLC_PAGE] = SLC_PAGE_WRITE_LATENCY_NS,
            },
            .pg_wr_lat[MLC] = {
                [MLC_LOWER_PAGE] = MLC_LOWER_PAGE_WRITE_LATENCY_NS,
                [MLC_UPPER_PAGE] = MLC_UPPER_PAGE_WRITE_LATENCY_NS
            },
            .pg_wr_lat[TLC] = {
                [TLC_LOWER_PAGE]  = TLC_LOWER_PAGE_WRITE_LATENCY_NS,
                [TLC_CENTER_PAGE] = TLC_CENTER_PAGE_WRITE_LATENCY_NS,
                [TLC_UPPER_PAGE]  = TLC_UPPER_PAGE_WRITE_LATENCY_NS,
            },
            .pg_wr_lat[QLC] = {
                [QLC_LOWER_PAGE]        = QLC_LOWER_PAGE_WRITE_LATENCY_NS,
                [QLC_LOWER_CENTER_PAGE] = QLC_CENTER_LOWER_PAGE_WRITE_LATENCY_NS,
                [QLC_UPPER_CENTER_PAGE] = QLC_CENTER_UPPER_PAGE_WRITE_LATENCY_NS,
                [QLC_UPPER_PAGE]        = QLC_UPPER_PAGE_WRITE_LATENCY_NS,
            },

            /* Nand Block Erase */
            .blk_er_lat = {
                [SLC] = SLC_BLOCK_ERASE_LATENCY_NS,
                [MLC] = MLC_BLOCK_ERASE_LATENCY_NS,
                [TLC] = TLC_BLOCK_ERASE_LATENCY_NS,
                [QLC] = QLC_BLOCK_ERASE_LATENCY_NS,
            },  

            /* Nand Page Channel Transfer */
            .chnl_pg_xfer_lat = {
                [SLC] = SLC_CHNL_PAGE_TRANSFER_LATENCY_NS,
                [MLC] = MLC_CHNL_PAGE_TRANSFER_LATENCY_NS,
                [TLC] = TLC_CHNL_PAGE_TRANSFER_LATENCY_NS,
                [QLC] = QLC_CHNL_PAGE_TRANSFER_LATENCY_NS,
            },  
    };  
}   

void kvssd_init_latency(struct kvssd_latency *n, int flash_type)
{
    kvssd_init_mlc_page_pairing(n);
    kvssd_init_tlc_page_pairing(n);
    kvssd_init_qlc_page_pairing(n);
    kvssd_init_nand_flash_timing(n);
    n->flash_type = flash_type;
}

