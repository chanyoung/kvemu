#include "hw/femu/kvssd/lksv/lksv3_ftl.h"
#include <math.h>

static float calc_sizefactor(lksv3_lsp *lsp) {
    float f = ceil(pow(10, log10(lsp->LAST_LEVEL_HEADERNUM) / LSM_LEVELN));
    while (pow(f, LSM_LEVELN) > lsp->LAST_LEVEL_HEADERNUM) {
        f -= 0.05f;
    }
    return f;
}

void lksv3_lsm_setup_params(struct ssd *ssd)
{
    lksv3_lsp *lsp = &ssd->lsp;
    lksv3_llp *llp = &ssd->llp;

    uint64_t TOTALSIZE = (unsigned long long) ssd->sp.tt_pgs * PAGESIZE;
    uint64_t SHOWINGSIZE = TOTALSIZE * 90 / 100;  // 10% of SSD capacity is assigned to OP.
    kv_log("SHOWINGSIZE: %lu GB\n", SHOWINGSIZE / 1024 / 1024 / 1024);

    uint64_t showing_pages = ssd->sp.tt_pgs * 90 / 100;
    showing_pages = showing_pages * PG_N / PG_N;

    lsp->LAST_LEVEL_HEADERNUM = showing_pages / PG_N;
    lsp->LEVEL_SIZE_FACTOR = calc_sizefactor(lsp);
    llp->size_factor = llp->last_size_factor = lsp->LEVEL_SIZE_FACTOR;
}

