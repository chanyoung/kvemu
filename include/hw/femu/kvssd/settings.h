#ifndef __FEMU_KVSSD_SETTINGS_H
#define __FEMU_KVSSD_SETTINGS_H

// Comment out the debug flag before running experiments.
//#define FEMU_DEBUG_KV
#include "hw/femu/kvssd/debug.h"

//#define PAGE4
//#define PAGE16

#define HASH_COLLISION_MODELING
//#define TEST_CACHING
//#define GB_96
//#define RANGEQUERY
#define RQ_MAX 150

#include <math.h>
#include <stdio.h>

// KVSSD ---
#define WORKLOAD_KVSSD
// Start with 90% of log lines.
#define LK_LOG_LINES ((512/10)*9)
#define LK_METALINES (512-LK_LOG_LINES)
#define AVGKEYLENGTH 16
#define AVGVALUESIZE 4096
// ---

/*
// YCSB ---
#define WORKLOAD_YCSB
#define PINK_METALINES 40
// Start with 90% of log lines.
#define LK_LOG_LINES ((512/10)*9)
#define LK_METALINES (512-LK_LOG_LINES)
#define AVGKEYLENGTH 20
#define AVGVALUESIZE 1000
// ---
*/

/*
// PinK ---
#define WORKLOAD_PINK
#define PINK_METALINES 40
// Start with 90% of log lines.
#define LK_LOG_LINES ((512/10)*9)
#define LK_METALINES (512-LK_LOG_LINES)
#define AVGKEYLENGTH 32
#define AVGVALUESIZE 1024
// ---
*/

/*
// Xbox ---
#define WORKLOAD_XBOX
#define PINK_METALINES 40
// Start with 90% of log lines.
#define LK_LOG_LINES ((512/10)*9)
#define LK_METALINES (512-LK_LOG_LINES)
#define AVGKEYLENGTH 94
#define AVGVALUESIZE 1200
// ---
*/

/*
// ETC ---
#define WORKLOAD_ETC
#define PINK_METALINES 40
// Start with 90% of log lines.
#define LK_LOG_LINES ((512/10)*9)
#define LK_METALINES (512-LK_LOG_LINES)
#define AVGKEYLENGTH 41
#define AVGVALUESIZE 360
// ---
*/

/*
// UDB ---
#define WORKLOAD_UDB
#define PINK_METALINES 120
// Start with 90% of log lines.
#define LK_LOG_LINES ((512/10)*9)
#define LK_METALINES (512-LK_LOG_LINES)
#define AVGKEYLENGTH 27
// uNVMe requires to be aligned 4x.
//#define AVGVALUESIZE 127
#define AVGVALUESIZE 128
// ---
*/

/*
// Cache ---
#define WORKLOAD_CACHE
#define PINK_METALINES 120
// Start with 90% of log lines.
#define LK_LOG_LINES ((512/10)*9)
#define LK_METALINES (512-LK_LOG_LINES)
#define AVGKEYLENGTH 42
// uNVMe requires to be aligned 4x.
//#define AVGVALUESIZE 188
#define AVGVALUESIZE 188
// ---
*/

/*
// VAR ---
#define WORKLOAD_VAR
#define PINK_METALINES 120
// Start with 90% of log lines.
#define LK_LOG_LINES ((512/10)*9)
#define LK_METALINES (512-LK_LOG_LINES)
#define AVGKEYLENGTH 35
// uNVMe requires to be aligned 4x.
//#define AVGVALUESIZE 115
#define AVGVALUESIZE 116
// ---
*/

/*
// Crypto2 ---
#define WORKLOAD_CRYPTO2
#define PINK_METALINES 120
// Start with 90% of log lines.
#define LK_LOG_LINES ((512/10)*9)
#define LK_METALINES (512-LK_LOG_LINES)
#define AVGKEYLENGTH 37
// uNVMe requires to be aligned 4x.
//#define AVGVALUESIZE 110
#define AVGVALUESIZE 112
// ---
*/

/*
// Dedup ---
#define WORKLOAD_DEDUP
#define PINK_METALINES 120
// Start with 90% of log lines.
#define LK_LOG_LINES ((512/10)*9)
#define LK_METALINES (512-LK_LOG_LINES)
#define AVGKEYLENGTH 20
// uNVMe requires to be aligned 4x.
//#define AVGVALUESIZE 44
#define AVGVALUESIZE 44
// ---
*/

/*
// Cache15 ---
#define WORKLOAD_CACHE15
#define PINK_METALINES 120
// Start with 90% of log lines.
#define LK_LOG_LINES ((512/10)*9)
#define LK_METALINES (512-LK_LOG_LINES)
#define AVGKEYLENGTH 38
// uNVMe requires to be aligned 4x.
//#define AVGVALUESIZE 38
#define AVGVALUESIZE 40
// ---
*/

/*
// ZippyDB ---
#define WORKLOAD_ZIPPYDB
#define PINK_METALINES 220
#define LK_LOG_LINES ((512/10)*9)
#define LK_METALINES (512-LK_LOG_LINES)
#define AVGKEYLENGTH 48
// uNVMe requires to be aligned 2x.
//#define AVGVALUESIZE 43
#define AVGVALUESIZE 44
// ---
*/

/*
// Crypto1 ---
#define WORKLOAD_CRYPTO1
#define PINK_METALINES 120
// Start with 90% of log lines.
#define LK_LOG_LINES ((512/10)*9)
#define LK_METALINES (512-LK_LOG_LINES)
#define AVGKEYLENGTH 76
// uNVMe requires to be aligned 4x.
//#define AVGVALUESIZE 50
#define AVGVALUESIZE 52
// ---
*/

/*
// RTDATA ---
#define WORKLOAD_RTDATA
#define PINK_METALINES 120
// Start with 90% of log lines.
#define LK_LOG_LINES ((512/10)*9)
#define LK_METALINES (512-LK_LOG_LINES)
#define AVGKEYLENGTH 24
// uNVMe requires to be aligned 4x.
//#define AVGVALUESIZE 10
#define AVGVALUESIZE 12
// ---
*/

#define INVALID_PPA     (~(0U))
#define INVALID_LPN     (~(0U))
#define UNMAPPED_PPA    (~(0U))

#define K (1024)
#define M (1024*K)
#define G (1024*M)
#define T (1024L*G)
#define P (1024L*T)
#define MILI (1000000)

#if defined(PAGE4)
#define PAGESIZE (4*K)
#elif defined(PAGE16)
#define PAGESIZE (16*K)
#else
#define PAGESIZE (8*K)
#endif

enum {
    NAND_READ =  0,
    NAND_WRITE = 1,
    NAND_ERASE = 2,
};

enum {
    USER_IO = 0,
    GC_IO = 1,
    COMP_IO = 2,
};

enum {
    SEC_FREE = 0,
    SEC_INVALID = 1,
    SEC_VALID = 2,

    PG_FREE = 0,
    PG_INVALID = 1,
    PG_VALID = 2
};

enum {
    FEMU_ENABLE_GC_DELAY = 1,
    FEMU_DISABLE_GC_DELAY = 2,

    FEMU_ENABLE_DELAY_EMU = 3,
    FEMU_DISABLE_DELAY_EMU = 4,

    FEMU_RESET_ACCT = 5,
    FEMU_ENABLE_LOG = 6,
    FEMU_DISABLE_LOG = 7,
};

#define BLK_BITS    (9)
#define PG_BITS     (9)
#define SEC_BITS    (4)
#define PL_BITS     (1)
#define LUN_BITS    (4)
#define CH_BITS     (4)

#define PPA_LENGTH 4
struct femu_ppa {
    union {
        struct {
            uint32_t blk : BLK_BITS;
            uint32_t pg  : PG_BITS;
            uint32_t sec : SEC_BITS;
            uint32_t pl  : PL_BITS;
            uint32_t lun : LUN_BITS;
            uint32_t ch  : CH_BITS;
            uint32_t rsv : 1;
        } g;

        uint32_t ppa;
    };
};

struct ssdparams {
    int secsz;        /* sector size in bytes */
    int secs_per_pg;  /* # of sectors per page */
    int pgs_per_blk;  /* # of NAND pages per block */
    int blks_per_pl;  /* # of blocks per plane */
    int pls_per_lun;  /* # of planes per LUN (Die) */
    int luns_per_ch;  /* # of LUNs per channel */
    int nchs;         /* # of channels in the SSD */

    double gc_thres_pcent;
    int gc_thres_lines;
    double gc_thres_pcent_high;
    int gc_thres_lines_high;
    bool enable_gc_delay;
    bool enable_comp_delay;

    /* below are all calculated values */
    int secs_per_blk; /* # of sectors per block */
    int secs_per_pl;  /* # of sectors per plane */
    int secs_per_lun; /* # of sectors per LUN */
    int secs_per_ch;  /* # of sectors per channel */
    int tt_secs;      /* # of sectors in the SSD */

    int pgs_per_pl;   /* # of pages per plane */
    int pgs_per_lun;  /* # of pages per LUN (Die) */
    int pgs_per_ch;   /* # of pages per channel */
    int tt_pgs;       /* total # of pages in the SSD */

    int blks_per_lun; /* # of blocks per LUN */
    int blks_per_ch;  /* # of blocks per channel */
    int tt_blks;      /* total # of blocks in the SSD */

    int secs_per_line;
    int pgs_per_line;
    int blks_per_line;
    int tt_lines;
    int meta_lines;
    int meta_gc_thres_lines;
    int meta_gc_thres_lines_high;
    int data_lines;
    int data_gc_thres_lines;
    int data_gc_thres_lines_high;

    int pls_per_ch;   /* # of planes per channel */
    int tt_pls;       /* total # of planes in the SSD */

    int tt_luns;      /* total # of LUNs in the SSD */
};

struct range_lun {
    int read[8][8];
};

static inline void stat_range_lun(struct range_lun *l)
{
    static uint64_t reqs = 0;
    static double stds = 0;
    static double reads = 0;

    double average = 0;

    for (int i = 0; i < 8; i++) {
        for (int j = 0; j < 8; j++) {
            average += l->read[i][j];
        }
    }
    reads += average;
    average /= 64;

    //printf("average: %lf\n", average);

    double distance;
    double distance_sum = 0;
    for (int i = 0; i < 8; i++) {
        for (int j = 0; j < 8; j++) {
             distance = average - l->read[i][j];
             distance_sum += distance * distance;
        }
    }
    distance_sum /= 64;

    double std = sqrt(distance_sum);
    stds += std;

    //printf("std: %lf\n", std);

    reqs++;
    if (reqs < 100000 && reqs % 1000 == 0) {
        printf("Reads per request: %.2lf\n", reads / reqs);
        printf("Std per request: %.2lf\n", stds / reqs);
    } else if (reqs % 10000 == 0) {
        printf("Reads per request: %.2lf\n", reads / reqs);
        printf("Std per request: %.2lf\n", stds / reqs);
    }
}

#define MAXVALUESIZE 4096

#define PREFIX_LENGTH 4
#define RANGE_POINTER_LENGTH 4

#ifdef WORKLOAD_KVSSD
  #define FLUSHMEM (3*M)
#else
  #define FLUSHMEM (1*M)
#endif
#define FLUSHNUM (FLUSHMEM/(AVGKEYLENGTH + AVGVALUESIZE + 20))

#endif

