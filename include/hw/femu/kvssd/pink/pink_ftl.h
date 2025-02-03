#ifndef __FEMU_PINK_FTL_H
#define __FEMU_PINK_FTL_H

#include <assert.h>
#include "hw/femu/kvssd/nand.h"
#include "hw/femu/kvssd/latency.h"
#include "hw/femu/kvssd/settings.h"
#include "hw/femu/kvssd/kv_types.h"
#include "hw/femu/kvssd/utils.h"
#include "hw/femu/kvssd/skiplist.h"
#include "hw/femu/kvssd/cache.h"
#include "hw/femu/kvssd/pink/cache.h"
#include "hw/femu/kvssd/lsm.h"
#include "hw/femu/kvssd/compaction_info.h"

// ftl.h ===================================================

#define ASYNC_IO_UNIT 64
//#define CACHE_UPDATE

uint64_t pink_ssd_advance_status(struct ssd *ssd, struct femu_ppa *ppa, struct nand_cmd *ncmd);

// kvssd.h ================================================
struct pink_lsmtree;

#define LINE_AGE_MAX 64

struct line_age {
    union {
        struct {
            uint16_t in_page_idx : 10;
            uint16_t line_age : 6;
        } g;
        uint16_t age;
    };
};

typedef struct pink_key_age {
    kv_key k;
    struct line_age line_age;
} pink_key_age;
#define PINK_KEYAGET pink_key_age

/*
 * The LSM-tree maintains an in-memory data structure that points to runs of
 * levels in the flash. Each run contains a header that holds the locations of
 * KV objects (KV indices) in the flash.
 */
typedef struct pink_run {
    kv_key key;
    kv_key end;
    struct femu_ppa ppa;

    // not null == cached
    kv_cache_entry *cache[CACHE_TYPES];

    // raw format of meta segment. (page size)
    char *buffer;
} pink_run;

typedef struct pink_run pink_run_t;

typedef struct pipe_line_run{
    pink_run *r;
}pl_run;

#define BULK_FLUSH_MARGIN 5

#define LEVEL_LIST_ENTRY_PER_PAGE (PAGESIZE/(AVGKEYLENGTH+PPA_LENGTH))
#define MAXKEYINMETASEG ((PAGESIZE - KEYBITMAP - VERSIONBITMAP)/(AVGKEYLENGTH+PPA_LENGTH))
#define KEYFORMAT(input) input.len>AVGKEYLENGTH?AVGKEYLENGTH:input.len,input.key

#define KEYBITMAP (PAGESIZE / 16)
#define VERSIONBITMAP (PAGESIZE / 16)
#define PREFIXCHECK 4
#define KEYLEN(a) (a.len+sizeof(struct femu_ppa))

bool pink_should_data_gc_high(struct ssd *ssd);
bool pink_should_meta_gc_high(struct ssd *ssd);

// level.h ================================================

typedef struct keyset {
    struct femu_ppa ppa;
    PINK_KEYAGET lpa;
    kv_value value;
} keyset;

typedef struct pink_level {
    int32_t idx;
    int32_t m_num,n_num;
    kv_key start,end;
    /*
     * Each level is divided into fixed-size runs.
     * run_impl: array_body
     */
    void* level_data;
} pink_level;

typedef struct lev_iter{
    int lev_idx;
    kv_key from,to;
    void *iter_data;
} lev_iter;

/* level operations */
pink_level* level_init(int idx);
void free_level(struct pink_lsmtree *, pink_level *);
void free_run(struct pink_lsmtree*, pink_run *);
pink_run* insert_run(struct ssd *ssd, pink_level* des, pink_run *r);
void copy_level(struct ssd *ssd, pink_level *des, pink_level *src);
keyset *find_keyset(char *data, kv_key lpa);
uint32_t range_find_compaction(pink_level *l, kv_key start, kv_key end, pink_run ***r);
lev_iter* get_iter(pink_level*, kv_key from, kv_key to); //from<= x <to
pink_run* iter_nxt(lev_iter*);
char* mem_cvt2table(struct ssd *ssd, kv_skiplist *, pink_run *);
void merger(struct ssd *ssd, kv_skiplist*, pink_run** src, pink_run** org, pink_level *des);
pink_run *cutter(struct pink_lsmtree *, kv_skiplist *, pink_level* des, kv_key* start, kv_key* end);
void make_partition(struct pink_lsmtree*, pink_level *);

pink_run *find_run(pink_level*, kv_key lpa, struct ssd *ssd, NvmeRequest *req);
pink_run *find_run2(pink_level*, kv_key lpa, struct ssd *ssd, NvmeRequest *req);
pink_run *find_run_se(struct pink_lsmtree*, pink_level *lev, kv_key lpa, pink_run *upper_run, struct ssd *ssd, NvmeRequest *req);
void read_run_delay_comp(struct ssd *ssd, pink_level *lev);
pink_run *make_run(kv_key start, kv_key end, struct femu_ppa);
int cache_comp_formatting(pink_level *, pink_run ***, bool isnext_cache);
void print_level_summary(struct pink_lsmtree*);

// page.h ====================================================

typedef struct pink_gc_node {
    uint32_t ppa;
    uint32_t nppa;
    struct femu_ppa fppa;
    struct femu_ppa new_fppa;
    kv_key lpa;
    char *value;
    bool invalidate;
    uint8_t status;
    uint32_t plength;
    void *params;
} pink_gc_node;

int gc_meta_femu(struct ssd *ssd);
int gc_data_femu(struct ssd *ssd);

// compaction.h ==============================================

typedef struct compaction_req compR;

struct compaction_req {
    int fromL;
    kv_skiplist *temptable;
    QTAILQ_ENTRY(compaction_req) entry;
};

typedef struct leveling_node{
    kv_skiplist *mem;
    kv_key start;
    kv_key end;
    pink_run_t *entry;
} leveling_node;

struct pink_lsmtree;

bool should_compact(pink_level *l);

uint32_t level_change(struct pink_lsmtree *LSM, pink_level *from, pink_level *to, pink_level *target);
uint32_t partial_leveling(struct ssd *ssd, pink_level* t, pink_level *origin, leveling_node *lnode, pink_level* upper);
uint32_t leveling(struct ssd *ssd, pink_level *from, pink_level *to, leveling_node *l_node);

bool compaction_init(struct ssd *ssd);
void compaction_free(struct pink_lsmtree *LSM);
void compaction_check(struct ssd *ssd);
void pink_do_compaction(struct ssd *ssd);
void compaction_subprocessing(struct ssd *ssd, struct kv_skiplist *top, struct pink_run** src, struct pink_run** org, struct pink_level *des);
bool meta_segment_read_preproc(pink_run_t *r);
void meta_segment_read_postproc(struct ssd *ssd, pink_run_t *r);
uint32_t compaction_empty_level(struct ssd *ssd, pink_level **from, leveling_node *lnode, pink_level **des);

void compaction_data_write(struct ssd *ssd, leveling_node* lnode);
struct femu_ppa compaction_meta_segment_write_femu(struct ssd *ssd, char *data);
bool compaction_meta_segment_read_femu(struct ssd *ssd, pink_run_t *ent);
void pink_flush_cache_when_evicted(kv_cache_entry *ent);

// array.h ==================================================

static inline char *data_from_run(pink_run_t *a){
    return a->buffer;
}

typedef struct prifix_node{
    char pr_key[PREFIXCHECK];
} pr_node;

typedef struct partition_node{
    uint32_t start;
    uint32_t end;
} pt_node;

typedef struct array_body{
    pink_run_t *arrs;
    int max_depth;
    pr_node *pr_arrs;
    pt_node *p_nodes;
} array_body;

typedef struct array_iter{
    pink_run_t *arrs;
    int max;
    int now;
    bool ispartial;
} a_iter;

// pipe.h ===================================================

typedef struct pipe_body{
    uint32_t max_page;
    uint32_t pidx;
    char **data_ptr;
    pl_run *pl_datas;

    char *now_page;
    uint16_t *bitmap_ptr;
    uint16_t *vbitmap_ptr;
    uint32_t length;
    uint32_t max_key;
    uint32_t kidx;
    bool read_from_run;
}p_body;

// lsmtree.h ================================================

enum READTYPE{
    NOTFOUND,FOUND,CACHING,FLYING,COMP_FOUND
};

typedef struct pink_lsmtree {
    struct kv_lsm_options *opts;

    struct ssd *ssd;
    uint8_t LEVELCACHING;

    struct kv_skiplist *temptable[64];    /* Compaction temp data */
    int temp_n;
    struct kv_skiplist *memtable;     /* L0 */
    pink_level **disk;                  /* L1 ~ */
    pink_level *c_level;
    kv_compaction_info comp_ctx;

    struct kv_cache* lsm_cache;

    struct kv_skiplist *gc_list;

    QTAILQ_HEAD(compaction_queue, compaction_req) compaction_queue;

    /* for pipe merger cutter */
    p_body *rp;
    char **r_data;
    bool cutter_start;

    uint64_t num_data_written;
    uint64_t cache_hit;
    uint64_t cache_miss;
    int header_gc_cnt;
    int data_gc_cnt;
} pink_lsmtree;

void pink_lsm_adjust_level_multiplier(void);
void pink_lsm_create(struct ssd *ssd);
uint8_t lsm_find_run(struct ssd *ssd, kv_key key, pink_run_t **entry, pink_run_t *up_entry, keyset **found, int *level, NvmeRequest *req);
uint8_t lsm_scan_run(struct ssd *ssd, kv_key key, pink_run_t **entry, pink_run_t *up_entry, keyset **found, int *level, NvmeRequest *req);

// ftl.h =====================================================

struct ssd {
    char *ssdname;
    struct ssdparams sp;
    struct ssd_channel *ch;
    struct write_pointer wp;
    struct line_mgmt lm;

    /* lockless ring for communication with NVMe IO thread */
    struct rte_ring **to_ftl;
    struct rte_ring **to_poller;
    bool *dataplane_started_ptr;
    QemuThread ftl_thread;
    QemuThread comp_thread;
    QemuMutex comp_mu;
    QemuMutex comp_q_mu;
    QemuMutex memtable_mu;
    QemuMutex lat_mu;
    pthread_spinlock_t nand_lock;

    FemuCtrl *n;
    bool do_reset;
    bool start_log;

    struct kvssd_latency lat;

    bool need_flush;
    const struct kv_lsm_operations *lops;
};

void pinkssd_init(FemuCtrl *n);

struct femu_ppa get_new_meta_page(struct ssd *ssd);
struct femu_ppa get_new_data_page(struct ssd *ssd);
struct nand_page *get_pg(struct ssd *ssd, struct femu_ppa *ppa);
struct line *get_line(struct ssd *ssd, struct femu_ppa *ppa);
struct line *get_next_free_line(struct line_partition *lm);
void ssd_advance_write_pointer(struct ssd *ssd, struct line_partition *lm);
void mark_sector_invalid(struct ssd *ssd, struct femu_ppa *ppa);
void mark_page_invalid(struct ssd *ssd, struct femu_ppa *ppa);
void mark_page_valid(struct ssd *ssd, struct femu_ppa *ppa);
void mark_block_free(struct ssd *ssd, struct femu_ppa *ppa);
struct line *select_victim_meta_line(struct ssd *ssd, bool force);
struct line *select_victim_data_line(struct ssd *ssd, bool force);
void mark_line_free(struct ssd *ssd, struct femu_ppa *ppa);

struct nand_lun *get_lun(struct ssd *ssd, struct femu_ppa *ppa);

static inline void wait_pending_reads(struct ssd *ssd) {
    while (qatomic_read(&ssd->n->pending_reads) > 0) {
        usleep(1);
    }
}

void pink_adjust_lines(struct ssd *ssd);
void pink_open(struct kv_lsm_options *opts);

extern pink_lsmtree *pink_lsm;

#endif
