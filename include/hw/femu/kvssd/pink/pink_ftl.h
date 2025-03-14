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

extern struct pink_ssd     *pink_ssd;
extern struct pink_lsmtree *pink_lsm;

// ftl.h ===================================================

//#define CACHE_UPDATE

struct pink_ssd {
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
    pthread_spinlock_t nand_lock;

    FemuCtrl *n;
    bool do_reset;
    bool start_log;

    struct kvssd_latency lat;

    const struct kv_lsm_operations *lops;
};

uint64_t pink_ssd_advance_status(struct femu_ppa *ppa, struct nand_cmd *ncmd);

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
typedef struct pink_level_list_entry
{
    uint64_t        id;

    kv_key          smallest;
    kv_key          largest;
    struct femu_ppa ppa;

    // not null == cached
    kv_cache_entry  *cache[CACHE_TYPES];

    // raw format of meta segment. (page size)
    char            *buffer;

    int             ref_count;
} pink_level_list_entry;

typedef struct pipe_line_run{
    pink_level_list_entry *r;
}pl_run;

#define LEVEL_LIST_ENTRY_PER_PAGE (PAGESIZE/(AVGKEYLENGTH+PPA_LENGTH))
#define MAXKEYINMETASEG ((PAGESIZE - KEYBITMAP - VERSIONBITMAP)/(AVGKEYLENGTH+PPA_LENGTH))
#define KEYFORMAT(input) input.len>AVGKEYLENGTH?AVGKEYLENGTH:input.len,input.key

#define KEYBITMAP (PAGESIZE / 16)
#define VERSIONBITMAP (PAGESIZE / 16)
#define KEYLEN(a) (a.len+sizeof(struct femu_ppa))

bool pink_should_data_gc_high(void);
bool pink_should_meta_gc_high(void);

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
    pink_level_list_entry **level_data;
} pink_level;

/* level operations */
pink_level* level_init(int idx);
void free_level(struct pink_lsmtree *, pink_level *);
void free_run(struct pink_lsmtree*, pink_level_list_entry *);
pink_level_list_entry* insert_run(pink_level* des, pink_level_list_entry *r);
keyset *find_keyset(char *data, kv_key lpa);
uint32_t range_find_compaction(pink_level *l, kv_key start, kv_key end, pink_level_list_entry ***r);
void merger(kv_skiplist*, pink_level_list_entry** src, pink_level_list_entry** org, pink_level *des);
pink_level_list_entry *cutter(struct pink_lsmtree *, kv_skiplist *, pink_level* des, kv_key* start, kv_key* end);

pink_level_list_entry *find_run(pink_level*, kv_key lpa, NvmeRequest *req);
pink_level_list_entry *find_run2(pink_level*, kv_key lpa, NvmeRequest *req);
pink_level_list_entry *find_run_se(struct pink_lsmtree*, pink_level *lev, kv_key lpa, pink_level_list_entry *upper_run, NvmeRequest *req);
void read_run_delay_comp(pink_level *lev);
pink_level_list_entry *make_run(kv_key start, kv_key end, struct femu_ppa);
int cache_comp_formatting(pink_level *, pink_level_list_entry ***, bool isnext_cache);
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

int gc_meta_femu(void);
int gc_data_femu(void);

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
    pink_level_list_entry *entry;
} leveling_node;

struct pink_lsmtree;

uint32_t level_change(struct pink_lsmtree *LSM, pink_level *from, pink_level *to, pink_level *target);
uint32_t partial_leveling(pink_level* t, pink_level *origin, leveling_node *lnode, pink_level* upper);
uint32_t leveling(pink_level *from, pink_level *to, leveling_node *l_node);

void compaction_subprocessing(struct kv_skiplist *top, struct pink_level_list_entry** src, struct pink_level_list_entry** org, struct pink_level *des);
bool meta_segment_read_preproc(pink_level_list_entry *r);
void meta_segment_read_postproc(pink_level_list_entry *r);

void compaction_data_write(kv_skiplist *skl);
struct femu_ppa compaction_meta_segment_write_femu(char *data);
bool compaction_meta_segment_read_femu(pink_level_list_entry *ent);
void pink_flush_cache_when_evicted(kv_cache_entry *ent);
void pink_maybe_schedule_compaction(void);
void pink_compaction_init(void);

// array.h ==================================================

static inline char *data_from_run(pink_level_list_entry *a){
    return a->buffer;
}

typedef struct array_iter{
    pink_level_list_entry *arrs;
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

    uint8_t LEVELCACHING;

    struct kv_skiplist *mem;
    struct kv_skiplist *imm;
    struct kv_skiplist *key_only_mem;
    struct kv_skiplist *key_only_imm;
    QemuMutex mu;
    QemuThread comp_thread;
    bool compacting;
    double compaction_score;
    int compaction_level;
    uint64_t compaction_calls;

    pink_level **disk;                  /* L1 ~ */
    pink_level *c_level;

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

    GHashTable *level_list_entries;
    pthread_spinlock_t level_list_entries_lock;
    uint64_t next_level_list_entry_id;
} pink_lsmtree;

void pink_lsm_adjust_level_multiplier(void);
void pink_lsm_create(void);
uint8_t lsm_find_run(kv_key key, pink_level_list_entry **entry, keyset **found, int *level, NvmeRequest *req);
uint8_t lsm_scan_run(kv_key key, pink_level_list_entry **entry, keyset **found, int *level, NvmeRequest *req);

// ftl.h =====================================================

void pinkssd_init(FemuCtrl *n);

struct femu_ppa get_new_meta_page(void);
struct femu_ppa get_new_data_page(void);
struct nand_page *get_pg(struct femu_ppa *ppa);
struct line *get_line(struct femu_ppa *ppa);
struct line *get_next_free_line(struct line_partition *lm);
void ssd_advance_write_pointer(struct line_partition *lm);
void mark_sector_invalid(struct femu_ppa *ppa);
void mark_page_invalid(struct femu_ppa *ppa);
void mark_page_valid(struct femu_ppa *ppa);
void mark_block_free(struct femu_ppa *ppa);
struct line *select_victim_meta_line(bool force);
struct line *select_victim_data_line(bool force);
void mark_line_free(struct femu_ppa *ppa);

struct nand_lun *get_lun(struct femu_ppa *ppa);

void pink_adjust_lines(void);
void pink_open(struct kv_lsm_options *opts);

// version.c

void pink_lput(pink_level_list_entry *e);
pink_level_list_entry *pink_lget(uint64_t id);
pink_level_list_entry *pink_lnew(void);
void pink_update_compaction_score(void);

#endif
