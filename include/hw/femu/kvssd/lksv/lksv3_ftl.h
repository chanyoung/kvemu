#ifndef __FEMU_LKSV3_FTL_H
#define __FEMU_LKSV3_FTL_H

#include <assert.h>
#include <execinfo.h>
#include "hw/femu/kvssd/nand.h"
#include "hw/femu/kvssd/latency.h"
#include "hw/femu/kvssd/settings.h"
#define XXH_INLINE_ALL
#include "hw/femu/kvssd/xxhash.h"
#include "hw/femu/kvssd/kv_types.h"
#include "hw/femu/kvssd/utils.h"
#include "hw/femu/kvssd/skiplist.h"
#include "hw/femu/kvssd/cache.h"
#include "hw/femu/kvssd/lksv/cache.h"
#include "hw/femu/kvssd/lsm.h"

extern struct lksv3_lsmtree *lksv_lsm;

/* DO NOT EDIT */
#define HASH_BYTES 4
#define LEVELLIST_HASH_BYTES 2
#define LEVELLIST_HASH_SHIFTS 16
#define PG_N 32
#define ASYNC_IO_UNIT 64

/*
 * If uncommented, the simulator runs in OURS+ mode, where values are placed
 * back into the log area during the compaction process.
 */
//#define OURS

/*
 * The sg_list is used to determine whether a log line is eligible for early
 * erase. "Gathered" indicates that the key range of the values is densely
 * packed and that the log line was created by a compaction operation.
 * "Scattered" indicates that the key range of the values is dispersed and
 * that the log line was created by host I/O operations.
 */
typedef struct sg_list {
    bool    scattered;
    kv_key  skey;
    kv_key  ekey;
} sg_list;

typedef struct per_line_data {
    bool    referenced_levels[LSM_LEVELN];
    bool    referenced_flush_buffer;
    bool    referenced_flush;
    sg_list sg;
} per_line_data;

#define per_line_data(line) ((per_line_data *)((line)->private))

/*
 * update_sg
 *
 * Updates the scatter/gather state, start key, and end key for per-line data.
 * This information is used in the early reclaim operation of the log line.
 */
static inline void
update_sg(struct line *line, kv_key skey, kv_key ekey, bool scattered)
{
    kv_assert(skey.key != kv_key_min.key);
    kv_assert(ekey.key != kv_key_min.key);

    /*
     * A scattered line cannot transition to a gathered state even if gathered
     * data is added to it. However, if scattered data is added to a gathered
     * line, it will transition to a scattered line.
     */
    if (!per_line_data(line)->sg.scattered) {
        per_line_data(line)->sg.scattered = scattered;
    }

    /* Update the start key of the line */
    if (!per_line_data(line)->sg.skey.key) {
        kv_copy_key(&per_line_data(line)->sg.skey, &skey);
    } else {
        int res = kv_cmp_key(per_line_data(line)->sg.skey, skey);
        if (res < 0) {
            FREE(per_line_data(line)->sg.skey.key);
            kv_copy_key(&per_line_data(line)->sg.skey, &skey);
        }
    }

    /* Update the end key of the line */
    if (!per_line_data(line)->sg.ekey.key) {
        kv_copy_key(&per_line_data(line)->sg.ekey, &ekey);
    } else {
        int res = kv_cmp_key(per_line_data(line)->sg.ekey, ekey);
        if (res > 0) {
            FREE(per_line_data(line)->sg.ekey.key);
            kv_copy_key(&per_line_data(line)->sg.ekey, &ekey);
        }
    }
}

/* Functions found in lksv_ftl.c */
uint64_t lksv3_ssd_advance_status(struct ssd *ssd, struct femu_ppa *ppa, struct nand_cmd *ncmd);

typedef struct {
    uint32_t hash;
} lksv_hash;

typedef struct {
    lksv_hash   *hashes;
    uint16_t    n;
} lksv_hash_list;

/*
 * The LSM-tree maintains an in-memory data structure that points to runs of
 * levels in the flash. Each run contains a header that holds the locations of
 * KV objects (KV indices) in the flash.
 */
typedef struct lksv3_run {
    kv_key key;
    kv_key end;
    struct femu_ppa ppa;

    // not null == cached
    struct kv_cache_entry *cache[CACHE_TYPES];

    lksv_hash_list  hash_lists[PG_N];
    uint16_t        pg_start_hashes[PG_N];
    int             hash_list_n;
    int             n;

    // raw format of meta segment. (page size)
    char *buffer[PG_N];
} lksv3_run;

typedef struct lksv3_run lksv3_run_t;

#define LEVEL_LIST_ENTRY_PER_PAGE (PAGESIZE/(32+(PG_N*LEVELLIST_HASH_BYTES)+20))

#define RUNINPAGE (PAGESIZE/(AVGKEYLENGTH+(LEVELLIST_HASH_BYTES*PG_N)+20))
#define MAXKEYINMETASEG ((PAGESIZE - 1024)/(AVGKEYLENGTH+PPA_LENGTH))
#define KEYFORMAT(input) input.len>AVGKEYLENGTH?AVGKEYLENGTH:input.len,input.key

#define KEYBITMAP (PAGESIZE / 16)
#define PREFIXCHECK 4
#define KEYLEN(a) (a.len)

bool lksv3_should_meta_gc_high(struct ssd *ssd);
bool lksv3_should_data_gc_high(struct ssd *ssd, int margin);

// level.h ================================================

typedef struct keyset {
    struct femu_ppa ppa;
    kv_key lpa;
    int value_len;
    int voff;
    uint32_t hash;
} keyset;

typedef struct lksv3_level lksv3_level;
typedef struct lksv3_level {
    int32_t idx;
    int32_t m_num,n_num,v_num,x_num;
    uint64_t vsize;
    kv_key start,end;
    /*
     * Each level is divided into fixed-size runs.
     * run_impl: array_body
     */
    void* level_data;
    bool reference_lines[512];
} lksv3_level_t;

typedef struct lev_iter{
    int lev_idx;
    kv_key from,to;
    void *iter_data;
} lev_iter;

enum lksv3_sst_meta_flag {
    VMETA = 0,
    VLOG = 1,
};

#define LKSV3_SSTABLE_FOOTER_BLK_SIZE 8
#define LKSV3_SSTABLE_META_BLK_SIZE 16
#define LKSV3_SSTABLE_STR_IDX_SIZE 4
/*
 * --------------------------------------
 * Page Layout
 * --------------------------------------
 * Data blocks: N bytes (less than 2048)
 *  NB: Key + value
 * --------------------------------------
 * Meta blocks: 16 bytes
 *  2B: data block offset
 *  1B: key length
 *  1B: flags
 *  4B: hash key
 *  1B: shard id
 *  1B: total shard numbers
 *  2B: shard length
 *  2B: value log offset
 *  2B: reserved space
 * --------------------------------------
 * Footer block: 16 bytes
 *  2B: number of KV pairs
 *  2B: start key meta index (for range query)
 *  2B: end key meta index (for range query)
 * 10B: reserved space
 * --------------------------------------
 */
typedef struct lksv_block_meta {
    union {
        struct {
            uint64_t off    : 16;
            uint64_t klen   : 8;
            uint64_t flag   : 8;
            uint64_t hash   : 32;
        } g1;
        uint64_t m1;
    };
    union {
        struct {
            uint64_t sid    : 8;
            uint64_t snum   : 8;
            uint64_t slen   : 16;
            uint64_t voff   : 16;
            uint64_t rsv    : 16;
        } g2;
        uint64_t m2;
    };
} lksv_block_meta;

typedef struct lksv_block_footer {
    union {
        struct {
            uint64_t n      : 16;
            uint64_t skey   : 16;
            uint64_t ekey   : 16;
            uint64_t rsv    : 16;
        } g;
        uint64_t f;
    };
} lksv_block_footer;

typedef struct lksv3_sst_str_idx_t {
    union {
        struct {
            uint32_t sst : 8;
            uint32_t off : 24;
        } g1;
        uint32_t i;
    };
} lksv3_sst_str_idx_t;

typedef struct lksv_comp_entry {
    kv_key                  key;
    lksv_block_meta         meta;
    struct femu_ppa         ppa;
    uint32_t                hash_order;
    struct lksv_comp_entry  *next;
} lksv_comp_entry;

typedef struct lksv_comp_list {
    lksv_comp_entry         *str_order_entries;
    lksv_comp_entry         **hash_order_pointers;
    lksv3_sst_str_idx_t     *str_order_map;
    int                     n;
} lksv_comp_list;

typedef struct lksv_comp_lists_iterator {
    lksv_comp_list  **l;
    int             i;
    int             imax;
    int             n;
} lksv_comp_lists_iterator;

/* level operations */
void lksv3_array_range_update(lksv3_level *lev, lksv3_run_t* r, kv_key key);
lksv3_level* lksv3_level_init(int size, int idx);
void lksv3_free_level(struct lksv3_lsmtree *, lksv3_level *);
void lksv3_free_run(struct lksv3_lsmtree*, lksv3_run *);
lksv3_run* lksv3_insert_run(struct ssd *ssd, lksv3_level_t* des, lksv3_run *r);
lksv3_run_t* lksv3_insert_run2(struct ssd *ssd, lksv3_level *lev, lksv3_run_t* r);
void lksv3_copy_level(struct ssd *ssd, lksv3_level_t *des, lksv3_level_t *src);
keyset* lksv3_find_keyset(struct ssd *ssd, NvmeRequest *req, lksv3_run_t *run, kv_key lpa, uint32_t hash, int level);
uint32_t lksv3_range_find_compaction(lksv3_level_t *l, kv_key start, kv_key end, lksv3_run ***r);
lev_iter* lksv3_get_iter(lksv3_level_t*, kv_key from, kv_key to); //from<= x <to
lksv3_run* lksv3_iter_nxt(lev_iter*);
char* lksv3_mem_cvt2table(struct ssd *ssd, kv_skiplist *, lksv3_run *);
char *lksv3_mem_cvt2table2(struct ssd *ssd, lksv_comp_list *mem, lksv3_run_t *input);
void lksv3_make_partition(struct lksv3_lsmtree*, lksv3_level *);

uint8_t lksv3_lsm_scan_run(struct ssd *ssd, kv_key key, lksv3_run_t **entry, lksv3_run_t *up_entry, keyset **found, int *level, NvmeRequest *req);
lksv3_run_t *lksv3_find_run_slow(lksv3_level* lev, kv_key lpa, struct ssd *ssd);
lksv3_run_t *lksv3_find_run_slow_by_ppa(lksv3_level* lev, struct femu_ppa *ppa, struct ssd *ssd);
lksv3_run *lksv3_find_run(lksv3_level_t*, kv_key lpa, struct ssd *ssd, NvmeRequest *req);
lksv3_run *lksv3_find_run_se(struct lksv3_lsmtree*, lksv3_level_t *lev, kv_key lpa, lksv3_run *upper_run, struct ssd *ssd, NvmeRequest *req);
void lksv3_read_run_delay_comp(struct ssd *ssd, lksv3_level *lev);
lksv3_run *lksv3_make_run(kv_key start, kv_key end, struct femu_ppa);
void lksv3_print_level_summary(struct lksv3_lsmtree*);

// page.h ====================================================

int lksv3_gc_meta_femu(struct ssd *ssd);
void lksv3_gc_data_femu3(struct ssd *ssd, int ulevel, int level);
void lksv_gc_data_early(struct ssd *ssd, int ulevel, int level, kv_key k);

// skiplist.h ================================================

typedef struct lksv3_length_bucket {
    kv_snode **bucket[MAXVALUESIZE+1];
    struct lksv3_gc_node **gc_bucket[MAXVALUESIZE+1];
    uint32_t indices[MAXVALUESIZE+1];
    kv_value** contents;
    int contents_num;
} lksv3_l_bucket;

lksv3_l_bucket *lksv3_skiplist_make_length_bucket(kv_skiplist *sl);
kv_skiplist *lksv_skiplist_cutting_header(kv_skiplist *in, bool after_log_write, bool with_value, bool left);

// compaction.h ==============================================

typedef struct compaction_req compR;

struct compaction_req {
    int fromL;
    kv_skiplist *temptable;
    bool last;
    QTAILQ_ENTRY(compaction_req) entry;
};

typedef struct leveling_node{
    kv_skiplist *mem;
    kv_key start;
    kv_key end;
    lksv3_run_t *entry;
} leveling_node;

struct lksv3_lsmtree;

bool lksv3_should_compact(lksv3_level *l);

uint32_t lksv3_level_change(struct ssd *ssd, lksv3_level *from, lksv3_level *to, lksv3_level *target);
uint32_t lksv3_leveling(struct ssd *ssd, lksv3_level *from, lksv3_level *to, leveling_node *l_node);

void lksv3_do_compaction(struct ssd *ssd);
bool lksv3_compaction_init(struct ssd *ssd);
void lksv3_compaction_free(struct lksv3_lsmtree *LSM);
void lksv3_compaction_check(struct ssd *ssd);
uint32_t lksv3_compaction_empty_level(struct ssd *ssd, lksv3_level **from, leveling_node *lnode, lksv3_level **des);

void lksv3_compaction_data_write(struct ssd *ssd, leveling_node* lnode);
struct femu_ppa lksv3_compaction_meta_segment_write_femu(struct ssd *ssd, char *data, int level);
struct femu_ppa lksv3_compaction_meta_segment_write_insert_femu(struct ssd *ssd, lksv3_level *target, lksv3_run_t *entry);

// lksv3_table.h =================================================

enum LksvTableStatusCodes {
    LKSV3_TABLE_OK   = 0,
    LKSV3_TABLE_FULL = 1,
};

typedef struct lksv3_str_val {
    uint32_t len;
    char *val;
} lksv3_str_val;
#define LKSV3_VALT lksv3_str_val

typedef struct {
    kv_key k;
    LKSV3_VALT v;
    struct femu_ppa ppa;
    int voff;
} lksv3_kv_pair_t;

typedef struct {
    lksv_block_footer footer;
    lksv_block_meta *meta;
    lksv3_sst_str_idx_t *str_idx;
    void *raw;
} lksv3_sst_t;

typedef struct {
    int max_keys;

    lksv3_sst_t *high;
    int cursor_high;
    struct femu_ppa high_ppa;

    lksv3_sst_t *low;
    int cursor_low;
    struct femu_ppa low_ppa;

    lksv3_sst_t target;
    int target_write_pointer;
} lksv3_compaction;

lksv3_compaction *new_lksv3_compaction(int max_keys);
void free_lksv3_compaction(lksv3_compaction *c);
void do_lksv3_compaction2(struct ssd *ssd, int high_lev, int low_lev, leveling_node *l_node, lksv3_level *target_level);

int lksv3_sst_encode2(lksv3_sst_t *sst, lksv3_kv_pair_t *kv, uint32_t hash, int *wp, bool sharded);
void lksv3_sst_encode_str_idx(lksv3_sst_t *sst, lksv3_sst_str_idx_t *block, int n);
int lksv3_sst_decode(lksv3_sst_t *sst, void *raw);
struct femu_ppa lksv3_sst_write(struct ssd *ssd, struct femu_ppa ppa, lksv3_sst_t *sst);
void lksv3_sst_read(struct ssd *ssd, struct femu_ppa ppa, lksv3_sst_t *sst);

// array.h ==================================================

typedef struct small_node{
    kv_key start;
    lksv3_run_t *r;
} s_node;

typedef struct prifix_node{
    char pr_key[PREFIXCHECK];
} pr_node;

typedef struct partition_node{
    uint32_t start;
    uint32_t end;
} pt_node;

typedef struct array_body{
    lksv3_run_t *arrs;
    int max_depth;
    pr_node *pr_arrs;
    pt_node *p_nodes;
} array_body;

typedef struct array_iter{
    lksv3_run_t *arrs;
    int max;
    int now;
    bool ispartial;
} a_iter;

// lsmtree.h ================================================

enum READTYPE{
    NOTFOUND,FOUND,CACHING,FLYING,COMP_FOUND
};

typedef struct lksv3_lsmtree {
    struct kv_lsm_options *opts;

    uint8_t bottom_level; // Indicates the current bottom level index.
    uint8_t LEVELCACHING;

    struct kv_skiplist *temptable;    /* Compaction temp data */
    struct kv_skiplist *kmemtable;    /* keyonly memtable for flush */
    struct kv_skiplist *memtable;     /* L0 */
    lksv3_level **disk;                  /* L1 ~ */
    lksv3_level *c_level;

    struct kv_cache *lsm_cache;

    QTAILQ_HEAD(compaction_queue, compaction_req) compaction_queue;

    uint64_t num_data_written;
    uint64_t cache_hit;
    uint64_t cache_miss;
    int header_gc_cnt;
    int data_gc_cnt;

    bool gc_plan[4][512];
    bool flush_reference_lines[512];
    bool flush_buffer_reference_lines[512];
    int should_d2m;
    uint64_t m2d;
    int gc_planned;

    int t_meta;
    int t_data;
    int64_t inv;
    int64_t val;

    bool force;

    // They are calculated based on stats sampled from the bottom level.
    uint32_t avg_value_bytes;
    uint32_t avg_key_bytes;
    uint64_t sum_value_bytes;
    uint64_t sum_key_bytes;
    uint32_t samples_count;
} lksv3_lsmtree;

typedef struct lksv3_lsmtree_setting_parmaters {
    uint8_t LEVELCACHING;

    uint32_t TOTAL_KEYS;
    uint32_t LAST_LEVEL_HEADERNUM;
    uint32_t ALL_LEVEL_HEADERNUM;
    float LEVEL_SIZE_FACTOR;
    uint32_t HEADERNUM;
    float caching_size;

    uint64_t total_memory;
    uint64_t level_list_memory;
    uint64_t pinned_level_list_memory;
    uint64_t pinned_meta_segs_memory;
    uint64_t level_list_cache_memory;
    uint64_t cache_memory;
    uint64_t pin_memory;
    int64_t remain_memory;
} lksv3_lsp;

typedef struct lksv3_lsmtree_levelsize_params {
    float size_factor;
    float last_size_factor;
    bool* size_factor_change;//true: it will be changed size
    //keynum_in_header real;
    uint32_t keynum_in_header;
} lksv3_llp;

void lksv3_lsm_create(struct ssd *ssd);
void lksv3_lsm_setup_params(struct ssd *ssd);
uint8_t lksv3_lsm_find_run(struct ssd *ssd, kv_key key, lksv3_run_t **entry, lksv3_run_t *up_entry, keyset **found, int *level, NvmeRequest *req);

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

    lksv3_lsp     lsp;
    lksv3_llp     llp;

    FemuCtrl *n;
    bool do_reset;
    bool start_log;
    bool start_ramp;
    uint64_t ramp_start_time;

    struct kvssd_latency lat;
    const struct kv_lsm_operations *lops;
};

void lksv3ssd_init(FemuCtrl *n);

struct femu_ppa lksv3_get_new_meta_page(struct ssd *ssd);
struct femu_ppa lksv3_get_new_data_page(struct ssd *ssd);
struct nand_page *lksv3_get_pg(struct ssd *ssd, struct femu_ppa *ppa);
struct line *lksv3_get_line(struct ssd *ssd, struct femu_ppa *ppa);
struct line *lksv3_get_next_free_line(struct line_partition *lm);
void lksv3_ssd_advance_write_pointer(struct ssd *ssd, struct line_partition *lm);
void lksv3_mark_page_invalid(struct ssd *ssd, struct femu_ppa *ppa);
void lksv3_mark_page_valid(struct ssd *ssd, struct femu_ppa *ppa);
void lksv3_mark_page_valid2(struct ssd *ssd, struct femu_ppa *ppa);
void lksv3_mark_block_free(struct ssd *ssd, struct femu_ppa *ppa);
struct line *lksv3_select_victim_meta_line(struct ssd *ssd, bool force);
struct line *lksv3_select_victim_data_line(struct ssd *ssd, bool force);
void lksv3_mark_line_free(struct ssd *ssd, struct femu_ppa *ppa);

struct nand_lun *lksv3_get_lun(struct ssd *ssd, struct femu_ppa *ppa);

static inline struct femu_ppa get_next_write_ppa(struct ssd *ssd, struct femu_ppa pivot, int offset) {
    kv_assert(pivot.g.ch == 0);
    kv_assert(offset < 64);

    int ch_offset = offset % ssd->sp.nchs;
    int lun_offset = (offset / ssd->sp.nchs) % ssd->sp.luns_per_ch;

    pivot.g.ch += ch_offset;
    pivot.g.lun += lun_offset;

    return pivot;
}

static inline bool is_pivot_ppa(struct ssd *ssd, struct femu_ppa ppa) {
    int pg_n = ppa.g.lun * ssd->sp.nchs;
    if (ppa.g.ch == 0
        && pg_n % PG_N == 0) {
        return true;
    }
    return false;
}

struct lksv3_comp_entry_order {
    int str_order;
    int insert_order;
};

struct lksv3_comp_entry {
    struct kv_snode *snode;
    uint32_t hash;
};

static inline void wait_pending_reads(struct ssd *ssd) {
    while (qatomic_read(&ssd->n->pending_reads) > 0) {
        usleep(1);
    }
}

static inline bool should_unify(struct ssd *ssd, struct femu_ppa *log_ppa, int level, int to_level) {
    if (lksv_lsm->gc_plan[level][log_ppa->g.blk]) {
        return true;
    }
    if (to_level >= (LSM_LEVELN - 1)) {
        bool other_ref = false;

        if (!lksv_lsm->flush_buffer_reference_lines[log_ppa->g.blk] && !lksv_lsm->flush_reference_lines[log_ppa->g.blk] && ssd->lm.data.wp.blk != log_ppa->g.blk) {
            for (int i = 0; i < LSM_LEVELN; i++) {
                if (i == to_level || i == level) {
                    continue;
                }
                if (lksv_lsm->disk[i]->reference_lines[log_ppa->g.blk]) {
                    other_ref = true;
                }
            }
            if (!other_ref) {
                lksv_lsm->gc_plan[level][log_ppa->g.blk] = true;
                lksv_lsm->gc_plan[to_level][log_ppa->g.blk] = true;
                lksv_lsm->gc_planned++;
            }
        }
        return true;
    }
    return false;
}

static inline bool is_meta_line(struct ssd *ssd, int lineid)
{
    return ssd->lm.lines[lineid].meta;
}

static inline void check_473(struct ssd *ssd)
{
#ifdef HPCA_DEBUG
    for (int i = 0; i < 4; i++) {
        for (int l = 0; l < 512; l++) {
            if (is_meta_line(ssd, l)) {
                continue;
            }
            if (per_line_data(&ssd->lm.lines[l])->referenced_levels[i]) {
                kv_assert(lksv_lsm->disk[i]->reference_lines[l]);
            } else {
                kv_assert(!lksv_lsm->disk[i]->reference_lines[l]);
            }
        }
    }
    for (int l = 0; l < 512; l++) {
        if (is_meta_line(ssd, l)) {
            continue;
        }
        if (lksv_lsm->flush_reference_lines[l]) {
            kv_assert(per_line_data(&ssd->lm.lines[l])->referenced_flush);
        } else {
            kv_assert(!per_line_data(&ssd->lm.lines[l])->referenced_flush);
        }
    }
#endif
}

struct lksv3_gc {
    int lineid;
    int inv_ratio;
};

void check_kv_pair_count(struct ssd *ssd, struct line *line);

static inline void check_linecnt(struct ssd *ssd)
{
    int cnt = 0;
    if (ssd->lm.meta.wp.curline) {
        cnt++;
    }
    cnt += ssd->lm.meta.free_line_cnt;
    cnt += ssd->lm.meta.full_line_cnt;
    cnt += ssd->lm.meta.victim_line_cnt;
    // ssd->lm.meta.lines - 1: gc flying
    kv_assert(cnt == ssd->lm.meta.lines || cnt == ssd->lm.meta.lines - 1);
}

void gc_erase_delay(struct ssd *ssd, struct femu_ppa *ppa);

static inline void update_lines(struct ssd *ssd)
{
    int pgs = 0;

    for (int i = 0; i < lksv_lsm->bottom_level; i++)
        pgs += lksv_lsm->disk[i]->m_num * 32;
    pgs += lksv_lsm->disk[lksv_lsm->bottom_level]->n_num * 32;

    lksv_lsm->t_meta = (pgs / ssd->sp.pgs_per_line);
    lksv_lsm->t_data = ssd->sp.tt_lines - lksv_lsm->t_meta;
    lksv_lsm->t_data /= 2;
    lksv_lsm->t_data += ((double) lksv_lsm->inv / (double) (lksv_lsm->inv + lksv_lsm->val + 1)) * lksv_lsm->t_data;

    if (lksv_lsm->t_data < ssd->sp.tt_lines * 0.05)
        lksv_lsm->t_data = ssd->sp.tt_lines * 0.05;
    if (lksv_lsm->t_data > ssd->sp.tt_lines * 0.98)
        lksv_lsm->t_data = ssd->sp.tt_lines * 0.98;
    lksv_lsm->t_meta = ssd->sp.tt_lines - lksv_lsm->t_data;

    static uint64_t cnt = 0;
    if (cnt++ % 100 == 0)
        printf("[Update target] meta: %d, data: %d\n", lksv_lsm->t_meta, lksv_lsm->t_data);
}

bool move_line_m2d(struct ssd *ssd, bool force);
bool move_line_d2m(struct ssd *ssd, bool force);

static inline bool check_voffset(struct ssd *ssd, struct femu_ppa *ppa, int voff, uint32_t hash)
{
    struct nand_page *pg = lksv3_get_pg(ssd, ppa);
    int offset = PAGESIZE - LKSV3_SSTABLE_FOOTER_BLK_SIZE - (LKSV3_SSTABLE_META_BLK_SIZE * (voff + 1));
    lksv_block_meta meta = *(lksv_block_meta *) (pg->data + offset);

    return meta.g1.hash == hash;
}

static inline int round_up_pow2(int n)
{
    n -= 1;
    n |= n >> 1;
    n |= n >> 2;
    n |= n >> 4;
    n |= n >> 8;
    n |= n >> 16;
    return n + 1;
}

static inline void bucket_sort(lksv_comp_list *list)
{
#ifdef LK_OH
    uint64_t start = qemu_clock_get_ns(QEMU_CLOCK_REALTIME);
    static uint64_t elapsed;
    static uint64_t stat_count = 0;
#endif

    int bucket_size = round_up_pow2(list->n);
    lksv_comp_entry **bucket = calloc(bucket_size, sizeof(lksv_comp_entry *));
    int pow_n = 0;
    int n = 1;

    while (n < bucket_size) {
        n = n << 1;
        pow_n++;
    }

    //uint32_t mask = UINT32_MAX << (32 - pow_n) >> (32 - pow_n);
    //uint32_t mask = UINT32_MAX >> (32 - pow_n) << (32 - pow_n);
    uint32_t mask = 32 - pow_n;
    int max = list->n;

    lksv_comp_entry *se = list->str_order_entries;

    for (int i = 0; i < max; i++) {
        int idx = (se[i].meta.g1.hash >> mask);
        if (bucket[idx] == NULL) {
            bucket[idx] = &se[i];
            se[i].next = NULL;
        } else {
            lksv_comp_entry **e = &bucket[idx];
            while (true) {
                if (se[i].meta.g1.hash < (*e)->meta.g1.hash) {
                    se[i].next = (*e);
                    (*e) = &se[i];
                    break;
                } else if ((*e)->next == NULL) {
                    (*e)->next = &se[i];
                    se[i].next = NULL;
                    break;
                }
                e = &(*e)->next;
            }
        }
    }

    int k = 0;
    int i = 0;
    while (i < bucket_size) {
        if (bucket[i] == NULL) {
            i++;
            continue;
        }
        lksv_comp_entry *en = bucket[i];
        while (en) {
            list->hash_order_pointers[k] = en;
            list->hash_order_pointers[k]->hash_order = k;
            k++;

            en = en->next;
        }
        i++;
    }

#ifdef LK_OH
    elapsed += qemu_clock_get_ns(QEMU_CLOCK_REALTIME) - start;
    stat_count++;
    if (stat_count % 100 == 0) {
        printf("[LSORT Overhead analysis]\n");
        printf("Average time consumed: %lu\n", elapsed / stat_count);
    }
#endif

    kv_assert(k == list->n);

    FREE(bucket);
}

struct lksv3_hash_sort_t {
    lksv_comp_entry **u[512];
    int un[512];
    int u_start_i;
    int u_start_n;
    int u_end_i;
    int u_end_n;

    lksv_comp_entry **l[512];
    int ln[512];
    int l_start_i;
    int l_start_n;
    int l_end_i;
    int l_end_n;
};

static inline void default_merge(struct lksv3_hash_sort_t *sort, lksv_comp_list *list, lksv_comp_lists_iterator *ui, lksv_comp_lists_iterator *li) {
    int upper_list_n = 0;
    while (sort->u[upper_list_n]) {
        upper_list_n++;
    }

    int lower_list_n = 0;
    while (sort->l[lower_list_n]) {
        lower_list_n++;
    }

    for (int i = 0; i < upper_list_n; i++) {
        if (sort->u[i+1] == NULL) {
            break;
        }
        lksv_comp_entry **tmp = calloc(list->n, sizeof(lksv_comp_entry *));
        int prev = 0;
        int next = 0;
        int merged = 0;
        int merged_target = sort->un[i] + sort->un[i+1];
        while (merged < merged_target) {
            if (sort->un[i] > 0) {
                while (sort->u[i][prev] == NULL) {
                    prev++;
                }
            }
            if (sort->un[i+1] > 0) {
                while (sort->u[i+1][next] == NULL) {
                    next++;
                }
            }
            if (sort->un[i] == 0 || (sort->un[i+1] > 0 && sort->u[i][prev]->meta.g1.hash > sort->u[i+1][next]->meta.g1.hash)) {
                tmp[merged] = sort->u[i+1][next];
                next++;
                sort->un[i+1]--;
            } else {
                tmp[merged] = sort->u[i][prev];
                prev++;
                sort->un[i]--;
            }
            merged++;
        }
        free(sort->u[i+1]);
        sort->u[i+1] = tmp;
        sort->un[i+1] = merged_target;
    }

    for (int i = 0; i < lower_list_n; i++) {
        if (sort->l[i+1] == NULL) {
            break;
        }
        lksv_comp_entry **tmp = calloc(list->n, sizeof(lksv_comp_entry *));
        int prev = 0;
        int next = 0;
        int merged = 0;
        int merged_target = sort->ln[i] + sort->ln[i+1];
        while (merged < merged_target) {
            if (sort->ln[i] > 0) {
                while (sort->l[i][prev] == NULL) {
                    prev++;
                }
            }
            if (sort->ln[i+1] > 0) {
                while (sort->l[i+1][next] == NULL) {
                    next++;
                }
            }
            if (sort->ln[i] == 0 || (sort->ln[i+1] > 0 && sort->l[i][prev]->meta.g1.hash > sort->l[i+1][next]->meta.g1.hash)) {
                tmp[merged] = sort->l[i+1][next];
                next++;
                sort->ln[i+1]--;
            } else {
                tmp[merged] = sort->l[i][prev];
                prev++;
                sort->ln[i]--;
            }
            merged++;
        }
        free(sort->l[i+1]);
        sort->l[i+1] = tmp;
        sort->ln[i+1] = merged_target;
    }

    int prev = 0;
    int next = 0;
    int merged = 0;
    int merged_target = list->n;
    kv_assert(list->n == sort->un[upper_list_n - 1] + sort->ln[lower_list_n - 1]);
    while (merged < merged_target) {
        if (sort->ln[lower_list_n - 1] > 0) {
            while (sort->l[lower_list_n - 1][prev] == NULL) {
                prev++;
            }
        }
        if (sort->un[upper_list_n - 1] > 0) {
            while (sort->u[upper_list_n - 1][next] == NULL) {
                next++;
            }
        }
        if (sort->ln[lower_list_n - 1] == 0 || (sort->un[upper_list_n - 1] > 0 && sort->l[lower_list_n - 1][prev]->meta.g1.hash > sort->u[upper_list_n - 1][next]->meta.g1.hash)) {
            list->hash_order_pointers[merged] = sort->u[upper_list_n - 1][next];
            next++;
            --sort->un[upper_list_n - 1];
        } else {
            list->hash_order_pointers[merged] = sort->l[lower_list_n - 1][prev];
            prev++;
            --sort->ln[lower_list_n - 1];
        }
        merged++;
    }

    int i_end = sort->u_end_i - sort->u_start_i;
    if (sort->u_end_i < ui->imax) {
        i_end++;
    }
    for (int i = 0; i < i_end; i++) {
        free(sort->u[i]);
        sort->u[i] = NULL;
        sort->un[i] = 0;
    }

    i_end = sort->l_end_i - sort->l_start_i;
    if (sort->l_end_i < li->imax) {
        i_end++;
    }
    for (int i = 0; i < i_end; i++) {
        free(sort->l[i]);
        sort->l[i] = NULL;
        sort->ln[i] = 0;
    }
    //memset(sort, 0, sizeof(struct lksv3_hash_sort_t));

    // Keep u_start_i and u_end_i
    //sort->u_start_i = sort->u_end_i = ui->i;
    sort->u_start_i = sort->u_end_i;
    //sort->u_start_n = ui->n;
    sort->u_start_n = 0;
    if (sort->u_start_i < ui->imax)
        sort->u[sort->u_end_i - sort->u_start_i] = calloc((*ui->l)[sort->u_start_i].n, sizeof(lksv_comp_entry*));

    sort->l_start_i = sort->l_end_i;
    //sort->l_start_n = li->n;
    sort->l_start_n = 0;
    if (sort->l_start_i < li->imax)
        sort->l[sort->l_end_i - sort->l_start_i] = calloc((*li->l)[sort->l_start_i].n, sizeof(lksv_comp_entry*));
}

void lksv_open(struct kv_lsm_options *opts);

#endif
