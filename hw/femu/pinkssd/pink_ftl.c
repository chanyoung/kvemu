#include <pthread.h>
#include "hw/femu/kvssd/pink/pink_ftl.h"
#include "hw/femu/kvssd/pink/skiplist.h"

static void *ftl_thread(void *arg);

bool pink_should_meta_gc_high(void)
{
    return pink_ssd->lm.meta.free_line_cnt == 0;
}

bool pink_should_data_gc_high(void)
{
    return pink_ssd->lm.data.free_line_cnt == 0;
}

static inline int victim_line_cmp_vsc(pqueue_pri_t next, pqueue_pri_t curr)
{
    return (next > curr);
}

static inline pqueue_pri_t victim_line_get_vsc(void *a)
{
    return ((struct line *)a)->vsc;
}

static inline void victim_line_set_vsc(void *a, pqueue_pri_t pri)
{
    ((struct line *)a)->vsc = pri;
}

static inline int victim_line_cmp_pri(pqueue_pri_t next, pqueue_pri_t curr)
{
    return (next > curr);
}

static inline pqueue_pri_t victim_line_get_pri(void *a)
{
    return ((struct line *)a)->vpc;
}

static inline void victim_line_set_pri(void *a, pqueue_pri_t pri)
{
    ((struct line *)a)->vpc = pri;
}

static inline size_t victim_line_get_pos(void *a)
{
    return ((struct line *)a)->pos;
}

static inline void victim_line_set_pos(void *a, size_t pos)
{
    ((struct line *)a)->pos = pos;
}

static void _ssd_init_write_pointer(struct line_partition *lm)
{
    struct write_pointer *wpp = &lm->wp;
    struct line *curline = NULL;

    curline = QTAILQ_FIRST(&lm->free_line_list);
    QTAILQ_REMOVE(&lm->free_line_list, curline, entry);
    lm->free_line_cnt--;

    // wpp->curline is always our next-to-write super-block 
    wpp->curline = curline;
    wpp->ch = 0;
    wpp->lun = 0;
    wpp->pg = 0;
    wpp->blk = wpp->curline->id;
    wpp->pl = 0;
}

static void ssd_init_write_pointer(void)
{
    _ssd_init_write_pointer(&pink_ssd->lm.meta);
    _ssd_init_write_pointer(&pink_ssd->lm.data);
}

static void ssd_init_lines(void)
{
    struct ssdparams *spp = &pink_ssd->sp;
    struct line_mgmt *lm = &pink_ssd->lm;
    struct line *line;

    lm->tt_lines = spp->blks_per_pl;
    kv_assert(lm->tt_lines == spp->tt_lines);

    lm->meta.lines = spp->meta_lines;
    lm->data.lines = spp->data_lines;
    kv_assert(lm->meta.lines + lm->data.lines <= lm->tt_lines);

    lm->lines = (struct line*) malloc(sizeof(struct line) * lm->tt_lines);

    QTAILQ_INIT(&lm->meta.free_line_list);
    lm->meta.victim_line_pq = pqueue_init(spp->meta_lines, victim_line_cmp_pri, victim_line_get_pri, victim_line_set_pri, victim_line_get_pos, victim_line_set_pos);
    QTAILQ_INIT(&lm->meta.full_line_list);

    lm->meta.free_line_cnt = 0;
    for (int i = 0; i < lm->meta.lines; i++) {
        line = &lm->lines[i];
        line->id = i;
        line->ipc = 0;
        line->vpc = 0;
        line->isc = 0;
        line->vsc = 0;
        line->pos = 0;
        line->meta = true;
        /* initialize all the lines as free lines */
        QTAILQ_INSERT_TAIL(&lm->meta.free_line_list, line, entry);
        lm->meta.free_line_cnt++;
    }

    kv_assert(lm->meta.free_line_cnt == lm->meta.lines);
    lm->meta.victim_line_cnt = 0;
    lm->meta.full_line_cnt = 0;
    lm->meta.age = 0;

    QTAILQ_INIT(&lm->data.free_line_list);
    lm->data.victim_line_pq = pqueue_init(spp->data_lines, victim_line_cmp_vsc, victim_line_get_vsc, victim_line_set_vsc, victim_line_get_pos, victim_line_set_pos);
    QTAILQ_INIT(&lm->data.full_line_list);

    lm->data.free_line_cnt = 0;
    for (int i = lm->meta.lines; i < lm->meta.lines + lm->data.lines; i++) {
        line = &lm->lines[i];
        line->id = i;
        line->ipc = 0;
        line->vpc = 0;
        line->isc = 0;
        line->vsc = 0;
        line->pos = 0;
        /* initialize all the lines as free lines */
        QTAILQ_INSERT_TAIL(&lm->data.free_line_list, line, entry);
        lm->data.free_line_cnt++;
    }

    kv_assert(lm->data.free_line_cnt == lm->data.lines);
    lm->data.victim_line_cnt = 0;
    lm->data.full_line_cnt = 0;
    lm->data.age = 0;
}

static inline void check_addr(int a, int max)
{
    kv_assert(a >= 0 && a < max);
}

struct line *get_next_free_line(struct line_partition *lm)
{
    struct line *curline = NULL;

    curline = QTAILQ_FIRST(&lm->free_line_list);
    if (!curline) {
        printf("No free lines left!!!!\n");
        return NULL;
    }

    QTAILQ_REMOVE(&lm->free_line_list, curline, entry);
    lm->free_line_cnt--;
    curline->age = lm->age++;
    return curline;
}

void ssd_advance_write_pointer(struct line_partition *lm)
{
    struct ssdparams *spp = &pink_ssd->sp;
    struct write_pointer *wpp = &lm->wp;

    check_addr(wpp->ch, spp->nchs);
    wpp->ch++;
    if (wpp->ch == spp->nchs) {
        wpp->ch = 0;
        check_addr(wpp->lun, spp->luns_per_ch);
        wpp->lun++;
        /* in this case, we should go to next lun */
        if (wpp->lun == spp->luns_per_ch) {
            wpp->lun = 0;
            /* go to next page in the block */
            check_addr(wpp->pg, spp->pgs_per_blk);
            wpp->pg++;
            if (wpp->pg == spp->pgs_per_blk) {
                wpp->pg = 0;
                /* move current line to {victim,full} line list */
                if (wpp->curline->vpc == spp->pgs_per_line && wpp->curline->vsc == (spp->secs_per_line * 32)) {
                    /* all pgs are still valid, move to full line list */
                    kv_assert(wpp->curline->ipc == 0);
                    kv_assert(wpp->curline->isc == 0);
                    QTAILQ_INSERT_TAIL(&lm->full_line_list, wpp->curline, entry);
                    lm->full_line_cnt++;
                } else {
                    if (lm == &pink_ssd->lm.data) {
                        kv_assert(wpp->curline->vpc == spp->pgs_per_line);
                        kv_assert(wpp->curline->vsc >= 0 && wpp->curline->vsc < (spp->secs_per_line * 32));
                    } else {
                        kv_assert(wpp->curline->vpc >= 0 && wpp->curline->vpc < spp->pgs_per_line);
                    }
                    /* there must be some invalid pages in this line */
                    kv_assert(wpp->curline->ipc > 0 || wpp->curline->isc > 0);
                    pqueue_insert(lm->victim_line_pq, wpp->curline);
                    lm->victim_line_cnt++;
                }
                /* current line is used up, pick another empty line */
                check_addr(wpp->blk, spp->blks_per_pl);
                wpp->curline = NULL;
                wpp->curline = get_next_free_line(lm);
                if (!wpp->curline) {
                    /* TODO */
                    abort();
                }
                wpp->blk = wpp->curline->id;
                check_addr(wpp->blk, spp->blks_per_pl);
                /* make sure we are starting from page 0 in the super block */
                kv_assert(wpp->pg == 0);
                kv_assert(wpp->lun == 0);
                kv_assert(wpp->ch == 0);
                /* TODO: assume # of pl_per_lun is 1, fix later */
                kv_assert(wpp->pl == 0);
            }
        }
    }
}

static struct femu_ppa get_new_page(struct write_pointer *wpp)
{
    struct femu_ppa ppa;
    ppa.ppa = 0;
    ppa.g.ch = wpp->ch;
    ppa.g.lun = wpp->lun;
    ppa.g.pg = wpp->pg;
    ppa.g.blk = wpp->blk;
    ppa.g.pl = wpp->pl;
    kv_assert(ppa.g.pl == 0);

    return ppa;
}

struct femu_ppa get_new_meta_page(void)
{
    pthread_spin_lock(&pink_ssd->nand_lock);
    struct femu_ppa ppa = get_new_page(&pink_ssd->lm.meta.wp);
    mark_page_valid(&ppa);
    ssd_advance_write_pointer(&pink_ssd->lm.meta);
    pthread_spin_unlock(&pink_ssd->nand_lock);
    return ppa;
}

struct femu_ppa get_new_data_page(void)
{
    pthread_spin_lock(&pink_ssd->nand_lock);
    struct femu_ppa ppa = get_new_page(&pink_ssd->lm.data.wp);
    mark_page_valid(&ppa);
    ssd_advance_write_pointer(&pink_ssd->lm.data);
    pthread_spin_unlock(&pink_ssd->nand_lock);
    return ppa;
}

static void ssd_init_params(struct ssdparams *spp)
{
#ifdef GB_96
    /* 128 GB address */
    spp->secsz = 512;
    spp->secs_per_pg = 16;
    spp->pgs_per_blk = 384;
    spp->blks_per_pl = 512;
    spp->pls_per_lun = 1;
    spp->luns_per_ch = 8;
    spp->nchs = 8;
#else
    /* 64 GB address */
    spp->secsz = 512;
    spp->secs_per_pg = 16;
    spp->pgs_per_blk = 256;
    spp->blks_per_pl = 512;
    spp->pls_per_lun = 1;
    spp->luns_per_ch = 8;
    spp->nchs = 8;
#endif

    /* calculated values */
    spp->secs_per_blk = spp->secs_per_pg * spp->pgs_per_blk;
    spp->secs_per_pl = spp->secs_per_blk * spp->blks_per_pl;
    spp->secs_per_lun = spp->secs_per_pl * spp->pls_per_lun;
    spp->secs_per_ch = spp->secs_per_lun * spp->luns_per_ch;
    spp->tt_secs = spp->secs_per_ch * spp->nchs;

    spp->pgs_per_pl = spp->pgs_per_blk * spp->blks_per_pl;
    spp->pgs_per_lun = spp->pgs_per_pl * spp->pls_per_lun;
    spp->pgs_per_ch = spp->pgs_per_lun * spp->luns_per_ch;
    spp->tt_pgs = spp->pgs_per_ch * spp->nchs;

    spp->blks_per_lun = spp->blks_per_pl * spp->pls_per_lun;
    spp->blks_per_ch = spp->blks_per_lun * spp->luns_per_ch;
    spp->tt_blks = spp->blks_per_ch * spp->nchs;

    spp->pls_per_ch =  spp->pls_per_lun * spp->luns_per_ch;
    spp->tt_pls = spp->pls_per_ch * spp->nchs;

    spp->tt_luns = spp->luns_per_ch * spp->nchs;

    /* line is special, put it at the end */
    spp->blks_per_line = spp->tt_luns; /* TODO: to fix under multiplanes */
    spp->pgs_per_line = spp->blks_per_line * spp->pgs_per_blk;
    spp->secs_per_line = spp->pgs_per_line * spp->secs_per_pg;
    spp->tt_lines = spp->blks_per_lun; /* TODO: to fix under multiplanes */

    // Start with 5 lines. Note that number of meta/data lines are
    // dynamically changed as lsmtree's level multiplier change.
    spp->meta_lines = 5;
    spp->data_lines = spp->tt_lines - spp->meta_lines;

    spp->gc_thres_pcent = 0.75;
    spp->gc_thres_lines = (int)((1 - spp->gc_thres_pcent) * spp->tt_lines);
    spp->meta_gc_thres_lines = (int)((1 - spp->gc_thres_pcent) * spp->meta_lines);
    spp->gc_thres_lines = (int)((1 - spp->gc_thres_pcent) * spp->tt_lines);
    spp->data_gc_thres_lines = (int)((1 - spp->gc_thres_pcent) * spp->data_lines);

    spp->gc_thres_pcent_high = 0.95;
    spp->gc_thres_lines_high = (int)((1 - spp->gc_thres_pcent_high) * spp->tt_lines);
    spp->meta_gc_thres_lines_high = (int)((1 - spp->gc_thres_pcent_high) * spp->meta_lines);
    spp->data_gc_thres_lines_high = (int)((1 - spp->gc_thres_pcent_high) * spp->data_lines);
    spp->enable_gc_delay = true;

    spp->enable_comp_delay = true;
}

static void move_line_d2m(void)
{
    struct line_partition *m = &pink_ssd->lm.meta;
    struct line_partition *d = &pink_ssd->lm.data;
    struct line *line;

    if (d->free_line_cnt < 2) {
        kv_err("no free data lines\n");
    }

    line = get_next_free_line(d);
    kv_assert(!line->meta);
    d->lines--;

    QTAILQ_INSERT_TAIL(&m->free_line_list, line, entry);
    line->meta = true;
    m->free_line_cnt++;
    m->lines++;

    kv_log("d2m line(%d): data lines(%d), meta lines(%d)\n", line->id, d->lines, m->lines);
}

void pink_adjust_lines(void)
{
    const int compaction_margin = 5;
    int min_meta_lines;
    int meta_pages = 0;

    for (int i = 0; i < LSM_LEVELN; i++)
        meta_pages += pink_lsm->disk[i]->m_num;

    min_meta_lines = (meta_pages / pink_ssd->sp.pgs_per_line) + compaction_margin;
    if (pink_ssd->lm.meta.lines < min_meta_lines)
        move_line_d2m();
}

static void ssd_init_nand_page(struct nand_page *pg, struct ssdparams *spp)
{
    pg->nsecs = spp->secs_per_pg;
    pg->sec = (nand_sec_status_t*) malloc(sizeof(nand_sec_status_t) * pg->nsecs);
    for (int i = 0; i < pg->nsecs; i++) {
        pg->sec[i] = SEC_FREE;
    }
    pg->status = PG_FREE;
    pg->data = NULL;
}

static void ssd_init_nand_blk(struct nand_block *blk, struct ssdparams *spp)
{
    blk->npgs = spp->pgs_per_blk;
    blk->pg = g_malloc0(sizeof(struct nand_page) * blk->npgs);
    for (int i = 0; i < blk->npgs; i++) {
        ssd_init_nand_page(&blk->pg[i], spp);
    }
    blk->ipc = 0;
    blk->vpc = 0;
    blk->erase_cnt = 0;
    blk->wp = 0;
}

static void ssd_init_nand_plane(struct nand_plane *pl, struct ssdparams *spp)
{
    pl->nblks = spp->blks_per_pl;
    pl->blk = g_malloc0(sizeof(struct nand_block) * pl->nblks);
    for (int i = 0; i < pl->nblks; i++) {
        ssd_init_nand_blk(&pl->blk[i], spp);
    }
}

static void ssd_init_nand_lun(struct nand_lun *lun, struct ssdparams *spp)
{
    lun->npls = spp->pls_per_lun;
    lun->pl = g_malloc0(sizeof(struct nand_plane) * lun->npls);
    for (int i = 0; i < lun->npls; i++) {
        ssd_init_nand_plane(&lun->pl[i], spp);
    }
    lun->next_lun_avail_time = 0;
    lun->busy = false;
}

static void ssd_init_ch(struct ssd_channel *ch, struct ssdparams *spp)
{
    ch->nluns = spp->luns_per_ch;
    ch->lun = g_malloc0(sizeof(struct nand_lun) * ch->nluns);
    for (int i = 0; i < ch->nluns; i++) {
        ssd_init_nand_lun(&ch->lun[i], spp);
    }
    ch->next_ch_avail_time = 0;
    ch->busy = 0;
}

static void pink_lsm_setup_cache_size(struct kv_lsm_options *lopts)
{
    uint64_t ssd_capacity = (unsigned long long) pink_ssd->sp.tt_pgs * PAGESIZE;
    // 10% of SSD capacity is assigned to OP.
    uint64_t ssd_capacity_exclude_op = ssd_capacity * 90 / 100;
    lopts->cache_memory_size = ssd_capacity_exclude_op / 1024; // 0.10%.
    kv_log("Set cache memory to %lu MB\n", lopts->cache_memory_size / 1024 / 1024);
}

void pinkssd_init(FemuCtrl *n) {
    struct ssdparams *spp = &pink_ssd->sp;
    struct kv_lsm_options *lopts;

    kv_assert(pink_ssd);

    ssd_init_params(spp);

    /* initialize ssd internal layout architecture */
    pink_ssd->ch = g_malloc0(sizeof(struct ssd_channel) * spp->nchs);
    for (int i = 0; i < spp->nchs; i++) {
        ssd_init_ch(&pink_ssd->ch[i], spp);
    }

    /* initialize all the lines */
    ssd_init_lines();

    /* initialize write pointer, this is how we allocate new pages for writes */
    ssd_init_write_pointer();

    lopts = kv_lsm_default_opts();
    // set custom options
    pink_lsm_setup_cache_size(lopts);
    /////////////////////
    kv_lsm_setup_db(&pink_ssd->lops, PINK);
    pink_ssd->lops->open(lopts);
    pink_lsm_create();

    pthread_spin_init(&pink_ssd->nand_lock, PTHREAD_PROCESS_PRIVATE);
    qemu_thread_create(&pink_ssd->ftl_thread, "FEMU-FTL-Thread", ftl_thread, n, QEMU_THREAD_JOINABLE);
    pink_ssd->do_reset = true;
}

static inline struct ssd_channel *get_ch(struct femu_ppa *ppa)
{
    return &(pink_ssd->ch[ppa->g.ch]);
}

inline struct nand_lun *get_lun(struct femu_ppa *ppa)
{
    struct ssd_channel *ch = get_ch(ppa);
    return &(ch->lun[ppa->g.lun]);
}

static inline struct nand_plane *get_pl(struct femu_ppa *ppa)
{
    struct nand_lun *lun = get_lun(ppa);
    return &(lun->pl[ppa->g.pl]);
}

static inline struct nand_block *get_blk(struct femu_ppa *ppa)
{
    struct nand_plane *pl = get_pl(ppa);
    return &(pl->blk[ppa->g.blk]);
}

inline struct line *get_line(struct femu_ppa *ppa)
{
    return &(pink_ssd->lm.lines[ppa->g.blk]);
}

inline struct nand_page *get_pg(struct femu_ppa *ppa)
{
    struct nand_block *blk = get_blk(ppa);
    return &(blk->pg[ppa->g.pg]);
}

static bool is_meta_page(struct femu_ppa *ppa)
{
    return pink_ssd->lm.lines[ppa->g.blk].meta;
}

uint64_t pink_ssd_advance_status(struct femu_ppa *ppa, struct nand_cmd *ncmd)
{
    if (!pink_ssd->start_log) {
        return false;
    }

    int c = ncmd->cmd;
    uint64_t cmd_stime = (ncmd->stime == 0) ? \
        qemu_clock_get_ns(QEMU_CLOCK_REALTIME) : ncmd->stime;
    uint64_t nand_stime;
    struct nand_lun *lun = get_lun(ppa);
    uint64_t lat = 0;

    if (ncmd->type == USER_IO) {
        if (c == NAND_READ) {
            qatomic_inc(&pink_ssd->n->host_read_cnt);
        } else if (c == NAND_WRITE) {
            qatomic_inc(&pink_ssd->n->host_write_cnt);
        }
    } else if (ncmd->type == COMP_IO) {
        if (c == NAND_READ) {
            qatomic_inc(&pink_ssd->n->comp_read_cnt);
        } else if (c == NAND_WRITE) {
            qatomic_inc(&pink_ssd->n->comp_write_cnt);
        }
    } else if (ncmd->type == GC_IO) {
        if (c == NAND_READ) {
            qatomic_inc(&pink_ssd->n->gc_read_cnt);
        } else if (c == NAND_WRITE) {
            qatomic_inc(&pink_ssd->n->gc_write_cnt);
        }
    }

    uint8_t page_type = kvssd_get_page_type(&pink_ssd->lat, ppa->g.pg);
    switch (c) {
    case NAND_READ:
        /* read: perform NAND cmd first */
        nand_stime = (lun->next_lun_avail_time < cmd_stime) ? cmd_stime : \
                     lun->next_lun_avail_time;
        lun->next_lun_avail_time = nand_stime + kvssd_get_page_read_latency(&pink_ssd->lat, page_type);
        lat = lun->next_lun_avail_time - cmd_stime;
#if 0
        lun->next_lun_avail_time = nand_stime + spp->pg_rd_lat;

        /* read: then data transfer through channel */
        chnl_stime = (ch->next_ch_avail_time < lun->next_lun_avail_time) ? \
            lun->next_lun_avail_time : ch->next_ch_avail_time;
        ch->next_ch_avail_time = chnl_stime + spp->ch_xfer_lat;

        lat = ch->next_ch_avail_time - cmd_stime;
#endif
        break;

    case NAND_WRITE:
        /* write: transfer data through channel first */
        nand_stime = (lun->next_lun_avail_time < cmd_stime) ? cmd_stime : \
                     lun->next_lun_avail_time;
        if (ncmd->type == USER_IO) {
            lun->next_lun_avail_time = nand_stime + kvssd_get_page_write_latency(&pink_ssd->lat, page_type);
        } else {
            lun->next_lun_avail_time = nand_stime + kvssd_get_page_write_latency(&pink_ssd->lat, page_type);
        }
        lat = lun->next_lun_avail_time - cmd_stime;

#if 0
        chnl_stime = (ch->next_ch_avail_time < cmd_stime) ? cmd_stime : \
                     ch->next_ch_avail_time;
        ch->next_ch_avail_time = chnl_stime + spp->ch_xfer_lat;

        /* write: then do NAND program */
        nand_stime = (lun->next_lun_avail_time < ch->next_ch_avail_time) ? \
            ch->next_ch_avail_time : lun->next_lun_avail_time;
        lun->next_lun_avail_time = nand_stime + spp->pg_wr_lat;

        lat = lun->next_lun_avail_time - cmd_stime;
#endif
        break;

    case NAND_ERASE:
        /* erase: only need to advance NAND status */
        nand_stime = (lun->next_lun_avail_time < cmd_stime) ? cmd_stime : \
                     lun->next_lun_avail_time;
        lun->next_lun_avail_time = nand_stime + kvssd_get_blk_erase_latency(&pink_ssd->lat);

        lat = lun->next_lun_avail_time - cmd_stime;
        break;

    default:
        kv_err("Unsupported NAND command: 0x%x\n", c);
    }

    return lat;
}

void mark_sector_invalid(struct femu_ppa *ppa)
{
    struct line_partition *lm;
    struct ssdparams *spp = &pink_ssd->sp;
    bool was_full_line = false;
    struct line *line;

    if (is_meta_page(ppa)) {
        lm = &pink_ssd->lm.meta;
    } else {
        lm = &pink_ssd->lm.data;
    }

    /* update corresponding line status */
    line = get_line(ppa);
    if (line->vsc <= 0) {
        kv_debug("line id: %d, vpc: %d, vsc(sector: 64byte): %d, isc: %d, pgs_per_line: %d\n", line->id, line->vpc, line->vsc, line->isc, spp->pgs_per_line);
    }

    const int secs_per_line = spp->secs_per_line * 32;
    if (line->vsc == secs_per_line) {
        was_full_line = true;
    }
    line->isc++;
    kv_assert(line->vsc > 0);
    kv_assert(line->vsc <= secs_per_line);

    /* Adjust the position of the victime line in the pq under over-writes */
    if (line->pos) {
        /* Note that line->vpc will be updated by this call */
        pqueue_change_priority(lm->victim_line_pq, line->vsc - 1, line);
    } else {
        line->vsc--;
    }

    if (was_full_line) {
        /* move line: "full" -> "victim" */
        QTAILQ_REMOVE(&lm->full_line_list, line, entry);
        lm->full_line_cnt--;
        pqueue_insert(lm->victim_line_pq, line);
        lm->victim_line_cnt++;
    }
}

/* update SSD status about one page from PG_VALID -> PG_VALID */
void mark_page_invalid(struct femu_ppa *ppa)
{
    struct line_partition *lm;
    struct ssdparams *spp = &pink_ssd->sp;
    struct nand_block *blk = NULL;
    struct nand_page *pg = NULL;
    bool was_full_line = false;
    struct line *line;

    if (is_meta_page(ppa)) {
        lm = &pink_ssd->lm.meta;
    } else {
        lm = &pink_ssd->lm.data;
    }

    /* update corresponding page status */
    pg = get_pg(ppa);
    kv_assert(pg->status == PG_VALID);
    pg->status = PG_INVALID;
    if (pg->data)
        FREE(pg->data);
    pg->data = NULL;

    /* update corresponding block status */
    blk = get_blk(ppa);
    kv_assert(blk->ipc >= 0 && blk->ipc < spp->pgs_per_blk);
    blk->ipc++;
    kv_assert(blk->vpc > 0 && blk->vpc <= spp->pgs_per_blk);
    blk->vpc--;

    /* update corresponding line status */
    line = get_line(ppa);
    kv_assert(line->ipc >= 0 && line->ipc < spp->pgs_per_line);
    if (line->vpc == spp->pgs_per_line) {
        kv_assert(line->ipc == 0);
        was_full_line = true;
    }
    line->ipc++;
    kv_assert(line->vpc > 0 && line->vpc <= spp->pgs_per_line);

    /* meta only. fix me. */
    kv_assert(is_meta_page(ppa));
    /* Adjust the position of the victime line in the pq under over-writes */
    if (line->pos) {
        /* Note that line->vpc will be updated by this call */
        pqueue_change_priority(lm->victim_line_pq, line->vpc - 1, line);
    } else {
        line->vpc--;
    }

    if (was_full_line) {
        /* move line: "full" -> "victim" */
        QTAILQ_REMOVE(&lm->full_line_list, line, entry);
        lm->full_line_cnt--;
        pqueue_insert(lm->victim_line_pq, line);
        lm->victim_line_cnt++;
    }
}

void mark_page_valid(struct femu_ppa *ppa)
{
    struct nand_block *blk = NULL;
    struct nand_page *pg = NULL;
    struct line *line;

    /* update page status */
    pg = get_pg(ppa);
    kv_assert(pg->status == PG_FREE);
    pg->status = PG_VALID;

    /* update corresponding block status */
    blk = get_blk(ppa);
    kv_assert(blk->vpc >= 0 && blk->vpc < pink_ssd->sp.pgs_per_blk);
    blk->vpc++;

    /* update corresponding line status */
    line = get_line(ppa);
    kv_assert(line->vpc >= 0 && line->vpc < pink_ssd->sp.pgs_per_line);
    line->vpc++;
    line->vsc += (pink_ssd->sp.secs_per_pg * 32);
}

void mark_block_free(struct femu_ppa *ppa)
{
    struct ssdparams *spp = &pink_ssd->sp;
    struct nand_block *blk = get_blk(ppa);
    struct nand_page *pg = NULL;

    for (int i = 0; i < spp->pgs_per_blk; i++) {
        /* reset page status */
        pg = &blk->pg[i];
        kv_assert(pg->nsecs == spp->secs_per_pg);
        pg->status = PG_FREE;
    }

    /* reset block status */
    kv_assert(blk->npgs == spp->pgs_per_blk);
    blk->ipc = 0;
    blk->vpc = 0;
    blk->erase_cnt++;
}

static struct line *select_victim_line(struct line_partition *lm, bool force, bool meta)
{
    struct line *victim_line = NULL;

    // If no victim data lines, we pop the oldest one in the full lines.
    // We don't have exact invalid counts, so just pick the oldest one.
    if (lm == &pink_ssd->lm.data &&
        !pqueue_size(lm->victim_line_pq)) {
        struct line *l = QTAILQ_FIRST(&lm->full_line_list);
        QTAILQ_REMOVE(&lm->full_line_list, l, entry);
        lm->full_line_cnt--;
        pqueue_insert(lm->victim_line_pq, l);
        lm->victim_line_cnt++;
    }

    victim_line = pqueue_peek(lm->victim_line_pq);
    if (!victim_line) {
        return NULL;
    }

    if (meta) {
        if (!force && victim_line->ipc < pink_ssd->sp.pgs_per_line / 32) {
            return NULL;
        }
    } else {
        if (!force && victim_line->isc < pink_ssd->sp.secs_per_line / 32) {
            return NULL;
        }
    }

    pqueue_pop(lm->victim_line_pq);
    victim_line->pos = 0;
    lm->victim_line_cnt--;

    /* victim_line is a danggling node now */
    return victim_line;
}

struct line *select_victim_meta_line(bool force)
{
    return select_victim_line(&pink_ssd->lm.meta, force, true);
}

struct line *select_victim_data_line(bool force)
{
    return select_victim_line(&pink_ssd->lm.data, force, false);
}

void mark_line_free(struct femu_ppa *ppa)
{
    struct line_partition *lm;
    struct line *line;

    if (is_meta_page(ppa)) {
        lm = &pink_ssd->lm.meta;
    } else {
        lm = &pink_ssd->lm.data;
    }

    line = get_line(ppa);
    line->ipc = 0;
    line->vpc = 0;
    line->isc = 0;
    line->vsc = 0;
    /* move this line to free line list */
    QTAILQ_INSERT_TAIL(&lm->free_line_list, line, entry);
    lm->free_line_cnt++;
}

static int
make_room_for_write(void)
{
    while (true)
    {
        if (kv_skiplist_approximate_memory_usage(pink_lsm->mem) < WRITE_BUFFER_SIZE)
            break;

        if (pink_lsm->imm)
        {
            qemu_mutex_unlock(&pink_lsm->mu);
            usleep(1000);
            qemu_mutex_lock(&pink_lsm->mu);
            continue;
        }

        pink_lsm->imm = pink_lsm->mem;
        pink_lsm->mem = kv_skiplist_init();

        pink_maybe_schedule_compaction();
    }

    return 0;
}

static keyset* find_from_list(kv_key key, kv_skiplist *list) {
    keyset *target_set = NULL;
    kv_snode *target_node;
    if (list) {
        target_node = kv_skiplist_find(list, key);
        if(target_node) {
            target_set = (keyset *) malloc(sizeof(struct keyset));
            kv_copy_key(&target_set->lpa.k, &target_node->key);
            if (target_node->private) {
                target_set->ppa = *snode_ppa(target_node);
                int off = *snode_off(target_node);

                struct nand_page *pg2 = get_pg(&target_set->ppa);
                kv_assert(((uint16_t *)pg2->data)[0]);
                kv_assert(strncmp(pg2->data + ((uint16_t *)pg2->data)[off+1],
                                  target_set->lpa.k.key, target_set->lpa.k.len) == 0);
                target_set->value.length = ((uint16_t *)pg2->data)[off+2] - ((uint16_t *)pg2->data)[off+1] - target_set->lpa.k.len;
                target_set->value.value = (char *) malloc(target_set->value.length * sizeof(char));
                memcpy(target_set->value.value, pg2->data + ((uint16_t *)pg2->data)[off+1] + target_set->lpa.k.len, target_set->value.length);
            } else {
                target_set->ppa.ppa = UNMAPPED_PPA;
                target_set->value.length = target_node->value->length;
                target_set->value.value = g_malloc0(target_set->value.length);
                memcpy(target_set->value.value, target_node->value->value, target_set->value.length);
            }
        }
    }
    return target_set;
}

static uint64_t ssd_retrieve(NvmeRequest *req)
{
    keyset *found = NULL;
    kv_key k;
    //uint64_t sublat, maxlat = 0;
    uint64_t sublat = 0;
    kv_skiplist *mem, *imm, *key_only_mem, *key_only_imm;

    qemu_mutex_lock(&pink_lsm->mu);

    mem = pink_lsm->mem;
    kv_skiplist_get(mem);

    imm = pink_lsm->imm;
    kv_skiplist_get(imm);

    key_only_mem = pink_lsm->key_only_mem;
    kv_skiplist_get(key_only_mem);

    key_only_imm = pink_lsm->key_only_imm;
    kv_skiplist_get(key_only_imm);

    qemu_mutex_unlock(&pink_lsm->mu);

    req->etime = qemu_clock_get_ns(QEMU_CLOCK_REALTIME);

    k.key = g_malloc0(req->key_length);
    memcpy(k.key, req->key_buf, req->key_length);
    k.len = req->key_length;

    /* 1. Check L0: memtable (skiplist). */
    found = find_from_list(k, mem);
    if (found) {
        req->value_length = found->value.length;
        req->value = (uint8_t *) found->value.value;
        FREE(found->lpa.k.key);
        FREE(found);
        FREE(k.key);

        kv_skiplist_put(mem);
        return req->etime - req->stime;
    }
    kv_skiplist_put(mem);

    found = find_from_list(k, imm);
    if (found) {
        req->value_length = found->value.length;
        req->value = (uint8_t *) found->value.value;
        FREE(found->lpa.k.key);
        FREE(found);
        FREE(k.key);

        kv_skiplist_put(imm);
        return req->etime - req->stime;
    }
    kv_skiplist_put(imm);

    /* 2. Check key only memtable (skiplist). */
    found = find_from_list(k, key_only_mem);
    if (found) {
        req->value_length = found->value.length;
        req->value = (uint8_t *) found->value.value;
        FREE(found->lpa.k.key);
        FREE(found);
        FREE(k.key);

        kv_skiplist_put(key_only_mem);
        return req->etime - req->stime;
    }
    kv_skiplist_put(key_only_mem);

    /* 2. Check compaction temp table (skiplist). */
    found = find_from_list(k, key_only_imm);
    if (found) {
        req->value_length = found->value.length;
        req->value = (uint8_t *) found->value.value;
        FREE(found->lpa.k.key);
        FREE(found);
        FREE(k.key);

        kv_skiplist_put(key_only_imm);
        return req->etime - req->stime;
    }
    kv_skiplist_put(key_only_imm);

    /* 3. Walk lower levels. prepare params */
    int level = 0;
    pink_level_list_entry *entry = NULL;
    uint8_t result;
retry:
    result = lsm_find_run(k, &entry, &found, &level, req);

    //int i;
    struct nand_page *pg;
    struct nand_cmd srd;
    switch (result) {
        case CACHING:
            kv_assert(found != NULL);

            pg = get_pg(&found->ppa);
            srd.type = USER_IO;
            srd.cmd = NAND_READ;
            //srd.stime = req->stime;
            srd.stime = req->etime;
            sublat = pink_ssd_advance_status(&found->ppa, &srd);
            // maxlat = (sublat > maxlat) ? sublat : maxlat;
            req->etime += sublat;
            //maxlat += sublat;
            kv_assert(((uint16_t *)pg->data)[0]);
            kv_assert(strncmp(
                        pg->data + ((uint16_t *)pg->data)[found->lpa.line_age.g.in_page_idx+1],
                        found->lpa.k.key, found->lpa.k.len) == 0);
            found->value.length = ((uint16_t *)pg->data)[found->lpa.line_age.g.in_page_idx+2] - ((uint16_t *)pg->data)[found->lpa.line_age.g.in_page_idx+1] - found->lpa.k.len;
            found->value.value = (char *) malloc(found->value.length * sizeof(char));
            memcpy(found->value.value, pg->data + ((uint16_t *)pg->data)[found->lpa.line_age.g.in_page_idx+1] + found->lpa.k.len, found->value.length);
            req->value_length = found->value.length;
            req->value = (uint8_t *) found->value.value;
            FREE(found->lpa.k.key);
            FREE(found);
            FREE(k.key);
            req->flash_access_count++;
            return req->etime - req->stime;
        case COMP_FOUND:
            /*
             * Success to find the run in a compaction level.
             * Compactioning level doesn't have a range pointer yet. Reset entry to NULL.
             */
            if (entry->buffer) {
                // entry has not been written to flash yet.
                found = find_keyset((char *)entry->buffer, k);
            } else {
                pg = get_pg(&entry->ppa);
                if (kv_is_cached(pink_lsm->lsm_cache, entry->cache[META_SEGMENT])) {
#ifdef CACHE_UPDATE
                    kv_cache_update(pink_lsm->lsm_cache, entry->cache[META_SEGMENT]);
#endif
                    pink_lsm->cache_hit++;
                    if (pink_lsm->cache_hit % 10000 == 0) {
                        kv_debug("cache hit ratio: %lu\n", pink_lsm->cache_hit * 100 / (pink_lsm->cache_hit + pink_lsm->cache_miss));
                    }
                } else {
                    srd.type = USER_IO;
                    srd.cmd = NAND_READ;
                    //srd.stime = req->stime;
                    srd.stime = req->etime;
                    sublat = pink_ssd_advance_status(&entry->ppa, &srd);
                    //maxlat = (sublat > maxlat) ? sublat : maxlat;
                    req->etime += sublat;
                    pink_lsm->cache_miss++;
                    req->flash_access_count++;
                }

                kv_assert(pg->data != NULL);
                found = find_keyset((char *)pg->data, k);
            }
            entry = NULL;
            if (found == NULL) {
                level++;
                goto retry;
            }

            pg = get_pg(&found->ppa);
            srd.type = USER_IO;
            srd.cmd = NAND_READ;
            //srd.stime = req->stime;
            srd.stime = req->etime;
            sublat = pink_ssd_advance_status(&found->ppa, &srd);
            //maxlat = (sublat > maxlat) ? sublat : maxlat;
            req->etime += sublat;

            kv_assert(((uint16_t *)pg->data)[0]);
            kv_assert(strncmp(
                        pg->data + ((uint16_t *)pg->data)[found->lpa.line_age.g.in_page_idx+1],
                        found->lpa.k.key, found->lpa.k.len) == 0);
            found->value.length = ((uint16_t *)pg->data)[found->lpa.line_age.g.in_page_idx+2] - ((uint16_t *)pg->data)[found->lpa.line_age.g.in_page_idx+1] - found->lpa.k.len;
            found->value.value = (char *) malloc(found->value.length * sizeof(char));
            memcpy(found->value.value, pg->data + ((uint16_t *)pg->data)[found->lpa.line_age.g.in_page_idx+1] + found->lpa.k.len, found->value.length);
            req->value_length = found->value.length;
            req->value = (uint8_t *) found->value.value;
            FREE(found->lpa.k.key);
            FREE(found);
            FREE(k.key);
            req->flash_access_count++;
            return req->etime - req->stime;
        case FOUND:
            /*
             * Success to find the run in not pinned levels.
             * But not try to find the keyset yet.
             */
            pg = get_pg(&entry->ppa);
            if (kv_is_cached(pink_lsm->lsm_cache, entry->cache[META_SEGMENT])) {
#ifdef CACHE_UPDATE
                kv_cache_update(pink_lsm->lsm_cache, entry->cache[META_SEGMENT]);
#endif
                pink_lsm->cache_hit++;
                if (pink_lsm->cache_hit % 10000 == 0) {
                    kv_debug("cache hit ratio: %lu\n", pink_lsm->cache_hit * 100 / (pink_lsm->cache_hit + pink_lsm->cache_miss));
                }
            } else {
                srd.type = USER_IO;
                srd.cmd = NAND_READ;
                //srd.stime = req->stime;
                srd.stime = req->etime;
                sublat = pink_ssd_advance_status(&entry->ppa, &srd);
                //maxlat = (sublat > maxlat) ? sublat : maxlat;
                req->etime += sublat;
                if (kv_cache_available(pink_lsm->lsm_cache, cache_level(META_SEGMENT, level))) {
                    kv_cache_insert(pink_lsm->lsm_cache, &entry->cache[META_SEGMENT], PAGESIZE, cache_level(META_SEGMENT, level), KV_CACHE_WITHOUT_FLAGS);
                }
                pink_lsm->cache_miss++;
                req->flash_access_count++;
            }

            kv_assert(pg->data != NULL);
            found = find_keyset((char *)pg->data, k);
            if (found == NULL) {
                level++;
                goto retry;
            }

            pg = get_pg(&found->ppa);
            srd.type = USER_IO;
            srd.cmd = NAND_READ;
            //srd.stime = req->stime;
            srd.stime = req->etime;
            sublat = pink_ssd_advance_status(&found->ppa, &srd);
            //maxlat = (sublat > maxlat) ? sublat : maxlat;
            req->etime += sublat;

            kv_assert(((uint16_t *)pg->data)[0]);
            kv_assert(strncmp(
                        pg->data + ((uint16_t *)pg->data)[found->lpa.line_age.g.in_page_idx+1],
                        found->lpa.k.key, found->lpa.k.len) == 0);
            found->value.length = ((uint16_t *)pg->data)[found->lpa.line_age.g.in_page_idx+2] - ((uint16_t *)pg->data)[found->lpa.line_age.g.in_page_idx+1] - found->lpa.k.len;
            found->value.value = (char *) malloc(found->value.length * sizeof(char));
            memcpy(found->value.value, pg->data + ((uint16_t *)pg->data)[found->lpa.line_age.g.in_page_idx+1] + found->lpa.k.len, found->value.length);
            req->value_length = found->value.length;
            req->value = (uint8_t *) found->value.value;
            FREE(found->lpa.k.key);
            FREE(found);
            FREE(k.key);
            req->flash_access_count++;
            return req->etime - req->stime;
        case NOTFOUND:
            kv_debug("not found?\n");
            FREE(k.key);
            break;
    }
    return req->etime - req->stime;
}

static uint64_t ssd_read(NvmeRequest *req)
{
    return 0;
}

static uint64_t ssd_store(NvmeRequest *req)
{
    kv_key k;
    kv_value *v;

    k.key = g_malloc0(req->key_length);
    memcpy(k.key, req->key_buf, req->key_length);
    k.len = req->key_length;
    kv_assert(k.len >= KV_MIN_KEY_LEN && k.len <= KV_MAX_KEY_LEN);

    v = g_malloc0(sizeof(kv_value));
    v->length = req->value_length;
    v->value = (char *) req->value;

    qemu_mutex_lock(&pink_lsm->mu);

    if (make_room_for_write() != 0)
    {
        // TODO: handling error
        return -1;
    }
    kv_skiplist_insert(pink_lsm->mem, k, v);

    qemu_mutex_unlock(&pink_lsm->mu);

    return 0;
}

static uint64_t ssd_write(NvmeRequest *req)
{
    return 0;
}

static uint64_t ssd_delete(NvmeRequest *req)
{
    return 0;
}

static void reset_delay(void)
{
    struct ssdparams *spp = &pink_ssd->sp;
    struct femu_ppa ppa;
    uint64_t now;

    now = qemu_clock_get_ns(QEMU_CLOCK_REALTIME);
    ppa.ppa = 0;
    for (int ch = 0; ch < spp->nchs; ch++) {
        for (int lun = 0; lun < spp->luns_per_ch; lun++) {
            ppa.g.ch = ch;
            ppa.g.lun = lun;
            struct nand_lun *lun = get_lun(&ppa);

            lun->next_lun_avail_time = now;
        }
    }
}

static void *ftl_thread(void *arg)
{
    FemuCtrl *n = (FemuCtrl *)arg;
    NvmeRequest *req = NULL;
    uint64_t lat = 0;
    int rc;
    int i;

    while (!*(pink_ssd->dataplane_started_ptr)) {
        usleep(100000);
    }

    /* FIXME: not safe, to handle ->to_ftl and ->to_poller gracefully */
    pink_ssd->to_ftl = n->to_ftl;
    pink_ssd->to_poller = n->to_poller;

    pink_ssd->start_log = false;
    while (1) {
        for (i = 1; i <= n->num_poller; i++) {
            if (!pink_ssd->to_ftl[i] || !femu_ring_count(pink_ssd->to_ftl[i]))
                continue;

            rc = femu_ring_dequeue(pink_ssd->to_ftl[i], (void *)&req, 1);
            if (rc != 1) {
                printf("FEMU: FTL to_ftl dequeue failed\n");
            }

            kv_assert(req);
            // FIXME: cmd.opcode and cmd_opcode; this should be merged
            switch (req->cmd_opcode) {
            case NVME_CMD_KV_STORE:
                lat = ssd_store(req);
                break;
            case NVME_CMD_KV_RETRIEVE:
                if (pink_ssd->start_log == false) {
                    kv_debug("Reset latency timer!\n");
                    pink_ssd->start_log = true;
                    for (int ch = 0; ch < pink_ssd->sp.nchs; ch++) {
                        for (int lun = 0; lun < pink_ssd->sp.luns_per_ch; lun++) {
                            pink_ssd->ch[ch].lun[lun].next_lun_avail_time = qemu_clock_get_ns(QEMU_CLOCK_REALTIME);
                        }
                    }
                }
                req->flash_access_count = 0;
                lat = ssd_retrieve(req);
                break;
            case NVME_CMD_KV_DELETE:
                if (pink_ssd->do_reset) {
                    reset_delay();
                    pink_ssd->do_reset = false;
                }
                lat = ssd_delete(req);
                break;
            case NVME_CMD_KV_ITERATE_REQUEST:
            case NVME_CMD_KV_ITERATE_READ:
            case NVME_CMD_KV_DUMP:
                lat = 1;
                break;
            default:
                switch (req->cmd.opcode) {
                    case NVME_CMD_WRITE:
                        lat = ssd_write(req);
                        break;
                    case NVME_CMD_READ:
                        lat = ssd_read(req);
                        break;
                    case NVME_CMD_DSM:
                        lat = 0;
                        break;
                    default:
                        //kv_err("FTL received unkown request type, ERROR\n");
                        ;
                }
            }

            req->reqlat = lat;
            req->expire_time += lat;
            //kv_debug("op:%d lat:%ld\n", req->cmd_opcode, lat);

            rc = femu_ring_enqueue(pink_ssd->to_poller[i], (void *)&req, 1);
            if (rc != 1) {
                kv_err("FTL to_poller enqueue failed\n");
            }
        }
    }

    return NULL;
}
