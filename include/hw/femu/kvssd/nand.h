#ifndef __FEMU_KVSSD_NAND_H
#define __FEMU_KVSSD_NAND_H

#include <stdio.h>
#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>
#include "qemu/queue.h"
#include "hw/femu/inc/pqueue.h"

// nand.h: collection of data structures with NAND related things,
//         based on FEMU bbssd code.

typedef int nand_sec_status_t;

struct nand_page {
    nand_sec_status_t *sec;
    int               nsecs;
    int               status;
    void              *data;
};

struct nand_block {
    struct nand_page *pg;
    int              npgs;
    int              ipc; /* invalid page count */
    int              vpc; /* valid page count */
    int              erase_cnt;
    int              wp; /* current write pointer */
};

struct nand_plane {
    struct nand_block *blk;
    int               nblks;
};

struct nand_lun {
    struct nand_plane *pl;
    int               npls;
    uint64_t          next_lun_avail_time;
    bool              busy;
    uint64_t          gc_endtime;
};

struct ssd_channel {
    struct nand_lun *lun;
    int             nluns;
    uint64_t        next_ch_avail_time;
    bool            busy;
    uint64_t        gc_endtime;
};

struct nand_cmd {
    int     type;
    int     cmd;
    int64_t stime; /* Coperd: request arrival time */
};

typedef struct line {
    int                id;  /* line id, the same as corresponding block id */
    int                ipc; /* invalid page count in this line */
    int                vpc; /* valid page count in this line */
    QTAILQ_ENTRY(line) entry; /* in either {free,victim,full} list */
    /* position in the priority queue for victim lines */
    size_t             pos;
    uint64_t           age;
    int                isc; /* invalid sector count in this line */
    int                vsc; /* valid sector count in this line */
    bool               meta;
    void               *private;
} line;

/* wp: record next write addr */
struct write_pointer {
    struct line *curline;
    int         ch;
    int         lun;
    int         pg;
    int         blk;
    int         pl;
};

struct line_partition {
    /* free line list, we only need to maintain a list of blk numbers */
    QTAILQ_HEAD(free_line_list, line)      free_line_list;
    QTAILQ_HEAD(meta_full_line_list, line) full_line_list;
    pqueue_t                               *victim_line_pq;

    struct write_pointer wp;
    int                  lines;
    int                  free_line_cnt;
    int                  victim_line_cnt;
    int                  full_line_cnt;
    uint64_t             age;
};

struct line_mgmt {
    struct line           *lines;
    int                   tt_lines;
    struct line_partition meta;
    struct line_partition data;
};

#endif
