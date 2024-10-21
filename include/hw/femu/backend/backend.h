#ifndef __FEMU_BACKEND_H
#define __FEMU_BACKEND_H

/* Backend of SSD address space */
typedef struct SsdBackend {
    void    *logical_space;
    int64_t size; /* in bytes */
    int     femu_mode;
} SsdBackend;

typedef struct SsdBackendOps {
    int  (*init)(struct SsdBackend **, int64_t nbytes, int type);
    void (*free)(struct SsdBackend *);
    int  (*rw)(struct SsdBackend *, QEMUSGList *qsg, uint64_t *lbal, bool is_write);
} SsdBackendOps;

#endif
