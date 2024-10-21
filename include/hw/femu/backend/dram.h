#ifndef __FEMU_MEM_BACKEND
#define __FEMU_MEM_BACKEND

#include <stdint.h>

int init_dram_backend(SsdBackend **be, int64_t nbytes, int type);
void free_dram_backend(SsdBackend *);
int rw_dram_backend(SsdBackend *, QEMUSGList *, uint64_t *, bool);

#endif
