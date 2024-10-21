#ifndef __FEMU_KVSSD_DEBUG_H
#define __FEMU_KVSSD_DEBUG_H

#include <stdio.h>
#include <stdlib.h>

#ifdef FEMU_DEBUG_KV
#define kv_debug(fmt, ...) \
    do { printf("[FEMU] KV-Dbg: " fmt, ## __VA_ARGS__); } while (0)
#else
#define kv_debug(fmt, ...) \
    do { } while (0)
#endif

#define kv_err(fmt, ...) \
    do { fprintf(stderr, "[FEMU] KV-Err: " fmt, ## __VA_ARGS__); abort(); } while (0)

#define kv_log(fmt, ...) \
    do { printf("[FEMU] KV-Log: " fmt, ## __VA_ARGS__); } while (0)

/* FEMU assert() */
#ifdef FEMU_DEBUG_KV
#define kv_assert(expression) assert(expression)
#else
#define kv_assert(expression)
#endif

#endif
