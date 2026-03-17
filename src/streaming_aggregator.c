/*
 * streaming_aggregator.c — C port of the Java streaming aggregation engine.
 * Preserves all optimizations: mmap, all-integer arithmetic, branchless Lomuto
 * quickselect, parallel parse/merge/emit, digit-pair tables, direct fd write.
 * C advantage: direct pointer into mmap'd memory (no MBB bounds checks).
 *
 * Build: gcc -O3 -march=native -pthread -o streaming_aggregator src/streaming_aggregator.c
 * Run:   ./streaming_aggregator <input-file>
 */
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <pthread.h>
#include <limits.h>
#include <sys/uio.h>
#include <sched.h>

#define MAX_MINUTES 1500
#define MAX_SENSORS 1100
#define MAX_THREADS 64

/* Digit-pair lookup tables: halve divisions in number formatting */
static char DIGIT_TENS[100];
static char DIGIT_ONES[100];

static const int DAYS_CUM[]      = {0,31,59,90,120,151,181,212,243,273,304,334};
static const int DAYS_CUM_LEAP[] = {0,31,60,91,121,152,182,213,244,274,305,335};

/* ---- TumblingState: all-integer aggregates + values for percentiles ---- */

typedef struct {
    long count;
    long scaled_sum;
    int  scaled_min;
    int  scaled_max;
    int *values;
    int  size;
    int  cap;
    int  inline_values[8]; /* embedded small buffer — eliminates malloc for ~95% of states */
} TumblingState;

/* Per-thread arena allocator for TumblingState structs.
 * Eliminates ~833K malloc calls per thread, improves cache locality. */
#define ARENA_BLOCK_SIZE (1 << 16) /* 65536 structs per block */

typedef struct ArenaBlock {
    TumblingState states[ARENA_BLOCK_SIZE];
    struct ArenaBlock *next;
} ArenaBlock;

typedef struct {
    ArenaBlock *head;
    int used; /* count used in current block */
} Arena;

static __thread Arena t_arena = {NULL, ARENA_BLOCK_SIZE};

static inline TumblingState *ts_new(void) {
    if (t_arena.used >= ARENA_BLOCK_SIZE) {
        ArenaBlock *blk = malloc(sizeof(ArenaBlock));
        blk->next = t_arena.head;
        t_arena.head = blk;
        t_arena.used = 0;
    }
    TumblingState *ts = &t_arena.head->states[t_arena.used++];
    ts->count = 0;
    ts->scaled_sum = 0;
    ts->scaled_min = INT_MAX;
    ts->scaled_max = INT_MIN;
    ts->cap = 8;
    ts->values = ts->inline_values; /* use embedded buffer, no malloc */
    ts->size = 0;
    return ts;
}

static inline void ts_add(TumblingState *ts, int v) {
    ts->count++;
    ts->scaled_sum += v;
    if (v < ts->scaled_min) ts->scaled_min = v;
    if (v > ts->scaled_max) ts->scaled_max = v;
    if (ts->size == ts->cap) {
        ts->cap *= 2;
        if (ts->values == ts->inline_values) {
            /* Transition from inline to heap */
            ts->values = malloc(ts->cap * sizeof(int));
            memcpy(ts->values, ts->inline_values, 8 * sizeof(int));
        } else {
            ts->values = realloc(ts->values, ts->cap * sizeof(int));
        }
    }
    ts->values[ts->size++] = v;
}

static inline void ts_merge(TumblingState *dst, const TumblingState *src) {
    dst->count += src->count;
    dst->scaled_sum += src->scaled_sum;
    if (src->scaled_min < dst->scaled_min) dst->scaled_min = src->scaled_min;
    if (src->scaled_max > dst->scaled_max) dst->scaled_max = src->scaled_max;
    int needed = dst->size + src->size;
    if (needed > dst->cap) {
        dst->cap = needed;
        if (dst->values == dst->inline_values) {
            dst->values = malloc(dst->cap * sizeof(int));
            memcpy(dst->values, dst->inline_values, dst->size * sizeof(int));
        } else {
            dst->values = realloc(dst->values, dst->cap * sizeof(int));
        }
    }
    memcpy(dst->values + dst->size, src->values, src->size * sizeof(int));
    dst->size += src->size;
}

/* ---- Software-pipelined Lomuto quickselect via inline ASM ----
 * Preloads arr[i+1] at end of each iteration so the compare in the
 * next iteration doesn't stall on the 4-cycle L1 load latency.
 * GCC cannot auto-generate this software pipelining pattern. */

static int quickselect(int *arr, int lo, int hi, int k) {
    while (lo < hi) {
        int mid = lo + (hi - lo) / 2;
        if (arr[mid] < arr[lo]) { int t = arr[lo]; arr[lo] = arr[mid]; arr[mid] = t; }
        if (arr[hi]  < arr[lo]) { int t = arr[lo]; arr[lo] = arr[hi];  arr[hi]  = t; }
        if (arr[mid] < arr[hi]) { int t = arr[mid]; arr[mid] = arr[hi]; arr[hi] = t; }
        int pivot = arr[hi];

        int si = lo;
        int *pi = arr + lo;
        int *pend = arr + hi;
        int ai_pre = *pi;  /* preload first element */

        __asm__ volatile(
            "jmp 2f\n\t"
            ".p2align 4\n"
            "1:\n\t"
            /* ai_pre already holds arr[i] from previous iteration's preload */
            "movslq %[si], %%rsi\n\t"           /* rsi = si (sign-extend) */
            "lea (%[base], %%rsi, 4), %%rsi\n\t" /* rsi = &arr[si] */
            "mov (%%rsi), %%r15d\n\t"            /* r15d = arr[si] */
            "cmp %[pivot], %[ai]\n\t"            /* compare arr[i] with pivot */
            "mov %%r15d, (%[pi])\n\t"            /* arr[i] = arr[si] */
            "mov %[ai], (%%rsi)\n\t"             /* arr[si] = arr[i] (old) */
            "setl %%cl\n\t"                      /* cl = (arr[i] < pivot) */
            "movzbl %%cl, %[ai]\n\t"             /* reuse ai reg for 0/1 */
            "add %[ai], %[si]\n\t"               /* si += (arr[i] < pivot) */
            "add $4, %[pi]\n\t"                  /* pi++ (advance to arr[i+1]) */
            "mov (%[pi]), %[ai]\n\t"             /* PRELOAD arr[i+1] — hides L1 latency */
            "2:\n\t"
            "cmp %[pend], %[pi]\n\t"
            "jne 1b\n\t"
            : [si] "+r"(si), [pi] "+r"(pi), [ai] "+r"(ai_pre)
            : [base] "r"(arr), [pivot] "r"(pivot), [pend] "r"(pend)
            : "rsi", "r15", "cl", "cc", "memory"
        );

        arr[hi] = arr[si]; arr[si] = pivot;
        if (k == si) return pivot;
        else if (k < si) hi = si - 1;
        else lo = si + 1;
    }
    return arr[lo];
}

/* ---- Number formatting (digit-pair tables, same as Java) ---- */

static inline int append_long(char *buf, int pos, long v) {
    if (v < 0) { buf[pos++] = '-'; v = -v; }
    if (v == 0) { buf[pos++] = '0'; return pos; }
    if (v < 10)  { buf[pos++] = '0' + (int)v; return pos; }
    if (v < 100) { buf[pos++] = DIGIT_TENS[v]; buf[pos++] = DIGIT_ONES[v]; return pos; }
    int digits;
    if      (v < 1000)        digits = 3;
    else if (v < 10000)       digits = 4;
    else if (v < 100000)      digits = 5;
    else if (v < 1000000)     digits = 6;
    else if (v < 10000000)    digits = 7;
    else if (v < 100000000)   digits = 8;
    else if (v < 1000000000)  digits = 9;
    else if (v < 10000000000L) digits = 10;
    else                       digits = 11;
    int end = pos + digits, p = end;
    while (v >= 100) {
        int r = (int)(v % 100); v /= 100;
        buf[--p] = DIGIT_ONES[r];
        buf[--p] = DIGIT_TENS[r];
    }
    if (v >= 10) { buf[--p] = DIGIT_ONES[(int)v]; buf[--p] = DIGIT_TENS[(int)v]; }
    else buf[--p] = '0' + (int)v;
    return end;
}

static inline int append_scaled_int(char *buf, int pos, int v) {
    if (v < 0) { buf[pos++] = '-'; v = -v; }
    int ip = v / 100, fp = v % 100;
    if      (ip < 10)  { buf[pos++] = '0' + ip; }
    else if (ip < 100) { buf[pos++] = DIGIT_TENS[ip]; buf[pos++] = DIGIT_ONES[ip]; }
    else               { pos = append_long(buf, pos, ip); }
    buf[pos++] = '.';
    buf[pos++] = DIGIT_TENS[fp];
    buf[pos++] = DIGIT_ONES[fp];
    return pos;
}

static inline int append_scaled_long(char *buf, int pos, long v) {
    if (v < 0) { buf[pos++] = '-'; v = -v; }
    long ip = v / 100; int fp = (int)(v % 100);
    pos = append_long(buf, pos, ip);
    buf[pos++] = '.';
    buf[pos++] = DIGIT_TENS[fp];
    buf[pos++] = DIGIT_ONES[fp];
    return pos;
}

/* ---- Date helpers ---- */

static long count_leap_years(int y) {
    if (y < 0) return 0;
    return y / 4 - y / 100 + y / 400;
}

static long parse_iso_ts(const unsigned char *b) {
    int year   = (b[0]-'0')*1000 + (b[1]-'0')*100 + (b[2]-'0')*10 + (b[3]-'0');
    int month  = (b[5]-'0')*10  + (b[6]-'0');
    int day    = (b[8]-'0')*10  + (b[9]-'0');
    int hour   = (b[11]-'0')*10 + (b[12]-'0');
    int minute = (b[14]-'0')*10 + (b[15]-'0');
    int second = (b[17]-'0')*10 + (b[18]-'0');
    long td = 365L * (year - 1970);
    td += count_leap_years(year - 1) - count_leap_years(1969);
    int leap = (year % 4 == 0) && ((year % 100 != 0) || (year % 400 == 0));
    td += (leap ? DAYS_CUM_LEAP : DAYS_CUM)[month - 1] + day - 1;
    return ((td * 24 + hour) * 60 + minute) * 60000L + second * 1000L;
}

/* ---- Per-thread partition state ---- */

typedef struct {
    TumblingState **flat_rows; /* contiguous MAX_MINUTES*MAX_SENSORS array */
    int sensor_count;
    int min_minute;
    int max_minute;
} PartitionState;

/* ---- Global shared state ---- */

static const unsigned char *g_data;
static long g_file_size;
static int  g_nthreads;
static long g_base_ms;
static long g_boundaries[MAX_THREADS + 1];

static PartitionState g_parts[MAX_THREADS];
static TumblingState **g_merged[MAX_MINUTES];
static int g_sensor_count;
static int g_min_minute, g_max_minute;

/* Emit buffers per thread */
static char *g_sliding_bufs[MAX_THREADS];
static int   g_sliding_lens[MAX_THREADS];
static char *g_tumbling_bufs[MAX_THREADS];
static int   g_tumbling_lens[MAX_THREADS];

/* Pre-computed prefix strings and sensor names */
static char g_sliding_pfx[MAX_MINUTES][32];
static int  g_sliding_pfx_len[MAX_MINUTES];
static char g_tumbling_pfx[MAX_MINUTES][32];
static int  g_tumbling_pfx_len[MAX_MINUTES];
static char g_sensor_names[MAX_SENSORS][16]; /* "sensor_XXXX" = 11 chars */

static const char NEW_LINE[] = ",new\n";

static pthread_barrier_t g_barrier;

/* ---- Phase 1: Parse chunk (per-thread) ---- */

static void parse_chunk(int tid) {
    int start = (int)g_boundaries[tid];
    int end   = (int)g_boundaries[tid + 1];
    PartitionState *ps = &g_parts[tid];

    ps->sensor_count = 0;
    ps->min_minute = MAX_MINUTES;
    ps->max_minute = -1;
    ps->flat_rows = calloc((size_t)MAX_MINUTES * MAX_SENSORS, sizeof(TumblingState *));

    if (start >= end) return;

    const unsigned char *d = g_data;

    /* Compute day offset from first line in this chunk */
    const unsigned char *f = d + start;
    int year    = (f[0]-'0')*1000 + (f[1]-'0')*100 + (f[2]-'0')*10 + (f[3]-'0');
    int month   = (f[5]-'0')*10  + (f[6]-'0');
    int day_val = (f[8]-'0')*10  + (f[9]-'0');
    int hour0   = (f[11]-'0')*10 + (f[12]-'0');
    int minute0 = (f[14]-'0')*10 + (f[15]-'0');
    int second0 = (f[17]-'0')*10 + (f[18]-'0');

    long td = 365L * (year - 1970);
    td += count_leap_years(year - 1) - count_leap_years(1969);
    int leap = (year % 4 == 0) && ((year % 100 != 0) || (year % 400 == 0));
    td += (leap ? DAYS_CUM_LEAP : DAYS_CUM)[month - 1] + day_val - 1;
    long first_ts = ((td * 24 + hour0) * 60 + minute0) * 60000L + second0 * 1000L;
    int base_day_min = (int)(first_ts / 60000) - (int)(first_ts / 60000) % 1440;
    int base_min = (int)(g_base_ms / 60000);
    int day_off = base_day_min - base_min;

    int pos = start;
    while (pos < end) {
        const unsigned char *p = d + pos;

        int hour   = (p[11]-'0')*10 + (p[12]-'0');
        int minute = (p[14]-'0')*10 + (p[15]-'0');
        int s_idx  = (p[28]-'0')*1000 + (p[29]-'0')*100
                   + (p[30]-'0')*10   + (p[31]-'0');

        if (s_idx >= ps->sensor_count) ps->sensor_count = s_idx + 1;

        /* Specialized integer parser — value starts at offset 33 */
        int di = 33;
        unsigned char b0 = p[di];
        int neg = (b0 == '-');
        if (neg) { di++; b0 = p[di]; }
        unsigned char b1 = p[di + 1];
        int sv, ll;
        if (b1 == '.') {
            sv = (b0-'0')*100 + (p[di+2]-'0')*10 + (p[di+3]-'0');
            ll = di + 5;
        } else {
            unsigned char b2 = p[di + 2];
            if (b2 == '.') {
                sv = ((b0-'0')*10 + (b1-'0'))*100 + (p[di+3]-'0')*10 + (p[di+4]-'0');
                ll = di + 6;
            } else {
                sv = ((b0-'0')*100 + (b1-'0')*10 + (b2-'0'))*100 + (p[di+4]-'0')*10 + (p[di+5]-'0');
                ll = di + 7;
            }
        }
        if (neg) sv = -sv;
        pos += ll;

        int em = day_off + hour * 60 + minute;
        if (em >= 0 && em < MAX_MINUTES) {
            if (em < ps->min_minute) ps->min_minute = em;
            if (em > ps->max_minute) ps->max_minute = em;
            int idx = em * MAX_SENSORS + s_idx;
            TumblingState *ts = ps->flat_rows[idx];
            if (!ts) { ts = ts_new(); ps->flat_rows[idx] = ts; }
            ts_add(ts, sv);
        }
    }
}

/* ---- Phase 2: Merge partitions (sensor-range parallel) ---- */

static void merge_range(int tid) {
    int spt = (g_sensor_count + g_nthreads - 1) / g_nthreads;
    int s_start = tid * spt;
    int s_end = s_start + spt;
    if (s_end > g_sensor_count) s_end = g_sensor_count;

    for (int s = s_start; s < s_end; s++) {
        for (int p = 0; p < g_nthreads; p++) {
            PartitionState *ps = &g_parts[p];
            for (int m = ps->min_minute; m <= ps->max_minute; m++) {
                TumblingState *ts = ps->flat_rows[m * MAX_SENSORS + s];
                if (!ts) continue;
                if (!g_merged[m][s]) g_merged[m][s] = ts; /* first partition: pointer move */
                else ts_merge(g_merged[m][s], ts);
            }
        }
    }
}

/* ---- Phase 3a: Emit sliding windows (minute-range parallel) ---- */

static void emit_sliding(int tid) {
    int emit_min = g_min_minute > 4 ? g_min_minute - 4 : 0;
    int minute_range = g_max_minute - emit_min + 1;
    int mpt = (minute_range + g_nthreads - 1) / g_nthreads;
    int m_start = emit_min + tid * mpt;
    int m_end = m_start + mpt;
    if (m_end > g_max_minute + 1) m_end = g_max_minute + 1;

    int buf_cap = 8 << 20;
    char *buf = malloc(buf_cap);
    int pos = 0;
    int combined_cap = 1024;
    int *combined = malloc(combined_cap * sizeof(int));

    for (int m = m_start; m < m_end; m++) {
        int k_end = m + 4;
        if (k_end > g_max_minute) k_end = g_max_minute;

        for (int s = 0; s < g_sensor_count; s++) {
            /* Prefetch next sensor's TumblingState data while processing
             * current sensor's quickselect. Hides L2/L3 miss latency for
             * scattered arena pointers (~180 cycles saved per sensor). */
            if (s + 1 < g_sensor_count) {
                for (int k = m; k <= k_end; k++) {
                    if (g_merged[k] && g_merged[k][s + 1])
                        __builtin_prefetch(g_merged[k][s + 1], 0, 1);
                }
            }
            int p = 0;
            for (int k = m; k <= k_end; k++) {
                if (!g_merged[k]) continue;
                TumblingState *ts = g_merged[k][s];
                if (!ts) continue;
                int needed = p + ts->size;
                if (needed > combined_cap) {
                    combined_cap = needed * 2;
                    combined = realloc(combined, combined_cap * sizeof(int));
                }
                memcpy(combined + p, ts->values, ts->size * sizeof(int));
                p += ts->size;
            }
            if (p == 0) continue;

            int i99 = (99 * p + 99) / 100 - 1;
            if (i99 < 0) i99 = 0;
            int i50 = (p + 1) / 2 - 1;
            if (i50 < 0) i50 = 0;

            int p99, p50;
            if (i99 == p - 1) {
                /* p99 is the max — simple linear scan replaces full quickselect.
                 * For p <= 100 (always true for this workload), i99 == p-1.
                 * Saves ~2 partition passes of ~35 elements each. */
                int maxval = combined[0], max_idx = 0;
                for (int j = 1; j < p; j++) {
                    if (combined[j] > maxval) { maxval = combined[j]; max_idx = j; }
                }
                combined[max_idx] = combined[p - 1];
                combined[p - 1] = maxval;
                p99 = maxval;
                p50 = quickselect(combined, 0, p - 2, i50);
            } else {
                p99 = quickselect(combined, 0, p - 1, i99);
                p50 = quickselect(combined, 0, i99, i50);
            }

            if (pos + 200 > buf_cap) { buf_cap *= 2; buf = realloc(buf, buf_cap); }
            memcpy(buf + pos, g_sliding_pfx[m], g_sliding_pfx_len[m]); pos += g_sliding_pfx_len[m];
            memcpy(buf + pos, g_sensor_names[s], 11); pos += 11;
            buf[pos++] = ',';
            pos = append_scaled_int(buf, pos, p50);
            buf[pos++] = ',';
            pos = append_scaled_int(buf, pos, p99);
            memcpy(buf + pos, NEW_LINE, 5); pos += 5;
        }
    }

    g_sliding_bufs[tid] = buf;
    g_sliding_lens[tid] = pos;
    free(combined);
}

/* ---- Phase 3b: Emit tumbling windows (minute-range parallel) ---- */

static void emit_tumbling(int tid) {
    int emit_min = g_min_minute > 4 ? g_min_minute - 4 : 0;
    int minute_range = g_max_minute - emit_min + 1;
    int mpt = (minute_range + g_nthreads - 1) / g_nthreads;
    int m_start = emit_min + tid * mpt;
    int m_end = m_start + mpt;
    if (m_end > g_max_minute + 1) m_end = g_max_minute + 1;

    int buf_cap = 10 << 20;
    char *buf = malloc(buf_cap);
    int pos = 0;

    for (int m = m_start; m < m_end; m++) {
        if (!g_merged[m]) continue;
        for (int s = 0; s < g_sensor_count; s++) {
            TumblingState *st = g_merged[m][s];
            if (!st) continue;

            if (pos + 200 > buf_cap) { buf_cap *= 2; buf = realloc(buf, buf_cap); }
            memcpy(buf + pos, g_tumbling_pfx[m], g_tumbling_pfx_len[m]); pos += g_tumbling_pfx_len[m];
            memcpy(buf + pos, g_sensor_names[s], 11); pos += 11;
            buf[pos++] = ',';
            pos = append_long(buf, pos, st->count);
            buf[pos++] = ',';
            pos = append_scaled_long(buf, pos, st->scaled_sum);
            buf[pos++] = ',';
            pos = append_scaled_int(buf, pos, st->scaled_min);
            buf[pos++] = ',';
            pos = append_scaled_int(buf, pos, st->scaled_max);
            buf[pos++] = ',';
            /* Integer avg: scaledSum fits in int (max ~50 events x 99999 = 4.9M) */
            int ss = (int)st->scaled_sum, cc = (int)st->count;
            int avg = (ss >= 0) ? (ss + cc/2) / cc : (ss - cc/2) / cc;
            pos = append_scaled_int(buf, pos, avg);
            memcpy(buf + pos, NEW_LINE, 5); pos += 5;
        }
    }

    g_tumbling_bufs[tid] = buf;
    g_tumbling_lens[tid] = pos;
}

/* ---- Worker thread: all phases with barriers ---- */

static void *worker(void *arg) {
    int tid = (int)(long)arg;

    /* Pin thread to CPU core — avoid cache migration */
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(tid % CPU_SETSIZE, &cpuset);
    sched_setaffinity(0, sizeof(cpuset), &cpuset);

    /* Phase 1: Parse */
    parse_chunk(tid);
    pthread_barrier_wait(&g_barrier);

    /* tid 0 computes globals between parse and merge */
    if (tid == 0) {
        g_sensor_count = 0;
        g_min_minute = MAX_MINUTES;
        g_max_minute = -1;
        for (int t = 0; t < g_nthreads; t++) {
            if (g_parts[t].sensor_count > g_sensor_count) g_sensor_count = g_parts[t].sensor_count;
            if (g_parts[t].min_minute < g_min_minute)     g_min_minute = g_parts[t].min_minute;
            if (g_parts[t].max_minute > g_max_minute)     g_max_minute = g_parts[t].max_minute;
        }
        /* Allocate merged arrays for [gMin..gMax] */
        for (int m = g_min_minute; m <= g_max_minute; m++)
            g_merged[m] = calloc(g_sensor_count, sizeof(TumblingState *));

        /* Pre-compute prefix strings */
        int pfx_min = g_min_minute > 4 ? g_min_minute - 4 : 0;
        for (int m = pfx_min; m <= g_max_minute; m++) {
            long ws = g_base_ms + (long)m * 60000;
            g_sliding_pfx_len[m]  = snprintf(g_sliding_pfx[m],  32, "sliding,%ld,",  ws);
            g_tumbling_pfx_len[m] = snprintf(g_tumbling_pfx[m], 32, "tumbling,%ld,", ws);
        }
        /* Generate sensor names: sensor_0000 .. sensor_XXXX */
        for (int s = 0; s < g_sensor_count; s++)
            snprintf(g_sensor_names[s], 16, "sensor_%04d", s);
    }
    pthread_barrier_wait(&g_barrier);

    /* Phase 2: Merge (sensor-range parallel) */
    merge_range(tid);
    pthread_barrier_wait(&g_barrier);

    /* Phase 3: Emit (minute-range parallel) */
    emit_sliding(tid);
    emit_tumbling(tid);

    return NULL;
}

/* ---- Robust write: handle partial writes ---- */

static void write_all(int fd, const char *buf, int len) {
    while (len > 0) {
        ssize_t n = write(fd, buf, len);
        if (n <= 0) { perror("write"); _exit(1); }
        buf += n;
        len -= (int)n;
    }
}

/* ---- Main ---- */

int main(int argc, char **argv) {
    if (argc < 2) {
        fprintf(stderr, "Usage: streaming_aggregator <input-file>\n");
        return 1;
    }

    /* Init digit-pair tables */
    for (int i = 0; i < 100; i++) {
        DIGIT_TENS[i] = '0' + i / 10;
        DIGIT_ONES[i] = '0' + i % 10;
    }

    /* Open and mmap file — use readahead() for non-blocking page cache warmup */
    int fd = open(argv[1], O_RDONLY);
    if (fd < 0) { perror("open"); return 1; }
    struct stat st;
    fstat(fd, &st);
    g_file_size = st.st_size;
    readahead(fd, 0, g_file_size); /* async kernel readahead — doesn't block */
    g_data = mmap(NULL, g_file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (g_data == MAP_FAILED) { perror("mmap"); return 1; }

    /* Thread count = available CPUs */
    g_nthreads = sysconf(_SC_NPROCESSORS_ONLN);
    if (g_nthreads > MAX_THREADS) g_nthreads = MAX_THREADS;

    /* Find line-aligned chunk boundaries */
    g_boundaries[0] = 0;
    g_boundaries[g_nthreads] = g_file_size;
    for (int i = 1; i < g_nthreads; i++) {
        long approx = g_file_size * i / g_nthreads;
        while (approx < g_file_size && g_data[approx] != '\n') approx++;
        if (approx < g_file_size) approx++; /* skip past the newline */
        g_boundaries[i] = approx;
    }

    /* Parse baseMs from first line */
    long first_ts = parse_iso_ts(g_data);
    g_base_ms = (first_ts / 60000 - 15) * 60000;

    /* Init barrier and spawn worker threads */
    pthread_barrier_init(&g_barrier, NULL, g_nthreads);
    pthread_t threads[MAX_THREADS];
    for (int t = 1; t < g_nthreads; t++)
        pthread_create(&threads[t], NULL, worker, (void *)(long)t);

    /* Main thread is worker 0 */
    worker((void *)0L);

    /* Join all workers */
    for (int t = 1; t < g_nthreads; t++)
        pthread_join(threads[t], NULL);

    /* Write output: writev scatter-gather — batch all buffers in one syscall */
    {
        struct iovec iov[MAX_THREADS * 2];
        int iov_cnt = 0;
        for (int t = 0; t < g_nthreads; t++)
            if (g_sliding_lens[t] > 0) {
                iov[iov_cnt].iov_base = g_sliding_bufs[t];
                iov[iov_cnt].iov_len = g_sliding_lens[t];
                iov_cnt++;
            }
        for (int t = 0; t < g_nthreads; t++)
            if (g_tumbling_lens[t] > 0) {
                iov[iov_cnt].iov_base = g_tumbling_bufs[t];
                iov[iov_cnt].iov_len = g_tumbling_lens[t];
                iov_cnt++;
            }
        /* writev may not write everything in one call for large totals */
        int idx = 0;
        while (idx < iov_cnt) {
            int batch = iov_cnt - idx;
            if (batch > IOV_MAX) batch = IOV_MAX;
            ssize_t n = writev(STDOUT_FILENO, &iov[idx], batch);
            if (n <= 0) { perror("writev"); _exit(1); }
            /* Advance past fully-written iovecs */
            while (idx < iov_cnt && n > 0) {
                if ((size_t)n >= iov[idx].iov_len) {
                    n -= iov[idx].iov_len;
                    idx++;
                } else {
                    iov[idx].iov_base = (char *)iov[idx].iov_base + n;
                    iov[idx].iov_len -= n;
                    n = 0;
                }
            }
        }
    }

    munmap((void *)g_data, g_file_size);
    close(fd);
    return 0;
}
