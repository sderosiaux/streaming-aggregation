# Streaming Aggregation: 57K to 48M events/sec

A streaming window aggregation engine optimized from **57,823 ev/s** (naive Java) to **48,076,923 ev/s** (optimized C) — an **831x improvement** — using [autoresearch](https://github.com/sderosiaux/claudecode-autoresearch).

Two implementations:
- **Java**: 57K → 11.4M ev/s (198x) in 167 experiments
- **C**: 22M → 48M ev/s (831x baseline) in 228 experiments — inline ASM, software-pipelined quickselect, direct mmap parsing

Every commit in this repo is an experiment. The commit messages document the technique, the measured throughput, and the delta from the previous best.

## The workload

Process 10M timestamped CSV sensor events through:
- **Tumbling windows** (1 min): count, sum, min, max, avg per sensor
- **Sliding windows** (5 min window, 1 min slide): p50, p99 per sensor
- 1,000 sensors, 24 hours of data, 5% late events

Output: ~166MB to stdout (real I/O, not /dev/null).

## Run it

```bash
# Java version (requires Java 21+)
./autoresearch.sh
./autoresearch.checks.sh

# C version (requires gcc, pthreads)
make && ./streaming_aggregator data/10m.txt > /dev/null
./autoresearch-c.checks.sh
```

## The optimization journey

Read the commit history bottom-up (`git log --oneline --reverse`) to follow the full path:

```
57,823     → baseline: Instant.parse, String.split, ArrayList sort
328,558    → array-indexed windows replacing HashMaps
1,857,700  → direct sensor index parsing, no HashMap
2,683,123  → multi-threaded parallel chunk processing
3,480,682  → parallel merge+emit by sensor range
4,928,536  → parallel byte[] conversion in emit threads
5,743,825  → direct byte[] emit, no StringBuilder
6,720,430  → merge SlidingState into TumblingState (1 cache line)
7,283,321  → integer percentile indices + zero-copy emit buffers
7,668,711  → mmap file reading
8,536,103  → inline sliding percentiles (eliminate intermediate allocation)
9,154,010  → direct mmap parsing via sun.misc.Unsafe
9,345,794  → MappedByteBuffer zero-copy parsing (no Unsafe)
9,784,735  → C2-only JIT (skip C1 tier) + AlwaysCompileLoopMethods
9,980,039  → specialized double parser — dispatch by dot position
10,493,179 → eliminate newline scan — compute line length from value format
10,741,138 → int[] values for percentile quickselect — halve memory
10,787,486 → all-integer TumblingState — no FP in parse hot path
11,061,946 → digit-pair lookup tables + integer avg + direct fd output
11,261,261 → bulk MBB copy into stack-local byte[] buffer
11,261,261 → branchless Lomuto partition in quickselect (A/B +8%)
11,441,647 → skip sensor name String allocation — generate from indices (A/B +4.7%)
```

## C optimization journey

The C port preserves the Java architecture (mmap, parallel parse/merge/emit, all-integer arithmetic) and pushes further with direct memory control and inline assembly:

```
17,513,134 → baseline: C port of optimized Java engine
18,903,591 → arena allocator for TumblingState (+7.9%)
19,193,857 → madvise(MADV_SEQUENTIAL) on mmap (+1.5%)
21,231,422 → inline values[8] in TumblingState (+10.6%)
21,505,376 → writev scatter-gather output (+1.3%)
21,978,021 → flat row arrays, eliminate per-minute calloc (+2.2%)
27,027,027 → eliminate linebuf memcpy — parse directly from mmap (+23%)
33,333,333 → inline ASM software-pipelined Lomuto quickselect (+23%)
35,714,285 → readahead() syscall — async page cache warmup (+7%)
37,735,849 → sched_setaffinity CPU pinning (+6%)
46,728,971 → p99 as max scan — O(n) replaces quickselect for p99 (+2%)
46,948,356 → prefetch next sensor TumblingState in emit_sliding (+0.5%)
47,619,047 → remove immintrin.h — eliminate auto-vectorization interference (+1.4%)
48,076,923 → remove MADV_SEQUENTIAL — aggressive page eviction hurts threads (+1%)
```

**Key C-specific wins:**

| Technique | Impact |
|-----------|--------|
| Inline ASM software-pipelined Lomuto partition (preload arr[i+1]) | +23% |
| Direct mmap parsing (eliminate 480MB linebuf memcpy) | +23% |
| Arena allocator (per-thread block alloc, 65536 structs/block) | +7.9% |
| Inline values[8] embedded in TumblingState (eliminates malloc) | +10.6% |
| readahead() + remove MADV_SEQUENTIAL (page cache management) | +8% |
| sched_setaffinity CPU pinning (prevent cache migration) | +6% |
| p99 as max scan (mathematical proof: for p≤100, i99==p-1) | +2% |
| __builtin_prefetch in emit_sliding (hide L2/L3 miss latency) | +0.5% |

**What didn't work in C (~180 discards):** clang compiler, PGO, LTO, -funroll-loops, -Os/-O2, -fno-plt, -fno-tree-vectorize, MAP_POPULATE (file and anonymous), mlock(), MADV_HUGEPAGE, MADV_RANDOM, __attribute__((hot)), __builtin_expect, restrict qualifier, MAX_SENSORS=1024, inline_values[4], cache-aligned ArenaBlock, ts_new noinline, partial insertion sort, 2-line interleaved parse, counting sort, selection networks, SWAR 64-bit loads, stack-allocated combined[], right-sized buffers, CMOV value parser, compact flat_rows, 6-core SMT, work-stealing, fused emit phases.

**Key insight:** GCC's code layout is extremely fragile at this optimization level. Nearly every change that adds instructions or modifies function structure causes icache regressions. Only changes that strictly eliminate work succeed.

## Java key techniques

| Category | Technique | Impact |
|----------|-----------|--------|
| **I/O** | mmap + MappedByteBuffer bulk copy into stack-local byte[] | +12% |
| **Parsing** | Manual ISO-8601, precomputed day offset, specialized int parser, no newline scan | +15% |
| **Data structures** | Array-indexed [minute][sensor] layout, no HashMap | +6x |
| **Parallelism** | 12-thread chunk parse, sensor-range merge, minute-range emit | +3x |
| **Percentiles** | Branchless Lomuto quickselect, median-of-3 pivot, int[] values (halved memory) | +20% |
| **Output** | Direct byte[] assembly, digit-pair lookup tables, FileOutputStream(fd) | +17% |
| **Memory** | All-integer TumblingState (scaledSum/scaledMin/scaledMax), no FP in hot path | +6% |
| **JVM** | C2-only compilation, AlwaysCompileLoopMethods | +5% |

## Architecture (shared by both implementations)

```
┌──────────────────────────────────────────────────────────┐
│  mmap file (388MB) → 12 chunks (~32MB each)              │
└────────┬─────────────────────────────────────────────────┘
         │ 12 threads, direct mmap pointer (C) / bulk copy (Java)
         ▼
┌──────────────────────────────────────────────────────────┐
│  Parse: manual ISO timestamp, hardcoded sensor_XXXX,     │
│         all-integer scaled values (×100), no newline scan │
│         → TumblingState[minute][sensor] per thread        │
└────────┬─────────────────────────────────────────────────┘
         │ 12 threads, sensor-range parallelism
         ▼
┌──────────────────────────────────────────────────────────┐
│  Merge: direct pointer move (first partition) or merge   │
└────────┬─────────────────────────────────────────────────┘
         │ 12 threads, minute-range parallelism
         ▼
┌──────────────────────────────────────────────────────────┐
│  Emit: sliding (inline ASM quickselect p50, max scan p99)│
│        tumbling (count/sum/min/max/avg as integers)      │
│        → direct byte buffers, digit-pair tables          │
└────────┬─────────────────────────────────────────────────┘
         │
         ▼
┌──────────────────────────────────────────────────────────┐
│  Output: writev() scatter-gather (C) / fd write (Java)   │
└──────────────────────────────────────────────────────────┘
```

C-specific: inline ASM quickselect, arena allocator, CPU pinning, readahead, __builtin_prefetch.
Java-specific: C2-only JIT, MappedByteBuffer, no GC pressure path.

## What is autoresearch?

[autoresearch](https://github.com/sderosiaux/claudecode-autoresearch) is a Claude Code plugin that runs autonomous experiment loops. It tries ideas, benchmarks them, keeps improvements, discards regressions, and never stops. Each experiment is a git commit with the measured metric in the commit message.

## Files

| File | Purpose |
|------|---------|
| `src/StreamingAggregator.java` | Optimized Java engine (~518 lines) |
| `src/streaming_aggregator.c` | Optimized C engine (~660 lines, inline ASM) |
| `Makefile` | C build (gcc -O3 -march=native -pthread) |
| `src/DataGenerator.java` | Generates deterministic test data |
| `src/BatchValidator.java` | Correctness oracle (naive but correct) |
| `jvm.opts` | JVM flags (C2-only, loop compilation) |
| `autoresearch.sh` | Java benchmark harness |
| `autoresearch-c.checks.sh` | C correctness validation |
| `autoresearch.ideas.md` | ~150 tried experiments, categorized |
| `autoresearch.jsonl` | Full experiment log (228 experiments) |
