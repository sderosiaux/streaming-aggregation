# Streaming Aggregation: 57K to 11.4M events/sec

A Java streaming window aggregation engine, optimized from **57,823 ev/s to 11,441,647 ev/s** (198x improvement) in **167 autonomous experiments** using [autoresearch](https://github.com/sderosiaux/claudecode-autoresearch).

Every commit in this repo is an experiment. The commit messages document the technique, the measured throughput, and the delta from the previous best.

## The workload

Process 10M timestamped CSV sensor events through:
- **Tumbling windows** (1 min): count, sum, min, max, avg per sensor
- **Sliding windows** (5 min window, 1 min slide): p50, p99 per sensor
- 1,000 sensors, 24 hours of data, 5% late events

Single file, Java 21+, no external dependencies.

## Run it

```bash
# Generate 10M events + run benchmark
./autoresearch.sh

# Correctness check (diff against batch recomputation)
./autoresearch.checks.sh
```

Requires Java 21+.

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

## Key techniques

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
| **Emit** | Range-bounded iteration [gMin..gMax], direct merge (no temp array) | +2% |

## What didn't work

~125 experiments were discarded. Patterns that consistently lost:
- **Minute-range merge parallelism** (-10%): cache thrashing on shared mergedTumbling array
- **Fused sliding+tumbling emit** (-13%): working set too large for L1/L2 cache
- **Arrays.sort replacing quickselect** (-5%): full sort is O(n log n), quickselect is O(n)
- **Adding fields to TumblingState** (-5%): pushed object past 64-byte cache line boundary
- **MemorySegment API** (-18%): segment validity + scope checks MORE overhead than MBB bounds checks
- **Batch getLong/getInt reads** (-2.5%): C2 already eliminates bounds checks, extra shift/mask ALU hurts
- **Byte[] copy from mmap** (-10%): 384MB heap allocation + copyMemory0 + GC overhead
- **pread-based parallel reading** (-12%): per-chunk syscalls slower than single mmap
- **Aggressive JIT inlining** (-1.6%): code bloat hurts icache on C2-only
- **ParallelGC** (-15%): more stop-the-world pauses than G1GC for this workload
- **Insertion sort for percentiles** (-3%): conditional branches hurt branch prediction
- **Explicit min/max branches** (-6.5%): Math.min/max compiles to branchless FCMOV/MAXSD on x86-64
- **Sensor-major emit ordering** (-6.4%): cross-core sharing on mergedTumbling row arrays
- **Object compaction** (-14.5%): 864K TumblingState allocations + GC outweigh cache gains
- **EpsilonGC** (-12.3%): memory fragmentation without compaction degrades locality
- **Software prefetch** (-6.4%): volatile fence + extra loop overhead, OoO engine already prefetches
- **Panama FFI for madvise** (-7.7%): FFI init overhead + THP defrag stalls
- **ForkJoinPool** (-11%): work-stealing queue overhead exceeds load balancing benefit
- **p99 as O(n) max scan** (-3%): p50 loses partial ordering benefit from p99 quickselect
- **Inline suffix byte writes** (-2%): replacing 4-byte arraycopy with individual byte writes hurts pipeline

## Architecture

```
┌──────────────────────────────────────────────────────────┐
│  mmap file (388MB) → 12 chunks (~32MB each)              │
└────────┬─────────────────────────────────────────────────┘
         │ 12 threads, bulk copy 48B lines into stack-local byte[]
         ▼
┌──────────────────────────────────────────────────────────┐
│  Parse: manual ISO timestamp, hardcoded sensor_XXXX,     │
│         all-integer scaled values, no newline scan        │
│         → TumblingState[min][sensor]                      │
└────────┬─────────────────────────────────────────────────┘
         │ 12 threads, sensor-range parallelism
         ▼
┌──────────────────────────────────────────────────────────┐
│  Merge: direct into shared mergedTumbling (no temp array) │
└────────┬─────────────────────────────────────────────────┘
         │ 12 threads, minute-range parallelism
         ▼
┌──────────────────────────────────────────────────────────┐
│  Emit: sliding (branchless Lomuto quickselect p50/p99)   │
│        tumbling (count/sum/min/max/avg as integers)      │
│        → direct byte[] buffers, digit-pair tables        │
└────────┬─────────────────────────────────────────────────┘
         │ sequential
         ▼
┌──────────────────────────────────────────────────────────┐
│  Output: FileOutputStream(FileDescriptor.out), ~166MB    │
└──────────────────────────────────────────────────────────┘
```

## What is autoresearch?

[autoresearch](https://github.com/sderosiaux/claudecode-autoresearch) is a Claude Code plugin that runs autonomous experiment loops. It tries ideas, benchmarks them, keeps improvements, discards regressions, and never stops. Each experiment is a git commit with the measured metric in the commit message.

The `autoresearch.jsonl` file contains the full experiment log with metrics for every attempt (kept and discarded). The `autoresearch.md` file is the session document with profiling notes, landscape model, and tabu list.

## Files

| File | Purpose |
|------|---------|
| `src/StreamingAggregator.java` | The optimized engine (~518 lines) |
| `src/DataGenerator.java` | Generates deterministic test data |
| `src/BatchValidator.java` | Correctness oracle (naive but correct) |
| `jvm.opts` | JVM flags (C2-only, loop compilation) |
| `autoresearch.sh` | Benchmark harness |
| `autoresearch.checks.sh` | Correctness validation |
| `autoresearch.md` | Experiment session notes |
| `autoresearch.jsonl` | Full experiment log |
