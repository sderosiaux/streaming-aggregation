# Autoresearch: Streaming Aggregation Throughput

## Objective
Maximize throughput (events/sec) of `StreamingAggregator.java`. 10M timestamped CSV events -> tumbling + sliding windows. Java 21, no external deps.

## Metrics
- **Primary**: throughput (events/sec, higher is better)

## Files in Scope
- `src/StreamingAggregator.java` (must `git add -f` to commit)

## Off Limits
- DataGenerator.java, BatchValidator.java, autoresearch.sh, autoresearch.checks.sh

## Constraints
- `./autoresearch.checks.sh` must pass
- Java 21 stdlib only

## Profiling Notes
### Phase breakdown at 3.48M ev/s (current best):
- Chunk processing (parallel parse+assign): 18ms (1%)
- Merge+emit (parallel percentiles): 1471ms (65%)
- Sort+format: 595ms (27%)
- Output: 160ms (7%)

### Key: merge+emit dominates. Percentile quickselect across ~30K windows is the main cost.

## What's Been Tried (24 experiments)

| # | Experiment | Metric | Result |
|---|-----------|--------|--------|
| 0 | Baseline | 57,823 | baseline |
| 1 | indexOf+ISO parser+StringBuilder+double[] | 199,282 | KEEP (+244%) |
| 2 | Single sort for both p50/p99 | 227,588 | KEEP |
| 3 | 1MB buffer + sensor interning | 236,966 | KEEP |
| 4 | Quickselect O(n) | 244,953 | KEEP |
| 5 | Fast double parser alone | 240,853 | DISCARD |
| 6 | Compact key + fast double + separate sets | 270,233 | KEEP |
| 7 | Array-indexed windows | 328,558 | KEEP (+22%) |
| 8 | Bounded scan floor/ceiling | 357,935 | KEEP |
| 9 | Emit every 100K -> end-only | 1,155,401 | KEEP (+223%) |
| 10 | Emit only at end | 1,422,879 | KEEP |
| 11 | Direct sensor index (no HashMap) | 1,857,700 | KEEP |
| 12 | Mmap byte-level parsing | 1,851,508 | DISCARD |
| 13 | Multi-threaded parallel chunks + byte[] | 2,683,123 | KEEP (+44%) |
| 14 | Independent file handles per thread | 2,670,940 | DISCARD |
| 15 | Simplified sliding loop | 2,598,752 | DISCARD |
| 16 | Pre-allocate sensor rows | 2,588,661 | DISCARD |
| 17 | Fixed-offset parsing | 2,590,673 | DISCARD |
| 18 | Smaller SlidingState initial array | 2,551,671 | DISCARD |
| 19 | Parallel merge+emit by sensor range | 3,480,682 | KEEP (+30%) |
| 20 | parallelSort | 3,107,520 | DISCARD |
| 21 | BufferedWriter output | 2,847,380 | DISCARD |
| 22 | Sorted-order emit (no sort) | 2,983,293 | DISCARD |
| 23 | Arrays.sort in-place percentiles | 3,356,831 | DISCARD |

## Tabu
- Mmap byte-level: no gain over BufferedReader+byte[]
- Independent file handles: no gain (OS page cache already parallel)
- Fixed-offset parsing: JIT already optimizes comma scan
- Pre-allocate sensor rows: memory waste, no speed gain
- parallelSort: overhead for small arrays
- BufferedWriter output: slower than StringBuilder+print

## Landscape Model
Current: 3.48M ev/s (60x baseline). Architecture: parallel file chunk processing + parallel merge/emit.

**Gradient**: Merge+emit = 65% of time. Percentile computation dominates. Need to either (a) compute percentiles faster, (b) avoid percentile computation, or (c) restructure merge to reduce overhead.

**Promising unexplored**: JVM flags (-Xmx, GC tuning), reduce per-window overhead in merge, restructure to avoid String allocation in emit, use radix sort instead of comparison sort for final output.

**Exhausted**: parsing, I/O, data structure layout, emission strategy, basic parallelism.
