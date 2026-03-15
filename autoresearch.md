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

## How to Run
`./autoresearch.sh` outputs `METRIC name=number` lines.
Log results with `${CLAUDE_PLUGIN_ROOT}/scripts/log-experiment.sh`.

## Profiling Notes
### Profile at 9.15M ev/s (experiment 108, async-profiler):
```
quickselect:        20.3% (137 samples)
sliding emit:       18.8% (134 samples) — includes percentile computation
processChunk:       13.3% (94 samples)
tumbling emit:      10.2% (68 samples)
mergePartitions:     8.4% (58 samples)
GC:                  2.7% (17 samples)
parseDoubleUnsafe:   2.4% (17 samples)
TumblingState.add:   2.1% (15 samples)
arraycopy:           1.4% (10 samples)
avg():               1.4% (10 samples)
```

### Data characteristics:
- 1440 active minutes, ~7 events/minute/sensor
- Sensor names: zero-padded 4 digits ("sensor_0000" to "sensor_0999"), all 11 chars
- All sensors: c2 is ALWAYS c1 + 12 (sensor name always 11 chars)
- Sliding windows average ~30 values (5 min x ~6 ev/min)
- Output size: ~166MB, File size: ~388MB
- 12 mmap'd chunks (~32MB each), parsed directly via Unsafe

## Landscape Model
Current: 9.15M ev/s (158x baseline). Architecture: parallel mmap+Unsafe parse -> parallel sensor-range merge -> parallel emit (sliding+tumbling separate) -> sequential output.

**Exhausted dimensions**: parsing micro-opts (JIT handles), emit strategy, sort elimination, merge parallelism model (sensor-range wins over minute-range), output method/buffer sizes, chunk count, fused emit (too large working set), median-of-3 pivot (optimal), values[] initial sizing.

**Hot path**: quickselect (20%) + sliding emit (19%) = 39% of runtime. processChunk (13%) is third.

**Gradient points toward**: reducing per-event branch count in parse, reducing quickselect calls, reducing merge overhead, pipelining output with compute.

## What's Been Tried (108 experiments, ~22 kept)

### Major wins (chronological):
| # | Experiment | Metric | Delta |
|---|-----------|--------|-------|
| 0 | Baseline | 57,823 | - |
| 1-11 | Parsing/data structure/emit opts | 1,857,700 | +32x |
| 13 | Multi-threaded parallel chunks | 2,683,123 | +44% |
| 19 | Parallel merge+emit by sensor range | 3,480,682 | +30% |
| 32 | Sorted-order emit (no sort) | 3,954,132 | +14% |
| 34 | Parallel emit by minute range | 4,327,131 | +9% |
| 39 | Parallel byte[] conversion in emit | 4,928,536 | +9% |
| 49 | Bundled emit: prefixes + direct append | 5,010,020 | +2% |
| ~55-80 | Series of micro-opts (lazy sliding, byte[] emit, merged bounds, branchless c2, transpose, single-pass merge, unified state, int percentile indices, zero-copy buffers) | 6,720,430 | +34% |
| 88 | mmap file reading | 7,668,711 | +5% |
| 91 | Skip 31 bytes before newline scan | 7,898,894 | +3% |
| 93 | Fast minute computation (precompute day offset) | 8,244,023 | +4% |
| 99 | Inline sliding percentiles (eliminate slidingPcts) | 8,536,103 | +4% |
| 101 | Direct mmap via Unsafe (eliminate 384MB heap copy) | 9,154,010 | +7% |

### Key architectural constraints:
- Sensor-range parallelism for merge is CRITICAL (minute-range: -10%)
- Separate sliding and tumbling emit is CRITICAL (fused: -13%)
- Quickselect with median-of-3 is optimal (simpler pivot: -3%, Arrays.sort: -5%)
- TumblingState MUST fit in one 64-byte cache line (no new fields)

## Tabu
- Minute-range merge parallelism (-10%)
- Fused sliding+tumbling emit (-13%)
- Arrays.sort replacing quickselect (-5%)
- Remove median-of-3 pivot (-3%)
- cachedAvg field (cache line overflow, -5%)
- Int-scaled raw values (per-event Math.round overhead)
- Direct merge into output layout (cache thrashing)
- Smaller initial values array [4] (resize copies offset savings)
- p99=max special case (branch overhead)
- Inline newline as direct byte writes (noise)
- Unroll 5-minute sliding k-loop (JIT already optimizes)
