# Deferred Ideas

## High Priority — ALL TRIED
- ~~Skip sensor name String allocation~~ — **KEPT (+4.7%)**, generate names from indices
- ~~Introselect~~ — FAILED (-3%): depth check overhead per iteration, fallback never triggers for N=30
- ~~Incremental sliding window~~ — FAILED (-20%): copy+reinsert overhead exceeds full gather+quickselect
- ~~4KB block buffer for parse~~ — FAILED: MBB.get() overhead reduced by other opts, block management overhead
- ~~Counting sort for percentiles~~ — FAILED (crash): N=30 too small, 6000+ bucket histogram dwarfs quickselect

## Medium Priority — ALL TRIED
- ~~Long-based sensor encoding~~ — MOOT: sensor name removed from hot path by skip-alloc optimization
- ~~NIO FileChannel scatter-gather writev~~ — FAILED (-8.6%): ByteBuffer.wrap + dispatch overhead exceeds syscall savings
- ~~Reduce [no_Java_frame] overhead~~ — EXHAUSTED via 8 sub-experiments:
  - UseAdaptiveSizePolicy=false (neutral)
  - ReservedCodeCacheSize=512m (neutral)
  - mbb.load() pre-fault pages (neutral, file in page cache)
  - Fixed heap -Xms1g -Xmx1g (neutral, reduces variance only)
  - CICompilerCount=4 (-7%, steals CPU from compute)
  - -XX:-BackgroundCompilation (-29%, blocks threads during C2 compilation)
  - MaxGCPauseMillis=5 (-5%, more frequent GC cycles)
  - SerialGC (-25%, STW blocks all 12 threads)

## Lower Priority — ALL TRIED OR BLOCKED
- ~~NUMA-aware chunk assignment~~ — N/A: single socket AMD Ryzen 5 3600
- ~~Selection networks for small N~~ — FAILED (-20%): bitonic sort O(N log²N)=240 comparators vs quickselect O(N)≈95
- ~~Vector API~~ — BLOCKED: requires --add-modules jdk.incubator.vector for javac, autoresearch.sh not in scope
- ~~Lock-free CAS on TumblingState slots~~ — N/A: no merge contention (sensor-range parallelism)

## Tried and Kept (do not retry — already applied)
- Batch MBB reads: bulk copy 48 bytes into stack-local byte[] (+1.8%, A/B +5.5%)
- int[] values for percentile quickselect — halve memory (+2.4%)
- All-integer TumblingState — scaledSum/scaledMin/scaledMax, no FP in hot path (+0.4%, A/B +7%)
- Digit-pair lookup tables + integer avg + direct FileOutputStream(fd) output (+2.5%, A/B)
- Branchless Lomuto partition in quickselect — eliminate branch mispredictions (A/B +8%)
- Specialized double parser — dispatch by dot position (+2.0%)
- Eliminate newline scan — compute line length from value format (+5.1%)
- C2-only JIT + AlwaysCompileLoopMethods (+4.7%)
- mmap file reading + single file-wide mmap shared across threads (+5.3%)
- MappedByteBuffer zero-copy parsing (no Unsafe) — MBB bounds checks cheaper than Unsafe scope management
- Inline sliding percentiles during emit — eliminate intermediate allocation (+3.5%)
- Integer percentile indices + zero-copy emit buffers (+8.4%)
- Merge SlidingState into TumblingState — unified state, one cache line (+4.2%)
- Direct byte[] emit — eliminate StringBuilder→String→byte[] triple copy (+8.5%)
- Parallel byte[] conversion in emit threads (+9.4%)
- Multi-threaded parallel chunk processing with byte[] parsing
- Array-indexed [minute][sensor] layout replacing HashMaps
- Direct sensor index from digits, eliminates HashMap lookup
- Skip sensor name String allocation — generate from indices (+4.7%)

## Tried and Failed (do not retry)

### Quickselect / Percentiles
- Arrays.sort replacing quickselect (-5% to -14%): full sort O(n log n) > quickselect O(n)
- Insertion sort for small sliding windows N≤32 (-3% to -19%): O(N²) branch-heavy
- Pre-sort values during merge (-25%): O(n log n) sort overhead per minute far exceeds savings
- Sorted SlidingState with O(n+m) merge (no improvement over quickselect)
- Pre-sorted sliding + merge-sort merge (-13%): sort+alloc overhead exceeds quickselect savings
- p99 as O(n) max scan (-3% to -6%): p50 loses partial ordering benefit from p99 quickselect
- Reverse percentile order p50 first (neutral): range narrowing offset by lost partial ordering
- Remove median-of-3 pivot (-3.3%): worse pivot quality increases iteration count
- Bitonic sort network (-20%): O(N log²N)=240 comparators vs O(N)≈95 quickselect comparisons
- Introselect hybrid quickselect+median-of-medians (-3%): depth check overhead, fallback never triggers
- Counting sort histogram for N=30 (crash): 6000+ buckets for N=30 is 100× more work than quickselect

### Memory / Data Structures
- Adding fields to TumblingState (-5%): pushes object past 64-byte cache line boundary
- TumblingState object size reduction 56→40 bytes (neutral): hot path on int[] arrays, not fields
- Object compaction (-14.5%): 864K TumblingState allocations + GC outweigh cache gains
- Smaller initial values array [4] (-8%): resize copies offset allocation savings
- SlidingState initial array 64→16/8 (-5%): high GC variance from resizes
- double[8] sliding arrays + in-place percentiles (-10%): too many resizes
- Flat slidingP50/P99 arrays with NaN sentinel (-5%): Arrays.fill + isNaN overhead
- Reuse per-sensor temp arrays with Arrays.fill (-7%): fill slower than TLAB alloc
- Pre-allocate sensor rows + hoist sRow (no improvement, memory waste)
- SoA tumbling (primitive arrays): Arrays.fill init overhead
- MAX_SENSORS=1000 exact count (-0.7%): 800 bytes smaller per row, no measurable cache benefit

### Parallelism / Threading
- Minute-range merge parallelism (-10%): cache thrashing on shared mergedTumbling array
- ForkJoinPool replacing FixedThreadPool (-11%): work-stealing overhead exceeds load balancing
- ForkJoinPool.commonPool() (neutral): nCPU-1 threads offset startup savings
- Virtual threads (-4%): scheduling overhead for CPU-bound work
- 6 emit threads instead of 12 (-11%): emit is CPU-bound, less parallelism hurts
- 6 parse threads (-9%): insufficient parallelism
- nThreads/2 chunks (no improvement): reduced parse parallelism
- Two-phase merge minute-parallel raw + sensor-parallel pcts (-8%): barrier overhead
- Single-partition processing (-38%): sequential parsing bottleneck

### Emit / Output
- Fused sliding+tumbling emit (-13%): working set too large for L1/L2 cache
- Combined sliding+tumbling emit per task (-7%): larger working set hurts cache
- Sensor-major emit ordering (-6.4%): cross-core sharing on mergedTumbling row arrays
- Sliding emit loop reorder sensor-outer, minute-inner (-2%): row spatial locality regression
- Write tumbling output first (-4.5%): output I/O evicts cache lines sliding still reads
- Inline suffix + sensor name with comma (regression 3+/5-): replacing small arraycopy with byte writes
- Inline newline suffix as direct byte writes (neutral): arraycopy overhead for 5 bytes negligible
- Concat emit chunks before writing (-14%): 166MB extra allocation GC pressure
- 8MB BufferedOutputStream (-5%): cache pressure
- 4MB output buffer (-5%): larger buffer evicts cache
- Bypass System.out with FileDescriptor.out (-8%): pipe buffer contention
- FileChannel output (no improvement)
- NIO FileChannel scatter-gather writev (-8.6%): ByteBuffer.wrap + GatheringByteChannel dispatch overhead
- Sensor-range sliding with contiguous value tape (-3%): tape building overhead offsets locality

### I/O / Parsing
- MemorySegment API (-18%): segment validity + scope checks more overhead than MBB bounds checks
- Batch getLong/getInt reads (-2.5%): C2 already eliminates bounds checks, shift/mask ALU hurts
- Byte[] copy from mmap (-10%): 384MB heap allocation + copyMemory0 + GC overhead
- pread-based parallel reading (-12%): per-chunk syscalls slower than single mmap
- Single bulk read + parallel parse (-23%): 388MB allocation GC pressure
- Hybrid bulk prefix read (-2%): mbb.get(pos,buf,0,33) bulk overhead offsets savings
- Per-line bulk read into local byte[48] (-1.5%): earlier version, less effective than current 48B approach
- SWAR newline scan on MBB (negligible): only ~7 bytes to scan after skip-31
- Memory-mapped I/O with byte-level parsing (no improvement vs BufferedReader)
- Independent file handles per thread (no improvement)

### JVM Tuning
- Aggressive JIT inlining MaxInlineLevel/InlineSmallCode/FreqInlineSize (-1.6%): code bloat hurts icache
- LoopUnrollLimit=120 (noise): no measurable effect
- ParallelGC (-15%): more stop-the-world pauses than G1GC
- EpsilonGC (-12.3%): memory fragmentation without compaction degrades locality
- ZGC (slower): ZGC overhead for this workload
- ShenandoahGC (-8.3%): concurrent barrier overhead exceeds GC pause reduction
- GraalVM CE/Oracle 21 (-20-30%): Graal JIT doesn't optimize MBB.get() as well as HotSpot C2
- AlwaysPreTouch (no effect)
- UseTransparentHugePages (no effect, THP already enabled system-wide)
- UseAdaptiveSizePolicy=false (neutral)
- ReservedCodeCacheSize=512m InitialCodeCacheSize=512m (neutral)
- Fixed heap -Xms1g -Xmx1g (neutral, reduces variance only)
- CICompilerCount=4 (-7%): extra compiler threads steal CPU
- BackgroundCompilation=false (-29%): synchronous C2 blocks all threads
- MaxGCPauseMillis=5 (-5%): more frequent GC cycles
- SerialGC (-25%): STW blocks all 12 compute threads
- CompileThreshold tuning (previous session, no improvement)

### Miscellaneous
- Explicit min/max branches (-6.5%): Math.min/max compiles to branchless FCMOV/MAXSD on x86-64
- Software prefetch (-6.4%): volatile fence + extra loop overhead, OoO engine already prefetches
- Panama FFI for madvise (-7.7%): FFI init overhead + THP defrag stalls
- 32-bit division for minute index (-5%): JIT already uses multiply-by-reciprocal
- Fused merge+sliding (-19%): sliding reads ALL minutes per sensor, poor cache locality
- Direct merge into output layout (-5%): worse cache locality from (partition,minute,sensor) loop order
- Pre-compute avg + inline newline + reuse merged[] (-5%): clearing loop overhead
- Cumulative size table for fast sliding window skip (-3%): 6MB array adds cache pressure
- Unroll 5-minute sliding k-loop (noise): JIT already optimizes the loop
- Hardcode c2=c1+12 + unrolled 4-digit sensor index (noise): JIT already predicts the pattern
- mbb.load() pre-fault pages (neutral): 388MB sequential read overhead, file already in page cache
