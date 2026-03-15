# Deferred Ideas

## High Priority (based on profiling — quickselect 20%, sliding emit 19%, processChunk 13%)
- Hardcode c2 = c1 + 12 — all sensors are "sensor_XXXX" (11 chars), eliminates 3 failed branch checks per event
- Hardcode sensor index parse to exactly 4 digits — eliminate loop overhead
- SWAR (8-byte batch reads via UNSAFE.getLong) for timestamp/sensor parsing
- Skip sensor name copy for already-seen sensors — use per-partition boolean[] seen array
- Pipeline: overlap output write with sliding emit computation (start writing tumbling while sliding still running)
- Pre-allocate TumblingState pool to reduce GC (2.7% profile)
- Reduce merge temp array from MAX_MINUTES to [globalMin..globalMax] range
- Introselect: hybrid quickselect+median-of-medians for guaranteed O(n) worst case

## Medium Priority
- Avoid values[] dynamic resizing in add() — pre-size to expected ~7 values/minute/sensor
- Read-ahead hint for mmap (madvise via Unsafe — MADV_SEQUENTIAL)
- Use long-based encoding for sensor names instead of String (avoid String allocation)
- Eliminate `new TumblingState[MAX_SENSORS]` per-minute allocation in processChunk — lazy sparse structure
- Float instead of double for values[] — halve memory, better cache utilization (but verify precision)
- Two-pass quickselect optimization: find p99 first, then p50 within [0..i99] (already doing this)

## Lower Priority
- JVM flags: -XX:+UseParallelGC, -XX:+AlwaysPreTouch (currently using G1GC defaults)
- NUMA-aware chunk assignment
- Custom percentile algorithm: selection networks for small N
- Vector API (Java 21 preview) for batch numeric operations
