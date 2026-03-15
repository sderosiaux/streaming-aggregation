# Deferred Ideas

## High Priority
- Replace Instant.parse with manual ISO-8601 timestamp parser (avoid DateTimeFormatter overhead)
- Replace String.split(",") with indexOf-based CSV parsing
- Replace SlidingState ArrayList+sort with t-digest or quantile sketch (avoid O(n log n) per window)
- Larger BufferedReader buffer size (e.g., 1MB)
- Replace String.format with StringBuilder for output
- Pre-size HashMaps based on expected window count
- Avoid WindowKey record allocation (use primitive key encoding)

## Medium Priority
- Multi-threaded processing (partition by sensor, parallel streams)
- Memory-mapped file I/O (MappedByteBuffer)
- Custom hash map with open addressing
- Byte-level parsing (skip String allocation for sensor IDs — intern or use byte ranges)
- Reduce sliding window memory by using a sorted structure (TreeMap<Double,Integer>) for online percentiles
- JVM flags: -XX:+UseG1GC, -Xmx, -XX:+AlwaysPreTouch

## Lower Priority
- SIMD-friendly data layout (SoA vs AoS)
- Off-heap storage for sliding window values
- Reservoir sampling for approximate percentiles
- Batch window emission (reduce iterator overhead)
- Use char[] parsing instead of String
