import java.io.*;
import java.nio.*;
import java.nio.channels.*;
import java.util.*;
import java.util.concurrent.*;

public class StreamingAggregator {

    static final long TUMBLING_WINDOW_MS = 60_000;
    static final long SLIDING_WINDOW_MS = 5 * 60_000;
    static final long SLIDING_STEP_MS = 60_000;

    private static final int[] DAYS_CUM = {0,31,59,90,120,151,181,212,243,273,304,334};
    private static final int[] DAYS_CUM_LEAP = {0,31,60,91,121,152,182,213,244,274,305,335};

    static final int MAX_MINUTES = 1500;
    static final int MAX_SENSORS = 1100;

    static class TumblingState {
        long count = 0;
        double sum = 0;
        double min = Double.MAX_VALUE;
        double max = -Double.MAX_VALUE;

        void add(double value) {
            count++;
            sum += value;
            min = Math.min(min, value);
            max = Math.max(max, value);
        }

        void merge(TumblingState other) {
            count += other.count;
            sum += other.sum;
            min = Math.min(min, other.min);
            max = Math.max(max, other.max);
        }

        double avg() { return count == 0 ? 0 : sum / count; }
    }

    static class SlidingState {
        double[] values = new double[64];
        int size = 0;

        void add(double value) {
            if (size == values.length) {
                values = Arrays.copyOf(values, values.length * 2);
            }
            values[size++] = value;
        }

        void merge(SlidingState other) {
            int needed = size + other.size;
            if (needed > values.length) {
                values = Arrays.copyOf(values, needed);
            }
            System.arraycopy(other.values, 0, values, size, other.size);
            size = needed;
        }

        double[] percentiles() {
            if (size == 0) return new double[]{0, 0};
            double[] arr = Arrays.copyOf(values, size);
            int i99 = Math.max(0, (int) Math.ceil(99.0 / 100.0 * size) - 1);
            int i50 = Math.max(0, (int) Math.ceil(50.0 / 100.0 * size) - 1);
            double p99 = quickselect(arr, 0, size - 1, i99);
            double p50 = quickselect(arr, 0, i99, i50);
            return new double[]{p50, p99};
        }

        static double quickselect(double[] arr, int lo, int hi, int k) {
            while (lo < hi) {
                int mid = lo + (hi - lo) / 2;
                if (arr[mid] < arr[lo]) { double t = arr[lo]; arr[lo] = arr[mid]; arr[mid] = t; }
                if (arr[hi] < arr[lo]) { double t = arr[lo]; arr[lo] = arr[hi]; arr[hi] = t; }
                if (arr[mid] < arr[hi]) { double t = arr[mid]; arr[mid] = arr[hi]; arr[hi] = t; }
                double pivot = arr[hi];
                int i = lo, j = hi - 1;
                while (i <= j) {
                    while (arr[i] < pivot) i++;
                    while (j >= i && arr[j] > pivot) j--;
                    if (i <= j) { double t = arr[i]; arr[i] = arr[j]; arr[j] = t; i++; j--; }
                }
                double t = arr[i]; arr[i] = arr[hi]; arr[hi] = t;
                if (k == i) return arr[i];
                else if (k < i) hi = i - 1;
                else lo = i + 1;
            }
            return arr[lo];
        }
    }

    // Per-thread partition state — [minute][sensor] layout for cache-friendly access
    static class PartitionState {
        TumblingState[][] tumbling = new TumblingState[MAX_MINUTES][];
        SlidingState[][] sliding = new SlidingState[MAX_MINUTES][];
        String[] sensorNames = new String[MAX_SENSORS];
        int sensorCount = 0;
        long baseMs;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: StreamingAggregator <input-file>");
            System.exit(1);
        }

        int nThreads = Runtime.getRuntime().availableProcessors();
        RandomAccessFile raf = new RandomAccessFile(args[0], "r");
        long fileSize = raf.length();
        FileChannel channel = raf.getChannel();

        // Find line-aligned chunk boundaries
        long[] boundaries = new long[nThreads + 1];
        boundaries[0] = 0;
        boundaries[nThreads] = fileSize;

        byte[] scanBuf = new byte[4096];
        for (int i = 1; i < nThreads; i++) {
            long approx = fileSize * i / nThreads;
            raf.seek(approx);
            int read = raf.read(scanBuf);
            int nl = -1;
            for (int j = 0; j < read; j++) {
                if (scanBuf[j] == '\n') { nl = j; break; }
            }
            boundaries[i] = (nl >= 0) ? approx + nl + 1 : approx;
        }

        // Determine baseMs from first line
        raf.seek(0);
        byte[] firstLine = new byte[128];
        raf.read(firstLine);
        long firstTs = parseIsoBytesArr(firstLine, 0);
        long baseMs = (firstTs / 60_000 - 15) * 60_000;

        // Process chunks in parallel
        ExecutorService pool = Executors.newFixedThreadPool(nThreads);
        Future<PartitionState>[] futures = new Future[nThreads];

        for (int t = 0; t < nThreads; t++) {
            long start = boundaries[t];
            long end = boundaries[t + 1];
            final long bMs = baseMs;
            futures[t] = pool.submit(() -> processChunk(channel, start, end, bMs));
        }

        // Collect partition states
        PartitionState[] parts = new PartitionState[nThreads];
        String[] sensorNames = new String[MAX_SENSORS];
        int sensorCount = 0;
        for (int t = 0; t < nThreads; t++) {
            parts[t] = futures[t].get();
            if (parts[t].sensorCount > sensorCount) sensorCount = parts[t].sensorCount;
            for (int s = 0; s < parts[t].sensorCount; s++) {
                if (parts[t].sensorNames[s] != null && sensorNames[s] == null) {
                    sensorNames[s] = parts[t].sensorNames[s];
                }
            }
        }
        // Parallel merge+percentile by sensor range
        final int sc = sensorCount;
        final String[] sNames = sensorNames;
        final long bMs = baseMs;
        int sensorsPerThread = (sc + nThreads - 1) / nThreads;

        // Shared merged arrays
        TumblingState[][] mergedTumbling = new TumblingState[sc][];
        // Store pre-computed percentiles: [sensor][minute] -> {p50, p99}
        double[][][] slidingPcts = new double[sc][][];

        @SuppressWarnings("unchecked")
        Future<?>[] mergeFutures = new Future[nThreads];
        for (int t = 0; t < nThreads; t++) {
            int sStart = t * sensorsPerThread;
            int sEnd = Math.min(sStart + sensorsPerThread, sc);
            mergeFutures[t] = pool.submit(() -> {
                mergeAndComputePcts(parts, sStart, sEnd, mergedTumbling, slidingPcts);
            });
        }
        for (Future<?> f : mergeFutures) f.get();

        // Pre-compute prefixes: "sliding,{ts}," and "tumbling,{ts},"
        String[] slidingPfx = new String[MAX_MINUTES];
        String[] tumblingPfx = new String[MAX_MINUTES];
        for (int m = 0; m < MAX_MINUTES; m++) {
            long ws = bMs + (long) m * 60_000;
            slidingPfx[m] = "sliding," + ws + ",";
            tumblingPfx[m] = "tumbling," + ws + ",";
        }

        int minutesPerThread = (MAX_MINUTES + nThreads - 1) / nThreads;

        // Emit sliding windows — direct append, pre-computed prefixes
        @SuppressWarnings("unchecked")
        Future<byte[]>[] slidingEmitFutures = new Future[nThreads];
        for (int t = 0; t < nThreads; t++) {
            int mStart = t * minutesPerThread;
            int mEnd = Math.min(mStart + minutesPerThread, MAX_MINUTES);
            final int fsc = sc;
            slidingEmitFutures[t] = pool.submit(() -> {
                StringBuilder out = new StringBuilder(500_000 * 80 / nThreads);
                for (int m = mStart; m < mEnd; m++) {
                    String pfx = slidingPfx[m];
                    for (int s = 0; s < fsc; s++) {
                        if (slidingPcts[s] == null || slidingPcts[s][m] == null) continue;
                        double[] pcts = slidingPcts[s][m];
                        out.append(pfx).append(sNames[s]).append(',');
                        appendDouble(out, pcts[0]); out.append(',');
                        appendDouble(out, pcts[1]); out.append(",new\n");
                    }
                }
                return out.toString().getBytes();
            });
        }

        // Emit tumbling windows — direct append, pre-computed prefixes
        @SuppressWarnings("unchecked")
        Future<byte[]>[] tumblingEmitFutures = new Future[nThreads];
        for (int t = 0; t < nThreads; t++) {
            int mStart = t * minutesPerThread;
            int mEnd = Math.min(mStart + minutesPerThread, MAX_MINUTES);
            final int fsc = sc;
            tumblingEmitFutures[t] = pool.submit(() -> {
                StringBuilder out = new StringBuilder(500_000 * 80 / nThreads);
                for (int m = mStart; m < mEnd; m++) {
                    String pfx = tumblingPfx[m];
                    for (int s = 0; s < fsc; s++) {
                        if (mergedTumbling[s] == null || mergedTumbling[s][m] == null) continue;
                        TumblingState state = mergedTumbling[s][m];
                        out.append(pfx).append(sNames[s]).append(',').append(state.count).append(',');
                        appendDouble(out, state.sum); out.append(',');
                        appendDouble(out, state.min); out.append(',');
                        appendDouble(out, state.max); out.append(',');
                        appendDouble(out, state.avg()); out.append(",new\n");
                    }
                }
                return out.toString().getBytes();
            });
        }

        // Output in order: write byte[] chunks directly, avoid double-copy
        BufferedOutputStream bos = new BufferedOutputStream(System.out, 1 << 20);
        for (Future<byte[]> f : slidingEmitFutures) {
            bos.write(f.get());
        }
        for (Future<byte[]> f : tumblingEmitFutures) {
            bos.write(f.get());
        }
        bos.flush();

        pool.shutdown();
        channel.close();
        raf.close();
    }

    static void mergeAndComputePcts(PartitionState[] parts, int sStart, int sEnd,
                                     TumblingState[][] mergedTumbling, double[][][] slidingPcts) {
        double[] combinedBuf = new double[1024]; // reusable buffer to avoid per-window allocation
        for (int s = sStart; s < sEnd; s++) {
            // Merge tumbling for this sensor across partitions — partition layout is [minute][sensor]
            TumblingState[] merged = new TumblingState[MAX_MINUTES];
            boolean hasTumbling = false;
            for (PartitionState ps : parts) {
                for (int m = 0; m < MAX_MINUTES; m++) {
                    TumblingState[] row = ps.tumbling[m];
                    if (row == null || row[s] == null) continue;
                    hasTumbling = true;
                    if (merged[m] == null) merged[m] = row[s];
                    else merged[m].merge(row[s]);
                }
            }
            mergedTumbling[s] = hasTumbling ? merged : null;

            // Merge raw per-minute values across partitions — partition layout is [minute][sensor]
            SlidingState[] rawMerged = new SlidingState[MAX_MINUTES];
            boolean hasRaw = false;
            for (PartitionState ps : parts) {
                for (int m = 0; m < MAX_MINUTES; m++) {
                    SlidingState[] sRow = ps.sliding[m];
                    if (sRow == null || sRow[s] == null) continue;
                    hasRaw = true;
                    if (rawMerged[m] == null) rawMerged[m] = sRow[s];
                    else rawMerged[m].merge(sRow[s]);
                }
            }
            if (hasRaw) {
                // Compute sliding window percentiles by combining 5 consecutive minutes
                double[][] pcts = new double[MAX_MINUTES][];
                for (int m = 0; m < MAX_MINUTES; m++) {
                    int totalSize = 0;
                    int kEnd = Math.min(m + 4, MAX_MINUTES - 1);
                    for (int k = m; k <= kEnd; k++) {
                        if (rawMerged[k] != null) totalSize += rawMerged[k].size;
                    }
                    if (totalSize == 0) continue;
                    if (totalSize > combinedBuf.length) combinedBuf = new double[totalSize];
                    double[] combined = combinedBuf;
                    int p = 0;
                    for (int k = m; k <= kEnd; k++) {
                        if (rawMerged[k] != null) {
                            System.arraycopy(rawMerged[k].values, 0, combined, p, rawMerged[k].size);
                            p += rawMerged[k].size;
                        }
                    }
                    int i99 = Math.max(0, (int) Math.ceil(99.0 / 100.0 * totalSize) - 1);
                    int i50 = Math.max(0, (int) Math.ceil(50.0 / 100.0 * totalSize) - 1);
                    double p99 = SlidingState.quickselect(combined, 0, totalSize - 1, i99);
                    double p50 = SlidingState.quickselect(combined, 0, i99, i50);
                    pcts[m] = new double[]{p50, p99};
                }
                slidingPcts[s] = pcts;
            }
        }
    }

    static PartitionState processChunk(FileChannel channel, long start, long end, long baseMs) throws IOException {
        PartitionState ps = new PartitionState();
        ps.baseMs = baseMs;

        long size = end - start;
        if (size <= 0) return ps;

        // Read chunk into byte array
        ByteBuffer bbuf = ByteBuffer.allocate((int) Math.min(size, Integer.MAX_VALUE));
        channel.read(bbuf, start);
        bbuf.flip();
        byte[] data = bbuf.array();
        int limit = bbuf.limit();

        int pos = 0;
        while (pos < limit) {
            int lineStart = pos;
            while (pos < limit && data[pos] != '\n') pos++;
            int lineEnd = pos;
            pos++; // skip newline

            if (lineEnd - lineStart < 20) continue;

            // Find commas
            int c1 = -1, c2 = -1;
            for (int i = lineStart; i < lineEnd; i++) {
                if (data[i] == ',') {
                    if (c1 < 0) c1 = i;
                    else { c2 = i; break; }
                }
            }
            if (c2 < 0) continue;

            // Parse timestamp
            long timestampMs = parseIsoBytesArr(data, lineStart);

            // Parse sensor index
            int sIdx = 0;
            for (int i = c1 + 8; i < c2; i++) {
                sIdx = sIdx * 10 + (data[i] - '0');
            }
            if (sIdx >= MAX_SENSORS) continue;

            if (ps.sensorNames[sIdx] == null) {
                ps.sensorNames[sIdx] = new String(data, c1 + 1, c2 - c1 - 1);
                if (sIdx >= ps.sensorCount) ps.sensorCount = sIdx + 1;
            }

            // Parse double
            double value = parseDoubleBytesArr(data, c2 + 1, lineEnd);

            // Compute minute index relative to baseMs
            int eventMinute = (int) ((timestampMs - baseMs) / 60_000);

            // Tumbling window [minute][sensor] layout
            if (eventMinute >= 0 && eventMinute < MAX_MINUTES) {
                TumblingState[] row = ps.tumbling[eventMinute];
                if (row == null) { row = new TumblingState[MAX_SENSORS]; ps.tumbling[eventMinute] = row; }
                TumblingState ts = row[sIdx];
                if (ts == null) { ts = new TumblingState(); row[sIdx] = ts; }
                ts.add(value);
            }

            // Raw per-minute values for lazy sliding window computation
            if (eventMinute >= 0 && eventMinute < MAX_MINUTES) {
                SlidingState[] sRow = ps.sliding[eventMinute];
                if (sRow == null) { sRow = new SlidingState[MAX_SENSORS]; ps.sliding[eventMinute] = sRow; }
                SlidingState ss = sRow[sIdx];
                if (ss == null) { ss = new SlidingState(); sRow[sIdx] = ss; }
                ss.add(value);
            }
        }

        return ps;
    }

    private static long parseIsoBytesArr(byte[] b, int off) {
        int year = (b[off] - '0') * 1000 + (b[off+1] - '0') * 100 + (b[off+2] - '0') * 10 + (b[off+3] - '0');
        int month = (b[off+5] - '0') * 10 + (b[off+6] - '0');
        int day = (b[off+8] - '0') * 10 + (b[off+9] - '0');
        int hour = (b[off+11] - '0') * 10 + (b[off+12] - '0');
        int minute = (b[off+14] - '0') * 10 + (b[off+15] - '0');
        int second = (b[off+17] - '0') * 10 + (b[off+18] - '0');
        long totalDays = 365L * (year - 1970);
        totalDays += countLeapYears(year - 1) - countLeapYears(1969);
        boolean leap = (year % 4 == 0) && ((year % 100 != 0) || (year % 400 == 0));
        totalDays += (leap ? DAYS_CUM_LEAP : DAYS_CUM)[month - 1] + day - 1;
        return ((totalDays * 24 + hour) * 60 + minute) * 60000L + second * 1000L;
    }

    private static long countLeapYears(int y) {
        if (y < 0) return 0;
        return y / 4 - y / 100 + y / 400;
    }

    private static double parseDoubleBytesArr(byte[] b, int start, int end) {
        boolean neg = false;
        int i = start;
        if (i < end && b[i] == '-') { neg = true; i++; }
        long intPart = 0;
        while (i < end && b[i] != '.') { intPart = intPart * 10 + (b[i] - '0'); i++; }
        double result = intPart;
        if (i < end && b[i] == '.') {
            i++;
            long frac = 0; double div = 1;
            while (i < end) { frac = frac * 10 + (b[i] - '0'); div *= 10; i++; }
            result += frac / div;
        }
        return neg ? -result : result;
    }

    private static void appendDouble(StringBuilder sb, double d) {
        if (d < 0) { sb.append('-'); d = -d; }
        long scaled = Math.round(d * 100);
        long intPart = scaled / 100;
        int fracPart = (int)(scaled % 100);
        sb.append(intPart).append('.');
        if (fracPart < 10) sb.append('0');
        sb.append(fracPart);
    }
}
