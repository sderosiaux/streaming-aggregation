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

    // Unified state: tumbling aggregates + raw values for sliding percentiles
    static class TumblingState {
        long count = 0;
        double sum = 0;
        double min = Double.MAX_VALUE;
        double max = -Double.MAX_VALUE;
        double[] values = new double[8];
        int size = 0;

        void add(double value) {
            count++;
            sum += value;
            min = Math.min(min, value);
            max = Math.max(max, value);
            if (size == values.length) {
                values = Arrays.copyOf(values, values.length * 2);
            }
            values[size++] = value;
        }

        void merge(TumblingState other) {
            count += other.count;
            sum += other.sum;
            min = Math.min(min, other.min);
            max = Math.max(max, other.max);
            int needed = size + other.size;
            if (needed > values.length) {
                values = Arrays.copyOf(values, needed);
            }
            System.arraycopy(other.values, 0, values, size, other.size);
            size += other.size;
        }

        double avg() { return count == 0 ? 0 : sum / count; }

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
        String[] sensorNames = new String[MAX_SENSORS];
        int sensorCount = 0;
        long baseMs;
        int minMinute = MAX_MINUTES;
        int maxMinute = -1;
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

        // Shared merged arrays — [minute][sensor] layout for cache-friendly emit
        TumblingState[][] mergedTumbling = new TumblingState[MAX_MINUTES][];
        double[][][] slidingPcts = new double[MAX_MINUTES][][];
        for (int m = 0; m < MAX_MINUTES; m++) {
            mergedTumbling[m] = new TumblingState[sc];
            slidingPcts[m] = new double[sc][];
        }

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

        // Pre-compute prefixes as byte[] and sensor names as byte[]
        byte[][] slidingPfx = new byte[MAX_MINUTES][];
        byte[][] tumblingPfx = new byte[MAX_MINUTES][];
        for (int m = 0; m < MAX_MINUTES; m++) {
            long ws = bMs + (long) m * 60_000;
            slidingPfx[m] = ("sliding," + ws + ",").getBytes();
            tumblingPfx[m] = ("tumbling," + ws + ",").getBytes();
        }
        byte[][] sNameBytes = new byte[sc][];
        for (int s = 0; s < sc; s++) {
            sNameBytes[s] = sNames[s] != null ? sNames[s].getBytes() : new byte[0];
        }
        byte[] NEW_LINE = ",new\n".getBytes();

        int minutesPerThread = (MAX_MINUTES + nThreads - 1) / nThreads;

        // Shared emit buffers — avoid Arrays.copyOf trim at end of each task
        byte[][] slidingBufs = new byte[nThreads][];
        int[] slidingLens = new int[nThreads];
        byte[][] tumblingBufs = new byte[nThreads][];
        int[] tumblingLens = new int[nThreads];

        // Emit sliding windows — direct byte[] output
        @SuppressWarnings("unchecked")
        Future<?>[] slidingEmitFutures = new Future[nThreads];
        for (int t = 0; t < nThreads; t++) {
            int tt = t;
            int mStart = t * minutesPerThread;
            int mEnd = Math.min(mStart + minutesPerThread, MAX_MINUTES);
            final int fsc = sc;
            slidingEmitFutures[t] = pool.submit(() -> {
                byte[] buf = new byte[8 << 20];
                int pos = 0;
                for (int m = mStart; m < mEnd; m++) {
                    byte[] pfx = slidingPfx[m];
                    double[][] row = slidingPcts[m];
                    for (int s = 0; s < fsc; s++) {
                        double[] pcts = row[s];
                        if (pcts == null) continue;
                        if (pos + 200 > buf.length) buf = Arrays.copyOf(buf, buf.length * 2);
                        System.arraycopy(pfx, 0, buf, pos, pfx.length); pos += pfx.length;
                        System.arraycopy(sNameBytes[s], 0, buf, pos, sNameBytes[s].length); pos += sNameBytes[s].length;
                        buf[pos++] = ',';
                        pos = appendDoubleBytes(buf, pos, pcts[0]);
                        buf[pos++] = ',';
                        pos = appendDoubleBytes(buf, pos, pcts[1]);
                        System.arraycopy(NEW_LINE, 0, buf, pos, NEW_LINE.length); pos += NEW_LINE.length;
                    }
                }
                slidingBufs[tt] = buf;
                slidingLens[tt] = pos;
                return null;
            });
        }

        // Emit tumbling windows — direct byte[] output
        @SuppressWarnings("unchecked")
        Future<?>[] tumblingEmitFutures = new Future[nThreads];
        for (int t = 0; t < nThreads; t++) {
            int tt = t;
            int mStart = t * minutesPerThread;
            int mEnd = Math.min(mStart + minutesPerThread, MAX_MINUTES);
            final int fsc = sc;
            tumblingEmitFutures[t] = pool.submit(() -> {
                byte[] buf = new byte[10 << 20];
                int pos = 0;
                for (int m = mStart; m < mEnd; m++) {
                    byte[] pfx = tumblingPfx[m];
                    TumblingState[] tRow = mergedTumbling[m];
                    for (int s = 0; s < fsc; s++) {
                        TumblingState state = tRow[s];
                        if (state == null) continue;
                        if (pos + 200 > buf.length) buf = Arrays.copyOf(buf, buf.length * 2);
                        System.arraycopy(pfx, 0, buf, pos, pfx.length); pos += pfx.length;
                        System.arraycopy(sNameBytes[s], 0, buf, pos, sNameBytes[s].length); pos += sNameBytes[s].length;
                        buf[pos++] = ',';
                        pos = appendLongBytes(buf, pos, state.count);
                        buf[pos++] = ',';
                        pos = appendDoubleBytes(buf, pos, state.sum);
                        buf[pos++] = ',';
                        pos = appendDoubleBytes(buf, pos, state.min);
                        buf[pos++] = ',';
                        pos = appendDoubleBytes(buf, pos, state.max);
                        buf[pos++] = ',';
                        pos = appendDoubleBytes(buf, pos, state.avg());
                        System.arraycopy(NEW_LINE, 0, buf, pos, NEW_LINE.length); pos += NEW_LINE.length;
                    }
                }
                tumblingBufs[tt] = buf;
                tumblingLens[tt] = pos;
                return null;
            });
        }

        // Output in order: write directly from emit buffers, no trim copy
        BufferedOutputStream bos = new BufferedOutputStream(System.out, 1 << 20);
        for (int t = 0; t < nThreads; t++) {
            slidingEmitFutures[t].get();
            bos.write(slidingBufs[t], 0, slidingLens[t]);
        }
        for (int t = 0; t < nThreads; t++) {
            tumblingEmitFutures[t].get();
            bos.write(tumblingBufs[t], 0, tumblingLens[t]);
        }
        bos.flush();

        pool.shutdown();
        channel.close();
        raf.close();
    }

    static void mergeAndComputePcts(PartitionState[] parts, int sStart, int sEnd,
                                     TumblingState[][] mergedTumbling, double[][][] slidingPcts) {
        int globalMin = MAX_MINUTES, globalMax = -1;
        for (PartitionState ps : parts) {
            if (ps.minMinute < globalMin) globalMin = ps.minMinute;
            if (ps.maxMinute > globalMax) globalMax = ps.maxMinute;
        }
        if (globalMax < 0) return;

        double[] combinedBuf = new double[1024];
        for (int s = sStart; s < sEnd; s++) {
            TumblingState[] merged = new TumblingState[MAX_MINUTES];
            boolean hasData = false;
            for (PartitionState ps : parts) {
                int lo = ps.minMinute, hi = ps.maxMinute;
                for (int m = lo; m <= hi; m++) {
                    TumblingState[] tRow = ps.tumbling[m];
                    if (tRow != null && tRow[s] != null) {
                        hasData = true;
                        if (merged[m] == null) merged[m] = tRow[s];
                        else merged[m].merge(tRow[s]);
                    }
                }
            }
            if (!hasData) continue;
            // Write merged tumbling to [minute][sensor] layout
            for (int m = globalMin; m <= globalMax; m++) {
                if (merged[m] != null) mergedTumbling[m][s] = merged[m];
            }
            // Compute sliding window percentiles from merged raw values
            for (int m = Math.max(0, globalMin - 4); m <= Math.min(globalMax, MAX_MINUTES - 1); m++) {
                int totalSize = 0;
                int kEnd = Math.min(m + 4, MAX_MINUTES - 1);
                for (int k = m; k <= kEnd; k++) {
                    if (merged[k] != null) totalSize += merged[k].size;
                }
                if (totalSize == 0) continue;
                if (totalSize > combinedBuf.length) combinedBuf = new double[totalSize];
                double[] combined = combinedBuf;
                int p = 0;
                for (int k = m; k <= kEnd; k++) {
                    if (merged[k] != null) {
                        System.arraycopy(merged[k].values, 0, combined, p, merged[k].size);
                        p += merged[k].size;
                    }
                }
                int i99 = Math.max(0, (99 * totalSize + 99) / 100 - 1);
                int i50 = Math.max(0, (totalSize + 1) / 2 - 1);
                double p99 = TumblingState.quickselect(combined, 0, totalSize - 1, i99);
                double p50 = TumblingState.quickselect(combined, 0, i99, i50);
                slidingPcts[m][s] = new double[]{p50, p99};
            }
        }
    }

    static PartitionState processChunk(FileChannel channel, long start, long end, long baseMs) throws IOException {
        PartitionState ps = new PartitionState();
        ps.baseMs = baseMs;

        long size = end - start;
        if (size <= 0) return ps;

        // Read chunk via mmap — benefits from kernel readahead
        java.nio.MappedByteBuffer mbb = channel.map(FileChannel.MapMode.READ_ONLY, start, Math.min(size, Integer.MAX_VALUE));
        byte[] data = new byte[(int) Math.min(size, Integer.MAX_VALUE)];
        mbb.get(data);
        int limit = data.length;

        int pos = 0;
        while (pos < limit) {
            int lineStart = pos;
            // Skip 31 bytes: minimum line is 32 (20 ts + 1 comma + 8 sensor_X + 1 comma + 1 digit + 1 newline)
            pos += 31;
            while (pos < limit && data[pos] != '\n') pos++;
            int lineEnd = pos;
            pos++; // skip newline

            if (lineEnd - lineStart < 22) continue;

            // Fixed CSV format: YYYY-MM-DDTHH:MM:SSZ,sensor_XXXX,value
            int c1 = lineStart + 20;
            // Second comma: sensor name is "sensor_" + 1-4 digits → c2 in [c1+9, c1+12]
            int c2;
            if (data[c1 + 9] == ',') c2 = c1 + 9;
            else if (data[c1 + 10] == ',') c2 = c1 + 10;
            else if (data[c1 + 11] == ',') c2 = c1 + 11;
            else c2 = c1 + 12;

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

            if (eventMinute >= 0 && eventMinute < MAX_MINUTES) {
                if (eventMinute < ps.minMinute) ps.minMinute = eventMinute;
                if (eventMinute > ps.maxMinute) ps.maxMinute = eventMinute;
                TumblingState[] row = ps.tumbling[eventMinute];
                if (row == null) { row = new TumblingState[MAX_SENSORS]; ps.tumbling[eventMinute] = row; }
                TumblingState ts = row[sIdx];
                if (ts == null) { ts = new TumblingState(); row[sIdx] = ts; }
                ts.add(value);
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

    private static int appendDoubleBytes(byte[] buf, int pos, double d) {
        if (d < 0) { buf[pos++] = '-'; d = -d; }
        long scaled = Math.round(d * 100);
        long intPart = scaled / 100;
        int fracPart = (int)(scaled % 100);
        pos = appendLongBytes(buf, pos, intPart);
        buf[pos++] = '.';
        buf[pos++] = (byte)('0' + fracPart / 10);
        buf[pos++] = (byte)('0' + fracPart % 10);
        return pos;
    }

    private static int appendLongBytes(byte[] buf, int pos, long v) {
        if (v < 0) { buf[pos++] = '-'; v = -v; }
        if (v == 0) { buf[pos++] = '0'; return pos; }
        int start = pos;
        while (v > 0) { buf[pos++] = (byte)('0' + (int)(v % 10)); v /= 10; }
        // Reverse digits
        for (int i = start, j = pos - 1; i < j; i++, j--) {
            byte t = buf[i]; buf[i] = buf[j]; buf[j] = t;
        }
        return pos;
    }
}
