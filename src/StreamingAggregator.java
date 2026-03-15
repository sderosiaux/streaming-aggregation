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

    // Unified state: tumbling aggregates + scaled int values for sliding percentiles
    static class TumblingState {
        long count = 0;
        double sum = 0;
        double min = Double.MAX_VALUE;
        double max = -Double.MAX_VALUE;
        int[] values = new int[8]; // scaled by 100 (e.g., 29.19 → 2919)
        int size = 0;

        void add(double value, int scaledValue) {
            count++;
            sum += value;
            min = Math.min(min, value);
            max = Math.max(max, value);
            if (size == values.length) {
                values = Arrays.copyOf(values, values.length * 2);
            }
            values[size++] = scaledValue;
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

        static int quickselect(int[] arr, int lo, int hi, int k) {
            while (lo < hi) {
                int mid = lo + (hi - lo) / 2;
                if (arr[mid] < arr[lo]) { int t = arr[lo]; arr[lo] = arr[mid]; arr[mid] = t; }
                if (arr[hi] < arr[lo]) { int t = arr[lo]; arr[lo] = arr[hi]; arr[hi] = t; }
                if (arr[mid] < arr[hi]) { int t = arr[mid]; arr[mid] = arr[hi]; arr[hi] = t; }
                int pivot = arr[hi];
                int i = lo, j = hi - 1;
                while (i <= j) {
                    while (arr[i] < pivot) i++;
                    while (j >= i && arr[j] > pivot) j--;
                    if (i <= j) { int t = arr[i]; arr[i] = arr[j]; arr[j] = t; i++; j--; }
                }
                int t = arr[i]; arr[i] = arr[hi]; arr[hi] = t;
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

        // Single mmap of entire file
        MappedByteBuffer mbb = channel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize);

        // Find line-aligned chunk boundaries
        long[] boundaries = new long[nThreads + 1];
        boundaries[0] = 0;
        boundaries[nThreads] = fileSize;

        byte[] scanBuf = new byte[4096];
        for (int i = 1; i < nThreads; i++) {
            int approx = (int)(fileSize * i / nThreads);
            mbb.get(approx, scanBuf, 0, Math.min(4096, (int)(fileSize - approx)));
            int nl = -1;
            for (int j = 0; j < scanBuf.length; j++) {
                if (scanBuf[j] == '\n') { nl = j; break; }
            }
            boundaries[i] = (nl >= 0) ? approx + nl + 1 : approx;
        }

        // Determine baseMs from first line
        byte[] firstLine = new byte[128];
        mbb.get(0, firstLine, 0, 128);
        long firstTs = parseIsoBytesArr(firstLine, 0);
        long baseMs = (firstTs / 60_000 - 15) * 60_000;

        // Process chunks in parallel — parse directly from MappedByteBuffer (zero copy)
        ExecutorService pool = Executors.newFixedThreadPool(nThreads);
        Future<PartitionState>[] futures = new Future[nThreads];

        for (int t = 0; t < nThreads; t++) {
            final int chunkStart = (int)boundaries[t];
            final int chunkEnd = (int)boundaries[t + 1];
            final long bMs = baseMs;
            futures[t] = pool.submit(() -> processChunkMBB(mbb, chunkStart, chunkEnd, bMs));
        }

        // Collect partition states
        PartitionState[] parts = new PartitionState[nThreads];
        String[] sensorNames = new String[MAX_SENSORS];
        int sensorCount = 0;
        int globalMinMinute = MAX_MINUTES;
        int globalMaxMinute = -1;
        for (int t = 0; t < nThreads; t++) {
            parts[t] = futures[t].get();
            if (parts[t].sensorCount > sensorCount) sensorCount = parts[t].sensorCount;
            if (parts[t].minMinute < globalMinMinute) globalMinMinute = parts[t].minMinute;
            if (parts[t].maxMinute > globalMaxMinute) globalMaxMinute = parts[t].maxMinute;
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
        final int gMin = globalMinMinute;
        final int gMax = globalMaxMinute;
        int sensorsPerThread = (sc + nThreads - 1) / nThreads;

        // Shared merged array — [minute][sensor] layout for cache-friendly emit
        // Only allocate for the actual used range [gMin..gMax]
        TumblingState[][] mergedTumbling = new TumblingState[MAX_MINUTES][];
        for (int m = gMin; m <= gMax; m++) {
            mergedTumbling[m] = new TumblingState[sc];
        }

        @SuppressWarnings("unchecked")
        Future<?>[] mergeFutures = new Future[nThreads];
        for (int t = 0; t < nThreads; t++) {
            int sStart = t * sensorsPerThread;
            int sEnd = Math.min(sStart + sensorsPerThread, sc);
            mergeFutures[t] = pool.submit(() -> {
                mergePartitions(parts, sStart, sEnd, mergedTumbling);
            });
        }
        for (Future<?> f : mergeFutures) f.get();

        // Pre-compute prefixes as byte[] and sensor names as byte[]
        int pfxMin = Math.max(0, gMin - 4);
        byte[][] slidingPfx = new byte[MAX_MINUTES][];
        byte[][] tumblingPfx = new byte[MAX_MINUTES][];
        for (int m = pfxMin; m <= gMax; m++) {
            long ws = bMs + (long) m * 60_000;
            slidingPfx[m] = ("sliding," + ws + ",").getBytes();
            tumblingPfx[m] = ("tumbling," + ws + ",").getBytes();
        }
        byte[][] sNameBytes = new byte[sc][];
        for (int s = 0; s < sc; s++) {
            sNameBytes[s] = sNames[s] != null ? sNames[s].getBytes() : new byte[0];
        }
        byte[] NEW_LINE = ",new\n".getBytes();

        // Sliding windows can start up to 4 minutes before first data
        final int emitMin = Math.max(0, gMin - 4);
        int minuteRange = gMax - emitMin + 1;
        int minutesPerThread = (minuteRange + nThreads - 1) / nThreads;

        // Shared emit buffers — avoid Arrays.copyOf trim at end of each task
        byte[][] slidingBufs = new byte[nThreads][];
        int[] slidingLens = new int[nThreads];
        byte[][] tumblingBufs = new byte[nThreads][];
        int[] tumblingLens = new int[nThreads];

        // Emit sliding windows — compute percentiles inline, direct byte[] output
        @SuppressWarnings("unchecked")
        Future<?>[] slidingEmitFutures = new Future[nThreads];
        for (int t = 0; t < nThreads; t++) {
            int tt = t;
            int mStart = emitMin + t * minutesPerThread;
            int mEnd = Math.min(mStart + minutesPerThread, gMax + 1);
            final int fsc = sc;
            slidingEmitFutures[t] = pool.submit(() -> {
                byte[] buf = new byte[8 << 20];
                int pos = 0;
                int[] combinedBuf = new int[1024];
                for (int m = mStart; m < mEnd; m++) {
                    byte[] pfx = slidingPfx[m];
                    int kEnd = Math.min(m + 4, gMax);
                    for (int s = 0; s < fsc; s++) {
                        int[] combined = combinedBuf;
                        int p = 0;
                        for (int k = m; k <= kEnd; k++) {
                            TumblingState[] row = mergedTumbling[k];
                            TumblingState ts = (row != null) ? row[s] : null;
                            if (ts != null) {
                                int sz = ts.size;
                                int needed = p + sz;
                                if (needed > combined.length) {
                                    combinedBuf = new int[needed * 2];
                                    System.arraycopy(combined, 0, combinedBuf, 0, p);
                                    combined = combinedBuf;
                                }
                                System.arraycopy(ts.values, 0, combined, p, sz);
                                p += sz;
                            }
                        }
                        if (p == 0) continue;
                        int totalSize = p;
                        int i99 = Math.max(0, (99 * totalSize + 99) / 100 - 1);
                        int i50 = Math.max(0, (totalSize + 1) / 2 - 1);
                        int p99 = TumblingState.quickselect(combined, 0, totalSize - 1, i99);
                        int p50 = TumblingState.quickselect(combined, 0, i99, i50);
                        if (pos + 200 > buf.length) buf = Arrays.copyOf(buf, buf.length * 2);
                        System.arraycopy(pfx, 0, buf, pos, pfx.length); pos += pfx.length;
                        System.arraycopy(sNameBytes[s], 0, buf, pos, sNameBytes[s].length); pos += sNameBytes[s].length;
                        buf[pos++] = ',';
                        pos = appendScaledInt(buf, pos, p50);
                        buf[pos++] = ',';
                        pos = appendScaledInt(buf, pos, p99);
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
            int mStart = emitMin + t * minutesPerThread;
            int mEnd = Math.min(mStart + minutesPerThread, gMax + 1);
            final int fsc = sc;
            tumblingEmitFutures[t] = pool.submit(() -> {
                byte[] buf = new byte[10 << 20];
                int pos = 0;
                for (int m = mStart; m < mEnd; m++) {
                    TumblingState[] tRow = mergedTumbling[m];
                    if (tRow == null) continue;
                    byte[] pfx = tumblingPfx[m];
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

    static void mergePartitions(PartitionState[] parts, int sStart, int sEnd,
                                TumblingState[][] mergedTumbling) {
        // Merge directly into mergedTumbling — no intermediate array
        for (int s = sStart; s < sEnd; s++) {
            for (PartitionState ps : parts) {
                int lo = ps.minMinute, hi = ps.maxMinute;
                for (int m = lo; m <= hi; m++) {
                    TumblingState[] tRow = ps.tumbling[m];
                    if (tRow != null && tRow[s] != null) {
                        TumblingState[] mRow = mergedTumbling[m];
                        if (mRow[s] == null) mRow[s] = tRow[s];
                        else mRow[s].merge(tRow[s]);
                    }
                }
            }
        }
    }

    static PartitionState processChunkMBB(MappedByteBuffer mbb, int chunkStart, int chunkEnd, long baseMs) {
        PartitionState ps = new PartitionState();
        ps.baseMs = baseMs;

        if (chunkStart >= chunkEnd) return ps;

        // Parse first timestamp to compute day offset
        int hour0 = (mbb.get(chunkStart + 11) - '0') * 10 + (mbb.get(chunkStart + 12) - '0');
        int minute0 = (mbb.get(chunkStart + 14) - '0') * 10 + (mbb.get(chunkStart + 15) - '0');
        int year = (mbb.get(chunkStart) - '0') * 1000 + (mbb.get(chunkStart+1) - '0') * 100
                 + (mbb.get(chunkStart+2) - '0') * 10 + (mbb.get(chunkStart+3) - '0');
        int month = (mbb.get(chunkStart+5) - '0') * 10 + (mbb.get(chunkStart+6) - '0');
        int day = (mbb.get(chunkStart+8) - '0') * 10 + (mbb.get(chunkStart+9) - '0');
        int second = (mbb.get(chunkStart+17) - '0') * 10 + (mbb.get(chunkStart+18) - '0');
        long totalDays = 365L * (year - 1970);
        totalDays += countLeapYears(year - 1) - countLeapYears(1969);
        boolean leap = (year % 4 == 0) && ((year % 100 != 0) || (year % 400 == 0));
        totalDays += (leap ? DAYS_CUM_LEAP : DAYS_CUM)[month - 1] + day - 1;
        long firstChunkTs = ((totalDays * 24 + hour0) * 60 + minute0) * 60000L + second * 1000L;
        int baseDayMinutes = (int)(firstChunkTs / 60_000) - (int)(firstChunkTs / 60_000) % 1440;
        int baseMinute = (int)(baseMs / 60_000);
        int dayOffset = baseDayMinutes - baseMinute;

        byte[] sensorBuf = new byte[11]; // "sensor_XXXX"

        int pos = chunkStart;
        while (pos < chunkEnd) {
            // Fixed layout: 20-char timestamp + comma + 11-char sensor + comma + value + newline
            // No newline scan needed — compute next position from value format
            int c1 = pos + 20;

            int hour = (mbb.get(pos + 11) - '0') * 10 + (mbb.get(pos + 12) - '0');
            int minute = (mbb.get(pos + 14) - '0') * 10 + (mbb.get(pos + 15) - '0');

            int sIdx = (mbb.get(c1 + 8) - '0') * 1000 + (mbb.get(c1 + 9) - '0') * 100
                     + (mbb.get(c1 + 10) - '0') * 10 + (mbb.get(c1 + 11) - '0');

            if (ps.sensorNames[sIdx] == null) {
                mbb.get(c1 + 1, sensorBuf, 0, 11);
                ps.sensorNames[sIdx] = new String(sensorBuf);
                if (sIdx >= ps.sensorCount) ps.sensorCount = sIdx + 1;
            }

            // Specialized parser: compute both double (for sum/min/max) and scaled int (for percentiles)
            // Format: [-]d{1,3}.dd — compute exact value length to skip newline scan
            int di = pos + 33; // c1 + 12 + 1 = pos + 20 + 12 + 1
            byte b0 = mbb.get(di);
            boolean neg = (b0 == '-');
            if (neg) { di++; b0 = mbb.get(di); }
            byte b1 = mbb.get(di + 1);
            double value;
            int scaledValue;
            if (b1 == '.') {
                // d.dd (4 chars)
                int d0 = b0 - '0', d2 = mbb.get(di + 2) - '0', d3 = mbb.get(di + 3) - '0';
                value = d0 + (d2 * 10 + d3) * 0.01;
                scaledValue = d0 * 100 + d2 * 10 + d3;
                pos = di + 5;
            } else {
                byte b2 = mbb.get(di + 2);
                if (b2 == '.') {
                    // dd.dd (5 chars, most common: 86%)
                    int d0 = b0 - '0', d1 = b1 - '0', d3 = mbb.get(di + 3) - '0', d4 = mbb.get(di + 4) - '0';
                    value = (d0 * 10 + d1) + (d3 * 10 + d4) * 0.01;
                    scaledValue = (d0 * 10 + d1) * 100 + d3 * 10 + d4;
                    pos = di + 6;
                } else {
                    // ddd.dd (6 chars)
                    int d0 = b0 - '0', d1 = b1 - '0', d2i = b2 - '0', d4 = mbb.get(di + 4) - '0', d5 = mbb.get(di + 5) - '0';
                    value = (d0 * 100 + d1 * 10 + d2i) + (d4 * 10 + d5) * 0.01;
                    scaledValue = (d0 * 100 + d1 * 10 + d2i) * 100 + d4 * 10 + d5;
                    pos = di + 7;
                }
            }
            if (neg) { value = -value; scaledValue = -scaledValue; }

            int eventMinute = dayOffset + hour * 60 + minute;

            if (eventMinute >= 0 && eventMinute < MAX_MINUTES) {
                if (eventMinute < ps.minMinute) ps.minMinute = eventMinute;
                if (eventMinute > ps.maxMinute) ps.maxMinute = eventMinute;
                TumblingState[] row = ps.tumbling[eventMinute];
                if (row == null) { row = new TumblingState[MAX_SENSORS]; ps.tumbling[eventMinute] = row; }
                TumblingState ts = row[sIdx];
                if (ts == null) { ts = new TumblingState(); row[sIdx] = ts; }
                ts.add(value, scaledValue);
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

    private static int appendScaledInt(byte[] buf, int pos, int v) {
        if (v < 0) { buf[pos++] = '-'; v = -v; }
        int intPart = v / 100;
        int fracPart = v % 100;
        pos = appendLongBytes(buf, pos, intPart);
        buf[pos++] = '.';
        buf[pos++] = (byte)('0' + fracPart / 10);
        buf[pos++] = (byte)('0' + fracPart % 10);
        return pos;
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
