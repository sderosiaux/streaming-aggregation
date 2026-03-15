import java.io.*;
import java.util.*;

public class StreamingAggregator {

    static final long TUMBLING_WINDOW_MS = 60_000;
    static final long SLIDING_WINDOW_MS = 5 * 60_000;
    static final long SLIDING_STEP_MS = 60_000;
    static final long WATERMARK_SLACK_MS = 10 * 60_000;
    static final long ALLOWED_LATENESS_MS = 10 * 60_000;

    private static final int[] DAYS_CUM = {0,31,59,90,120,151,181,212,243,273,304,334};
    private static final int[] DAYS_CUM_LEAP = {0,31,60,91,121,152,182,213,244,274,305,335};

    // Array-indexed window storage
    // Data spans ~24h + 10min late buffer = ~1450 minutes. Use 1500 for safety.
    static final int MAX_MINUTES = 1500;
    static final int MAX_SENSORS = 1100; // DataGenerator uses 1000, with buffer

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

        double avg() {
            return count == 0 ? 0 : sum / count;
        }
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

        double[] percentiles() {
            if (size == 0) return new double[]{0, 0};
            double[] arr = Arrays.copyOf(values, size);
            int i99 = Math.max(0, (int) Math.ceil(99.0 / 100.0 * size) - 1);
            int i50 = Math.max(0, (int) Math.ceil(50.0 / 100.0 * size) - 1);
            double p99 = quickselect(arr, 0, size - 1, i99);
            double p50 = quickselect(arr, 0, i99, i50);
            return new double[]{p50, p99};
        }

        private static double quickselect(double[] arr, int lo, int hi, int k) {
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
                    if (i <= j) {
                        double t = arr[i]; arr[i] = arr[j]; arr[j] = t;
                        i++; j--;
                    }
                }
                double t = arr[i]; arr[i] = arr[hi]; arr[hi] = t;
                if (k == i) return arr[i];
                else if (k < i) hi = i - 1;
                else lo = i + 1;
            }
            return arr[lo];
        }
    }

    // Flat arrays indexed by [sensorIdx][minuteIdx]
    private TumblingState[][] tumblingArr;
    private SlidingState[][] slidingArr;
    private boolean[][] emittedTumbling;
    private boolean[][] emittedSliding;
    private final HashMap<String, Integer> sensorIndex = new HashMap<>(2048);
    private final String[] sensorNames = new String[MAX_SENSORS];
    private int sensorCount = 0;
    private long baseMs; // epoch ms of earliest minute bucket
    private boolean baseSet = false;
    private final List<String> results = new ArrayList<>();
    private long watermarkMs = Long.MIN_VALUE;
    // Scan floors: lowest minute index not yet cleaned, per type
    private int tumblingFloor = 0;
    private int slidingFloor = 0;

    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.err.println("Usage: StreamingAggregator <input-file>");
            System.exit(1);
        }
        new StreamingAggregator().run(args[0]);
    }

    private int getSensorIdx(String sensorId) {
        Integer idx = sensorIndex.get(sensorId);
        if (idx != null) return idx;
        int i = sensorCount++;
        sensorIndex.put(sensorId, i);
        sensorNames[i] = sensorId;
        return i;
    }

    private int minuteIdx(long ms) {
        return (int) ((ms - baseMs) / 60_000);
    }

    private void ensureArrays(int minIdx) {
        // Lazy-allocate on first event
        if (tumblingArr == null) {
            int size = MAX_MINUTES;
            tumblingArr = new TumblingState[MAX_SENSORS][];
            slidingArr = new SlidingState[MAX_SENSORS][];
            emittedTumbling = new boolean[MAX_SENSORS][];
            emittedSliding = new boolean[MAX_SENSORS][];
        }
    }

    private TumblingState getTumblingState(int sensor, int minute) {
        if (tumblingArr[sensor] == null) {
            tumblingArr[sensor] = new TumblingState[MAX_MINUTES];
        }
        TumblingState s = tumblingArr[sensor][minute];
        if (s == null) {
            s = new TumblingState();
            tumblingArr[sensor][minute] = s;
        }
        return s;
    }

    private SlidingState getSlidingState(int sensor, int minute) {
        if (slidingArr[sensor] == null) {
            slidingArr[sensor] = new SlidingState[MAX_MINUTES];
        }
        SlidingState s = slidingArr[sensor][minute];
        if (s == null) {
            s = new SlidingState();
            slidingArr[sensor][minute] = s;
        }
        return s;
    }

    void run(String inputFile) throws IOException {
        try (BufferedReader reader = new BufferedReader(new FileReader(inputFile), 1 << 20)) {
            String line;
            long lineCount = 0;
            long maxEventTime = Long.MIN_VALUE;

            while ((line = reader.readLine()) != null) {
                int c1 = line.indexOf(',');
                if (c1 < 0) continue;
                int c2 = line.indexOf(',', c1 + 1);
                if (c2 < 0) continue;

                long timestampMs = parseIsoTimestamp(line, c1);
                if (timestampMs < 0) continue;
                double value = parseFastDouble(line, c2 + 1, line.length());

                if (!baseSet) {
                    baseMs = (timestampMs / 60_000 - 15) * 60_000;
                    baseSet = true;
                    ensureArrays(0);
                }

                // Parse sensor index directly: "sensor_NNNN" -> extract 4 digits
                int sIdx = parseSensorIdx(line, c1 + 1, c2);
                if (sIdx < 0 || sIdx >= MAX_SENSORS) continue;
                if (sensorNames[sIdx] == null) {
                    sensorNames[sIdx] = line.substring(c1 + 1, c2);
                    if (sIdx >= sensorCount) sensorCount = sIdx + 1;
                }

                lineCount++;
                maxEventTime = Math.max(maxEventTime, timestampMs);

                // Defer all emission to end — single pass over windows
                // Late data still gets accumulated correctly

                long tumblingStart = timestampMs - (timestampMs % TUMBLING_WINDOW_MS);
                int tMinute = minuteIdx(tumblingStart);
                if (tMinute >= 0 && tMinute < MAX_MINUTES) {
                    getTumblingState(sIdx, tMinute).add(value);
                }

                long firstSlidingStart = timestampMs - SLIDING_WINDOW_MS + SLIDING_STEP_MS;
                firstSlidingStart = firstSlidingStart - (firstSlidingStart % SLIDING_STEP_MS);
                if (firstSlidingStart < 0) firstSlidingStart = 0;

                for (long wStart = firstSlidingStart; wStart <= timestampMs; wStart += SLIDING_STEP_MS) {
                    long wEnd = wStart + SLIDING_WINDOW_MS;
                    if (timestampMs >= wStart && timestampMs < wEnd) {
                        int sMinute = minuteIdx(wStart);
                        if (sMinute >= 0 && sMinute < MAX_MINUTES) {
                            getSlidingState(sIdx, sMinute).add(value);
                        }
                    }
                }
            }

            watermarkMs = Long.MAX_VALUE;
            emitReadyWindows();
        }

        Collections.sort(results);
        StringBuilder sb = new StringBuilder(results.size() * 80);
        for (String result : results) {
            sb.append(result).append('\n');
        }
        System.out.print(sb);
    }

    private static long parseIsoTimestamp(String line, int end) {
        if (end < 20) return -1;
        int year = parseInt4(line, 0);
        int month = parseInt2(line, 5);
        int day = parseInt2(line, 8);
        int hour = parseInt2(line, 11);
        int minute = parseInt2(line, 14);
        int second = parseInt2(line, 17);
        long days = daysSinceEpoch(year, month, day);
        return ((days * 24 + hour) * 60 + minute) * 60000L + second * 1000L;
    }

    private static int parseInt4(String s, int off) {
        return (s.charAt(off) - '0') * 1000 + (s.charAt(off+1) - '0') * 100
             + (s.charAt(off+2) - '0') * 10 + (s.charAt(off+3) - '0');
    }

    private static int parseInt2(String s, int off) {
        return (s.charAt(off) - '0') * 10 + (s.charAt(off+1) - '0');
    }

    private static long daysSinceEpoch(int year, int month, int day) {
        long totalDays = 365L * (year - 1970);
        totalDays += leapYearsBetween(1970, year);
        boolean leap = isLeapYear(year);
        totalDays += (leap ? DAYS_CUM_LEAP : DAYS_CUM)[month - 1];
        totalDays += day - 1;
        return totalDays;
    }

    private static long leapYearsBetween(int from, int to) {
        return countLeapYears(to - 1) - countLeapYears(from - 1);
    }

    private static long countLeapYears(int y) {
        if (y < 0) return 0;
        return y / 4 - y / 100 + y / 400;
    }

    private static boolean isLeapYear(int year) {
        return (year % 4 == 0) && ((year % 100 != 0) || (year % 400 == 0));
    }

    // Parse "sensor_NNNN" -> NNNN as int
    private static int parseSensorIdx(String line, int start, int end) {
        // Skip "sensor_" prefix (7 chars), parse remaining digits
        int numStart = start + 7;
        int idx = 0;
        for (int i = numStart; i < end; i++) {
            idx = idx * 10 + (line.charAt(i) - '0');
        }
        return idx;
    }

    private static double parseFastDouble(String s, int start, int end) {
        boolean neg = false;
        int i = start;
        if (i < end && s.charAt(i) == '-') { neg = true; i++; }
        long intPart = 0;
        while (i < end && s.charAt(i) != '.') {
            intPart = intPart * 10 + (s.charAt(i) - '0');
            i++;
        }
        double result = intPart;
        if (i < end && s.charAt(i) == '.') {
            i++;
            long frac = 0;
            double div = 1;
            while (i < end) {
                frac = frac * 10 + (s.charAt(i) - '0');
                div *= 10;
                i++;
            }
            result += frac / div;
        }
        return neg ? -result : result;
    }

    private void emitReadyWindows() {
        StringBuilder sb = new StringBuilder(128);

        // Compute scan ceiling: max minute index where windowEnd <= watermarkMs
        int tCeil = watermarkMs == Long.MAX_VALUE ? MAX_MINUTES :
            Math.min(MAX_MINUTES, (int) ((watermarkMs - baseMs - TUMBLING_WINDOW_MS) / 60_000) + 1);
        int sCeil = watermarkMs == Long.MAX_VALUE ? MAX_MINUTES :
            Math.min(MAX_MINUTES, (int) ((watermarkMs - baseMs - SLIDING_WINDOW_MS) / 60_000) + 1);

        int newTFloor = tCeil;
        for (int s = 0; s < sensorCount; s++) {
            if (tumblingArr[s] == null) continue;
            if (emittedTumbling[s] == null) emittedTumbling[s] = new boolean[MAX_MINUTES];
            TumblingState[] arr = tumblingArr[s];
            boolean[] emitted = emittedTumbling[s];
            String sensor = sensorNames[s];

            for (int m = tumblingFloor; m < tCeil; m++) {
                TumblingState state = arr[m];
                if (state == null) continue;

                long windowStart = baseMs + (long) m * 60_000;
                long windowEnd = windowStart + TUMBLING_WINDOW_MS;

                boolean updated = emitted[m];
                emitted[m] = true;

                sb.setLength(0);
                sb.append("tumbling,").append(windowStart).append(',')
                  .append(sensor).append(',')
                  .append(state.count).append(',');
                appendDouble(sb, state.sum); sb.append(',');
                appendDouble(sb, state.min); sb.append(',');
                appendDouble(sb, state.max); sb.append(',');
                appendDouble(sb, state.avg()); sb.append(',');
                sb.append(updated ? "updated" : "new");
                results.add(sb.toString());

                if (windowEnd + ALLOWED_LATENESS_MS <= watermarkMs) {
                    arr[m] = null;
                } else if (m < newTFloor) {
                    newTFloor = m;
                }
            }
        }
        tumblingFloor = newTFloor;

        int newSFloor = sCeil;
        for (int s = 0; s < sensorCount; s++) {
            if (slidingArr[s] == null) continue;
            if (emittedSliding[s] == null) emittedSliding[s] = new boolean[MAX_MINUTES];
            SlidingState[] arr = slidingArr[s];
            boolean[] emitted = emittedSliding[s];
            String sensor = sensorNames[s];

            for (int m = slidingFloor; m < sCeil; m++) {
                SlidingState state = arr[m];
                if (state == null) continue;

                long windowStart = baseMs + (long) m * 60_000;
                long windowEnd = windowStart + SLIDING_WINDOW_MS;

                boolean updated = emitted[m];
                emitted[m] = true;

                double[] pcts = state.percentiles();
                sb.setLength(0);
                sb.append("sliding,").append(windowStart).append(',')
                  .append(sensor).append(',');
                appendDouble(sb, pcts[0]); sb.append(',');
                appendDouble(sb, pcts[1]); sb.append(',');
                sb.append(updated ? "updated" : "new");
                results.add(sb.toString());

                if (windowEnd + ALLOWED_LATENESS_MS <= watermarkMs) {
                    arr[m] = null;
                } else if (m < newSFloor) {
                    newSFloor = m;
                }
            }
        }
        slidingFloor = newSFloor;
    }

    private static void appendDouble(StringBuilder sb, double d) {
        if (d < 0) {
            sb.append('-');
            d = -d;
        }
        long scaled = Math.round(d * 100);
        long intPart = scaled / 100;
        int fracPart = (int)(scaled % 100);
        sb.append(intPart).append('.');
        if (fracPart < 10) sb.append('0');
        sb.append(fracPart);
    }
}
