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

    // Compact key: no windowType string (separate maps for tumbling/sliding)
    record SensorWindow(String sensorId, long windowStartMs) {}

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

    private final Map<SensorWindow, TumblingState> tumblingWindows = new HashMap<>();
    private final Map<SensorWindow, SlidingState> slidingWindows = new HashMap<>();
    private final Set<SensorWindow> emittedTumbling = new HashSet<>();
    private final Set<SensorWindow> emittedSliding = new HashSet<>();
    private final List<String> results = new ArrayList<>();
    private long watermarkMs = Long.MIN_VALUE;

    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.err.println("Usage: StreamingAggregator <input-file>");
            System.exit(1);
        }
        new StreamingAggregator().run(args[0]);
    }

    void run(String inputFile) throws IOException {
        HashMap<String, String> sensorIntern = new HashMap<>(2048);
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
                String rawSensor = line.substring(c1 + 1, c2);
                String sensorId = sensorIntern.computeIfAbsent(rawSensor, k -> k);
                double value = parseFastDouble(line, c2 + 1, line.length());

                lineCount++;
                maxEventTime = Math.max(maxEventTime, timestampMs);

                if (lineCount % 10_000 == 0) {
                    long newWatermark = maxEventTime - WATERMARK_SLACK_MS;
                    if (newWatermark > watermarkMs) {
                        watermarkMs = newWatermark;
                        emitReadyWindows();
                    }
                }

                long tumblingStart = timestampMs - (timestampMs % TUMBLING_WINDOW_MS);
                SensorWindow tKey = new SensorWindow(sensorId, tumblingStart);
                tumblingWindows.computeIfAbsent(tKey, k -> new TumblingState()).add(value);

                long firstSlidingStart = timestampMs - SLIDING_WINDOW_MS + SLIDING_STEP_MS;
                firstSlidingStart = firstSlidingStart - (firstSlidingStart % SLIDING_STEP_MS);
                if (firstSlidingStart < 0) firstSlidingStart = 0;

                for (long wStart = firstSlidingStart; wStart <= timestampMs; wStart += SLIDING_STEP_MS) {
                    long wEnd = wStart + SLIDING_WINDOW_MS;
                    if (timestampMs >= wStart && timestampMs < wEnd) {
                        SensorWindow sKey = new SensorWindow(sensorId, wStart);
                        slidingWindows.computeIfAbsent(sKey, k -> new SlidingState()).add(value);
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

        Iterator<Map.Entry<SensorWindow, TumblingState>> tIt = tumblingWindows.entrySet().iterator();
        while (tIt.hasNext()) {
            Map.Entry<SensorWindow, TumblingState> entry = tIt.next();
            SensorWindow key = entry.getKey();
            long windowEnd = key.windowStartMs + TUMBLING_WINDOW_MS;

            if (windowEnd <= watermarkMs) {
                TumblingState state = entry.getValue();
                boolean updated = emittedTumbling.contains(key);
                emittedTumbling.add(key);

                sb.setLength(0);
                sb.append("tumbling,").append(key.windowStartMs).append(',')
                  .append(key.sensorId).append(',')
                  .append(state.count).append(',');
                appendDouble(sb, state.sum); sb.append(',');
                appendDouble(sb, state.min); sb.append(',');
                appendDouble(sb, state.max); sb.append(',');
                appendDouble(sb, state.avg()); sb.append(',');
                sb.append(updated ? "updated" : "new");
                results.add(sb.toString());

                if (windowEnd + ALLOWED_LATENESS_MS <= watermarkMs) {
                    tIt.remove();
                }
            }
        }

        Iterator<Map.Entry<SensorWindow, SlidingState>> sIt = slidingWindows.entrySet().iterator();
        while (sIt.hasNext()) {
            Map.Entry<SensorWindow, SlidingState> entry = sIt.next();
            SensorWindow key = entry.getKey();
            long windowEnd = key.windowStartMs + SLIDING_WINDOW_MS;

            if (windowEnd <= watermarkMs) {
                SlidingState state = entry.getValue();
                boolean updated = emittedSliding.contains(key);
                emittedSliding.add(key);

                double[] pcts = state.percentiles();
                sb.setLength(0);
                sb.append("sliding,").append(key.windowStartMs).append(',')
                  .append(key.sensorId).append(',');
                appendDouble(sb, pcts[0]); sb.append(',');
                appendDouble(sb, pcts[1]); sb.append(',');
                sb.append(updated ? "updated" : "new");
                results.add(sb.toString());

                if (windowEnd + ALLOWED_LATENESS_MS <= watermarkMs) {
                    sIt.remove();
                }
            }
        }
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
