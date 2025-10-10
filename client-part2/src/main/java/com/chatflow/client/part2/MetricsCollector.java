package com.chatflow.client.part2;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * MetricsCollector with a single writer thread and a bounded queue so CSV
 * writes are never interleaved or corrupted when many threads log at once.
 *
 * CSV columns: timestamp,messageType,latencyMs,statusCode,roomId
 *
 * Also aggregates latency stats, per-room counts, type distribution
 *
 */
public class MetricsCollector {

    // CSV writer (single-threaded)

    private final Path csvPath;
    private final BlockingQueue<String> csvQueue = new LinkedBlockingQueue<>(200_000);
    private volatile boolean running;
    private Thread writerThread;
    private PrintWriter csvOut;

    // Aggregations

    private final int roomCount;
    private final long[] roomAcks;        // per-room ACK count
    private final AtomicLong textCnt = new AtomicLong();
    private final AtomicLong joinCnt = new AtomicLong();
    private final AtomicLong leaveCnt = new AtomicLong();

    private final List<Long> latencies = Collections.synchronizedList(new ArrayList<>());
    private final List<Long> ackEpochMillis = Collections.synchronizedList(new ArrayList<>());

    // test start millis (used to compute buckets)
    private volatile long startMillis = -1L;

    public MetricsCollector(int roomCount, String csvFilename) {
        this.roomCount = roomCount;
        this.roomAcks = new long[roomCount + 1]; // 1-based index: [1..roomCount]
        this.csvPath = Path.of(csvFilename);
    }

    /** Set test start time (epoch millis). Call once at the beginning of a phase. */
    public void setStartMillis(long startMillis) {
        this.startMillis = startMillis;
    }

    //  (launch/stop writer thread)
    public void open() throws IOException {
        // Open/truncate CSV and launch writer thread
        csvOut = new PrintWriter(new BufferedWriter(
                Files.newBufferedWriter(csvPath,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.TRUNCATE_EXISTING,
                        StandardOpenOption.WRITE)));
        // header
        csvOut.println("timestamp,messageType,latencyMs,statusCode,roomId");
        csvOut.flush();

        running = true;
        writerThread = new Thread(() -> { //using a thread to not block caller
            try {
                while (running || !csvQueue.isEmpty()) {
                    String line = csvQueue.poll(250, TimeUnit.MILLISECONDS);
                    if (line != null) {
                        csvOut.println(line);
                    }
                }
            } catch (InterruptedException ignored) {
            } finally {
                try {
                    csvOut.flush();
                } catch (Throwable ignore) {}
                try {
                    csvOut.close();
                } catch (Throwable ignore) {}
            }
        }, "csv-writer");
        writerThread.setDaemon(true);
        writerThread.start();
    }

    public void close() {
        running = false;
        if (writerThread != null) {
            try {
                writerThread.join(5_000);
            } catch (InterruptedException ignored) {}
        }
    }

    // Logging API

    /**
     * Log one ACK row into the CSV (thread-safe).
     * All fields must be plain (no commas). Timestamp should be ISO-8601.
     * Also updates in-memory aggregations for stats & charting.
     */
    public void logCsv(String timestampIso,
                       String messageType,
                       long latencyMs,
                       String statusCode,
                       int roomId) {
        // enqueue CSV row (non-blocking – drop if queue is totally full to avoid backpressure)
        String row = String.format(Locale.US, "%s,%s,%d,%s,%d",
                timestampIso, messageType, latencyMs, statusCode, roomId);
        csvQueue.offer(row);

        // update counters/arrays
        if (roomId >= 1 && roomId <= roomCount) {
            synchronized (roomAcks) { roomAcks[roomId]++; }
        }
        switch (safeUpper(messageType)) {
            case "TEXT":  textCnt.incrementAndGet(); break;
            case "JOIN":  joinCnt.incrementAndGet(); break;
            case "LEAVE": leaveCnt.incrementAndGet(); break;
        }
        latencies.add(latencyMs);

        // Best-effort timestamp parse for chart (uses ack timestamp)
        try {
            long ms = Instant.parse(timestampIso).toEpochMilli();
            ackEpochMillis.add(ms);
        } catch (Exception ignore) {}
    }

    // Reporting
    public void printSummary() {
        // Latency stats
        List<Long> copy;
        synchronized (latencies) { copy = new ArrayList<>(latencies); }

        double mean = 0;
        double median = 0;
        double p95 = 0;
        double p99 = 0;
        long min = 0;
        long max = 0;

        if (!copy.isEmpty()) {
            Collections.sort(copy);
            long sum = 0;
            for (long v : copy) sum += v;
            mean = sum / (double) copy.size();
            median = percentile(copy, 50);
            p95 = percentile(copy, 95);
            p99 = percentile(copy, 99);
            min = copy.get(0);
            max = copy.get(copy.size() - 1);
        }

        System.out.println();
        System.out.println("--- Latency Stats (ms, ACK-based) ---");
        System.out.printf(Locale.US, "Mean: %.2f%n", mean);
        System.out.printf(Locale.US, "Median: %.0f%n", median);
        System.out.printf(Locale.US, "P95: %.0f%n", p95);
        System.out.printf(Locale.US, "P99: %.0f%n", p99);
        System.out.printf(Locale.US, "Min: %.2f%n", (double) min);
        System.out.printf(Locale.US, "Max: %.2f%n", (double) max);

        // Type distribution
        long t = textCnt.get(), j = joinCnt.get(), l = leaveCnt.get();
        long total = t + j + l;
        System.out.println();
        System.out.println("--- Message Type Distribution (ACKs) ---");
        if (total == 0) {
            System.out.println("(no data)");
        } else {
            System.out.printf(Locale.US, "TEXT : %d (%.1f%%)%n", t, pct(t, total));
            System.out.printf(Locale.US, "JOIN : %d (%.1f%%)%n", j, pct(j, total));
            System.out.printf(Locale.US, "LEAVE: %d (%.1f%%)%n", l, pct(l, total));
        }

        // Per-room throughput (counts)
        System.out.println();
        System.out.println("--- Throughput per Room (ACKs) ---");
        synchronized (roomAcks) {
            for (int r = 1; r <= roomCount; r++) {
                System.out.printf("Room %2d: %d%n", r, roomAcks[r]);
            }
        }
    }

    // Helpers
    // null-safe upper-case
    private static String safeUpper(String s) {
        return s == null ? "" : s.toUpperCase(Locale.ROOT);
    }
    // percent (0.0 to 100.0)
    private static double pct(long part, long total) {
        return total == 0 ? 0.0 : (part * 100.0 / total);
    }
    // percentile from sorted list (0-100)
    private static double percentile(List<Long> sorted, int p) {
        if (sorted.isEmpty()) return 0.0;
        if (p <= 0) return sorted.get(0);
        if (p >= 100) return sorted.get(sorted.size() - 1);
        double idx = (p / 100.0) * (sorted.size() - 1);
        int i = (int) Math.floor(idx);
        int j = Math.min(i + 1, sorted.size() - 1);
        double w = idx - i;
        return sorted.get(i) * (1 - w) + sorted.get(j) * w;
    }
}
