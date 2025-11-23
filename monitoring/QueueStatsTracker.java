package com.chatflow.server.metrics;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Tracks queue depth statistics (latest, average, peak) for each queue and aggregates global stats.
 */
public class QueueStatsTracker {

    private static class Stats {
        final LongAdder sum = new LongAdder();
        final AtomicLong samples = new AtomicLong();
        final AtomicLong peak = new AtomicLong();
        final AtomicLong latest = new AtomicLong();
    }

    private final ConcurrentHashMap<String, Stats> byQueue = new ConcurrentHashMap<>();

    public void record(String queueName, long depth) {
        Stats stats = byQueue.computeIfAbsent(queueName, q -> new Stats());
        stats.sum.add(depth);
        stats.samples.incrementAndGet();
        stats.latest.set(depth);
        stats.peak.getAndUpdate(prev -> Math.max(prev, depth));
    }

    public QueueSnapshot snapshot() {
        long totalDepth = 0L;
        long totalSum = 0L;
        long totalSamples = 0L;
        long peak = 0L;

        Map<String, Long> latestPerQueue = new ConcurrentHashMap<>();

        for (Map.Entry<String, Stats> entry : byQueue.entrySet()) {
            Stats stats = entry.getValue();
            long latest = stats.latest.get();
            long queuePeak = stats.peak.get();
            long queueSamples = stats.samples.get();
            long queueSum = stats.sum.sum();

            totalDepth += latest;
            totalSum += queueSum;
            totalSamples += queueSamples;
            peak = Math.max(peak, queuePeak);

            latestPerQueue.put(entry.getKey(), latest);
        }

        double average = totalSamples == 0 ? 0.0 : (double) totalSum / totalSamples;

        return new QueueSnapshot(totalDepth, average, peak, Collections.unmodifiableMap(latestPerQueue));
    }

    public static class QueueSnapshot {
        public final long latestTotalDepth;
        public final double averageDepth;
        public final long peakDepth;
        public final Map<String, Long> latestPerQueue;

        private QueueSnapshot(long latestTotalDepth,
                              double averageDepth,
                              long peakDepth,
                              Map<String, Long> latestPerQueue) {
            this.latestTotalDepth = latestTotalDepth;
            this.averageDepth = averageDepth;
            this.peakDepth = peakDepth;
            this.latestPerQueue = latestPerQueue;
        }
    }
}
