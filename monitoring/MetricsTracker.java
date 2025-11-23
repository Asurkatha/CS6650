package com.chatflow.server.metrics;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

public class MetricsTracker {

    // Global metrics
    private static final LongAdder totalReceived = new LongAdder();
    private static final LongAdder totalPublished = new LongAdder();
    private static final LongAdder totalConfirmed = new LongAdder();
    private static final LongAdder totalConsumed = new LongAdder();
    private static final LongAdder totalBroadcast = new LongAdder();
    private static final LongAdder totalDuplicates = new LongAdder();
    private static final LongAdder totalRetries = new LongAdder();
    private static final LongAdder totalFailures = new LongAdder();

    // Per-room metrics
    private static final ConcurrentHashMap<String, RoomMetrics> roomMetrics = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, LatencyTracker> latencyTrackers = new ConcurrentHashMap<>();

    public static void recordReceived(String roomId) {
        totalReceived.increment();
        getRoomMetrics(roomId).received.increment();
    }

    public static void recordPublish(String roomId) {
        totalPublished.increment();
        getRoomMetrics(roomId).published.increment();
    }

    public static void recordConfirm(String roomId) {
        totalConfirmed.increment();
        getRoomMetrics(roomId).confirmed.increment();
    }

    public static void recordConsume(String roomId) {
        totalConsumed.increment();
        getRoomMetrics(roomId).consumed.increment();
    }

    public static void recordBroadcast(String roomId, int recipientCount) {
        totalBroadcast.increment();
        getRoomMetrics(roomId).broadcast.increment();
        getRoomMetrics(roomId).totalRecipients.addAndGet(recipientCount);
    }

    public static void recordDuplicate(String roomId) {
        totalDuplicates.increment();
        getRoomMetrics(roomId).duplicates.increment();
    }

    public static void recordRetry(String roomId) {
        totalRetries.increment();
        getRoomMetrics(roomId).retries.increment();
    }

    public static void recordFailure(String roomId) {
        totalFailures.increment();
        getRoomMetrics(roomId).failures.increment();
    }

    public static void recordLatency(String roomId, long latencyMs) {
        getLatencyTracker(roomId).record(latencyMs);
    }

    private static RoomMetrics getRoomMetrics(String roomId) {
        return roomMetrics.computeIfAbsent(roomId, k -> new RoomMetrics());
    }

    private static LatencyTracker getLatencyTracker(String roomId) {
        return latencyTrackers.computeIfAbsent(roomId, k -> new LatencyTracker());
    }

    public static MetricsSnapshot getSnapshot() {
        MetricsSnapshot snapshot = new MetricsSnapshot();
        snapshot.totalReceived = totalReceived.sum();
        snapshot.totalPublished = totalPublished.sum();
        snapshot.totalConfirmed = totalConfirmed.sum();
        snapshot.totalConsumed = totalConsumed.sum();
        snapshot.totalBroadcast = totalBroadcast.sum();
        snapshot.totalDuplicates = totalDuplicates.sum();
        snapshot.totalRetries = totalRetries.sum();
        snapshot.totalFailures = totalFailures.sum();
        return snapshot;
    }

    public static long getTotalReceived() {
        return totalReceived.sum();
    }

    // Inner classes - THIS IS WHAT WAS MISSING
    private static class RoomMetrics {
        final LongAdder published = new LongAdder();
        final LongAdder received = new LongAdder();
        final LongAdder confirmed = new LongAdder();
        final LongAdder consumed = new LongAdder();
        final LongAdder broadcast = new LongAdder();
        final AtomicLong totalRecipients = new AtomicLong(0);
        final LongAdder duplicates = new LongAdder();
        final LongAdder retries = new LongAdder();
        final LongAdder failures = new LongAdder();
    }

    private static class LatencyTracker {
        private final ConcurrentHashMap<Integer, AtomicLong> buckets = new ConcurrentHashMap<>();
        private final AtomicLong count = new AtomicLong(0);
        private final AtomicLong sum = new AtomicLong(0);

        void record(long latencyMs) {
            count.incrementAndGet();
            sum.addAndGet(latencyMs);
            int bucket = (int) Math.min(latencyMs / 10, 100);
            buckets.computeIfAbsent(bucket, k -> new AtomicLong(0)).incrementAndGet();
        }
    }

    public static class MetricsSnapshot {
        public long totalReceived;
        public long totalPublished;
        public long totalConfirmed;
        public long totalConsumed;
        public long totalBroadcast;
        public long totalDuplicates;
        public long totalRetries;
        public long totalFailures;
    }
}
