package com.chatflow.client.part1;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Arrays;
import java.util.Map;

/**
 * Tracks latency metrics for performance analysis
 *
 * Collects:
 * - Send latencies (time to send message)
 * - Round-trip latencies (send to ACK)
 * - Percentiles (p50, p95, p99)
 * - Min/Max/Average
 */
public class LatencyTracker {

    // Use array-based bucketing for memory efficiency with high volume
    private static final int BUCKET_SIZE = 10000; // Store up to 10K samples per bucket
    private final long[][] sendLatenciesBuckets;
    private final long[][] rttLatenciesBuckets;
    private final AtomicLong sendLatencyCount = new AtomicLong(0);
    private final AtomicLong rttLatencyCount = new AtomicLong(0);
    private int currentSendBucket = 0;
    private int currentRttBucket = 0;
    private int sendBucketIndex = 0;
    private int rttBucketIndex = 0;

    // Track send times for RTT calculation
    private final ConcurrentHashMap<String, Long> sendTimes = new ConcurrentHashMap<>();

    // Statistics
    private final AtomicLong totalSendLatency = new AtomicLong(0);
    private final AtomicLong totalRttLatency = new AtomicLong(0);
    private volatile long minSendLatency = Long.MAX_VALUE;
    private volatile long maxSendLatency = 0;
    private volatile long minRttLatency = Long.MAX_VALUE;
    private volatile long maxRttLatency = 0;

    // Sampling rate (to avoid memory overflow on very large tests)
    private final int samplingRate;
    private final AtomicLong sampleCounter = new AtomicLong(0);

    public LatencyTracker(int expectedMessages) {
        // Allocate buckets based on expected message count
        // For large tests (>100K), use sampling to keep memory reasonable
        if (expectedMessages > 100000) {
            this.samplingRate = 10; // Sample 1 in 10 messages
            int numBuckets = Math.max(1, (expectedMessages / samplingRate / BUCKET_SIZE) + 1);
            this.sendLatenciesBuckets = new long[numBuckets][BUCKET_SIZE];
            this.rttLatenciesBuckets = new long[numBuckets][BUCKET_SIZE];
        } else {
            this.samplingRate = 1; // Sample every message
            int numBuckets = Math.max(1, (expectedMessages / BUCKET_SIZE) + 1);
            this.sendLatenciesBuckets = new long[numBuckets][BUCKET_SIZE];
            this.rttLatenciesBuckets = new long[numBuckets][BUCKET_SIZE];
        }
    }

    /**
     * Record send latency (time to send message to server)
     */
    public void recordSendLatency(String messageId, long latencyNs) {
        // Update statistics always
        totalSendLatency.addAndGet(latencyNs);
        sendLatencyCount.incrementAndGet();
        updateMinMax(latencyNs, true);

        // Sample for percentile calculation
        if (sampleCounter.incrementAndGet() % samplingRate == 0) {
            synchronized (this) {
                if (sendBucketIndex >= BUCKET_SIZE) {
                    currentSendBucket++;
                    sendBucketIndex = 0;
                    if (currentSendBucket >= sendLatenciesBuckets.length) {
                        return; // Overflow, stop sampling
                    }
                }
                sendLatenciesBuckets[currentSendBucket][sendBucketIndex++] = latencyNs;
            }
        }

        // Track send time for RTT calculation
        sendTimes.put(messageId, System.nanoTime());
    }

    /**
     * Record ACK received (calculate round-trip time)
     */
    public void recordAck(String messageId) {
        Long sendTime = sendTimes.remove(messageId);
        if (sendTime != null) {
            long rttNs = System.nanoTime() - sendTime;
            totalRttLatency.addAndGet(rttNs);
            rttLatencyCount.incrementAndGet();
            updateMinMax(rttNs, false);

            // Sample for percentile calculation
            if (rttLatencyCount.get() % samplingRate == 0) {
                synchronized (this) {
                    if (rttBucketIndex >= BUCKET_SIZE) {
                        currentRttBucket++;
                        rttBucketIndex = 0;
                        if (currentRttBucket >= rttLatenciesBuckets.length) {
                            return; // Overflow, stop sampling
                        }
                    }
                    rttLatenciesBuckets[currentRttBucket][rttBucketIndex++] = rttNs;
                }
            }
        }
    }

    private synchronized void updateMinMax(long latencyNs, boolean isSend) {
        if (isSend) {
            if (latencyNs < minSendLatency) minSendLatency = latencyNs;
            if (latencyNs > maxSendLatency) maxSendLatency = latencyNs;
        } else {
            if (latencyNs < minRttLatency) minRttLatency = latencyNs;
            if (latencyNs > maxRttLatency) maxRttLatency = latencyNs;
        }
    }

    /**
     * Get comprehensive latency statistics
     */
    public LatencyStats getStats() {
        // Collect all samples from buckets
        long[] sendSamples = collectSamples(sendLatenciesBuckets, currentSendBucket, sendBucketIndex);
        long[] rttSamples = collectSamples(rttLatenciesBuckets, currentRttBucket, rttBucketIndex);

        // Sort for percentile calculation
        Arrays.sort(sendSamples);
        Arrays.sort(rttSamples);

        return new LatencyStats(
            // Send latencies
            sendLatencyCount.get(),
            sendLatencyCount.get() > 0 ? totalSendLatency.get() / sendLatencyCount.get() : 0,
            minSendLatency == Long.MAX_VALUE ? 0 : minSendLatency,
            maxSendLatency,
            calculatePercentile(sendSamples, 0.50),
            calculatePercentile(sendSamples, 0.95),
            calculatePercentile(sendSamples, 0.99),

            // RTT latencies
            rttLatencyCount.get(),
            rttLatencyCount.get() > 0 ? totalRttLatency.get() / rttLatencyCount.get() : 0,
            minRttLatency == Long.MAX_VALUE ? 0 : minRttLatency,
            maxRttLatency,
            calculatePercentile(rttSamples, 0.50),
            calculatePercentile(rttSamples, 0.95),
            calculatePercentile(rttSamples, 0.99),

            samplingRate
        );
    }

    private long[] collectSamples(long[][] buckets, int lastBucket, int lastIndex) {
        int totalSize = (lastBucket * BUCKET_SIZE) + lastIndex;
        long[] result = new long[totalSize];
        int pos = 0;

        for (int i = 0; i < lastBucket; i++) {
            System.arraycopy(buckets[i], 0, result, pos, BUCKET_SIZE);
            pos += BUCKET_SIZE;
        }

        if (lastIndex > 0) {
            System.arraycopy(buckets[lastBucket], 0, result, pos, lastIndex);
        }

        return result;
    }

    private long calculatePercentile(long[] sortedSamples, double percentile) {
        if (sortedSamples.length == 0) return 0;
        int index = (int) Math.ceil(percentile * sortedSamples.length) - 1;
        index = Math.max(0, Math.min(index, sortedSamples.length - 1));
        return sortedSamples[index];
    }

    /**
     * Container for latency statistics
     */
    public static class LatencyStats {
        // Send latencies (client → server)
        public final long sendCount;
        public final long sendAvgNs;
        public final long sendMinNs;
        public final long sendMaxNs;
        public final long sendP50Ns;
        public final long sendP95Ns;
        public final long sendP99Ns;

        // Round-trip latencies (send → ACK)
        public final long rttCount;
        public final long rttAvgNs;
        public final long rttMinNs;
        public final long rttMaxNs;
        public final long rttP50Ns;
        public final long rttP95Ns;
        public final long rttP99Ns;

        public final int samplingRate;

        public LatencyStats(
            long sendCount, long sendAvgNs, long sendMinNs, long sendMaxNs,
            long sendP50Ns, long sendP95Ns, long sendP99Ns,
            long rttCount, long rttAvgNs, long rttMinNs, long rttMaxNs,
            long rttP50Ns, long rttP95Ns, long rttP99Ns,
            int samplingRate
        ) {
            this.sendCount = sendCount;
            this.sendAvgNs = sendAvgNs;
            this.sendMinNs = sendMinNs;
            this.sendMaxNs = sendMaxNs;
            this.sendP50Ns = sendP50Ns;
            this.sendP95Ns = sendP95Ns;
            this.sendP99Ns = sendP99Ns;

            this.rttCount = rttCount;
            this.rttAvgNs = rttAvgNs;
            this.rttMinNs = rttMinNs;
            this.rttMaxNs = rttMaxNs;
            this.rttP50Ns = rttP50Ns;
            this.rttP95Ns = rttP95Ns;
            this.rttP99Ns = rttP99Ns;

            this.samplingRate = samplingRate;
        }

        // Convert nanoseconds to milliseconds
        public double sendAvgMs() { return sendAvgNs / 1_000_000.0; }
        public double sendMinMs() { return sendMinNs / 1_000_000.0; }
        public double sendMaxMs() { return sendMaxNs / 1_000_000.0; }
        public double sendP50Ms() { return sendP50Ns / 1_000_000.0; }
        public double sendP95Ms() { return sendP95Ns / 1_000_000.0; }
        public double sendP99Ms() { return sendP99Ns / 1_000_000.0; }

        public double rttAvgMs() { return rttAvgNs / 1_000_000.0; }
        public double rttMinMs() { return rttMinNs / 1_000_000.0; }
        public double rttMaxMs() { return rttMaxNs / 1_000_000.0; }
        public double rttP50Ms() { return rttP50Ns / 1_000_000.0; }
        public double rttP95Ms() { return rttP95Ns / 1_000_000.0; }
        public double rttP99Ms() { return rttP99Ns / 1_000_000.0; }

        @Override
        public String toString() {
            return String.format(
                "Send Latency: avg=%.2fms, p50=%.2fms, p95=%.2fms, p99=%.2fms, min=%.2fms, max=%.2fms (n=%d)%n" +
                "RTT Latency:  avg=%.2fms, p50=%.2fms, p95=%.2fms, p99=%.2fms, min=%.2fms, max=%.2fms (n=%d)%n" +
                "Sampling: 1 in %d messages",
                sendAvgMs(), sendP50Ms(), sendP95Ms(), sendP99Ms(), sendMinMs(), sendMaxMs(), sendCount,
                rttAvgMs(), rttP50Ms(), rttP95Ms(), rttP99Ms(), rttMinMs(), rttMaxMs(), rttCount,
                samplingRate
            );
        }
    }
}
