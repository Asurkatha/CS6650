package com.chatflow.client.part2;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Main client application that runs warmup and main phases of message sending to a WebSocket server.
 * It supports configurable parameters for threads, message counts, and modes of operation.
 * The application collects and prints detailed metrics including throughput and connection stats.
 */
public class ClientMain {

    private static final String DEFAULT_WS_BASE = "ws://localhost:8080";
    private static final int ROOM_COUNT = 20;

    private static final int QUEUE_CAP_WARMUP = 100_000;
    private static final int QUEUE_CAP_MAIN   = 300_000;

    public static void main(String[] args) throws Exception {
        // Args: [wsBase] [warmThreads] [warmMsgsPerThread] [mainThreads] [mainTotalMessages]
        final String wsBase           = args.length > 0 ? args[0] : DEFAULT_WS_BASE;
        final int warmThreads         = args.length > 1 ? Integer.parseInt(args[1]) : 32;
        final int warmMsgsPerThread   = args.length > 2 ? Integer.parseInt(args[2]) : 1000;
        final int mainThreads         = args.length > 3 ? Integer.parseInt(args[3]) : 50;
        final int mainTotalMessages   = args.length > 4 ? Integer.parseInt(args[4]) : 500_000;

        System.out.printf("WS base=%s%n", wsBase);

        // WARMUP
        final int warmTotalMessages = warmThreads * warmMsgsPerThread;
        CountDownLatch warmAcks = new CountDownLatch(warmTotalMessages);

        PhaseTotals warm = runPhase(
                "Warmup",
                wsBase,
                warmThreads,
                warmTotalMessages,
                SenderWorker.Mode.FIXED_ROOM,
                true,      // fixed room per thread (round-robin)
                null,      // no metrics file for warmup
                warmAcks   // ACK latch enabled in warmup
        );

        // wait for all warmup ACKs
        boolean warmAllAcked = warmAcks.await(30, TimeUnit.SECONDS);
        Instant warmAckDone = Instant.now();
        if (!warmAllAcked) {
            System.out.printf("WARMUP WAIT: timed out with %,d ACKs still pending%n",
                    warmAcks.getCount());
        }

        long warmE2Ems   = Math.max(1, Duration.between(warm.t0, warmAckDone).toMillis());
        long warmAckCnt  = warm.acksOk.get();
        double warmThr   = (warmAckCnt * 1000.0) / warmE2Ems;

        System.out.println();
        System.out.println("=== Warmup Results (ACK-based, end-to-end) ===");
        System.out.printf("ACKs Ok : %d%n", warmAckCnt);
        System.out.printf("Sends Ok: %d, Sends Failed: %d%n", warm.sendsOk.get(), warm.sendFails.get());
        System.out.printf("LostPendings(on close): %d%n", warm.lostPendings.get()); //if any pending ACKs were lost on close
        System.out.printf("Runtime (send -> last ACK): %.3f s%n", warmE2Ems / 1000.0);
        System.out.printf("Throughput (ACKs / e2e): %.2f msg/s%n", warmThr);
        System.out.printf("Connections: %d, Reconnections: %d%n",
                warm.totalConnections.get(), warm.reconnections.get());

        // MAIN
        MetricsCollector metrics = new MetricsCollector(ROOM_COUNT, "metrics.csv");
        metrics.open();
        CountDownLatch allAcks = new CountDownLatch(mainTotalMessages);

        PhaseTotals totals = runPhase(
                "Main",
                wsBase,
                mainThreads,
                mainTotalMessages,
                SenderWorker.Mode.RANDOM_ROOM_PER_MESSAGE,
                false,
                metrics,
                allAcks
        );

        // Wait for all ACKs (true end-to-end)
        boolean allAcked = allAcks.await(60, TimeUnit.SECONDS);
        Instant tAckDone = Instant.now();
        if (!allAcked) {
            System.out.printf("WAIT: timed out with %,d ACKs still pending%n", allAcks.getCount());
        }

        metrics.close();
        // Final, ACK-based end-to-end summary
        long e2eMillis = Math.max(1, Duration.between(totals.t0, tAckDone).toMillis());
        long ackCount  = totals.acksOk.get();

        System.out.println();
        System.out.println("=== Main Phase (ACK-based, end-to-end) ===");
        System.out.printf("ACKs Ok : %d%n", ackCount);
        System.out.printf("Sends Ok: %d, Sends Failed: %d%n", totals.sendsOk.get(), totals.sendFails.get());
        System.out.printf("LostPendings(on close): %d%n", totals.lostPendings.get());
        System.out.printf("Runtime (send -> last ACK): %.3f s%n", e2eMillis / 1000.0);
        System.out.printf("Throughput (ACKs / e2e): %.2f msg/s%n", (ackCount * 1000.0) / e2eMillis);
        System.out.printf("Connections: %d, Reconnections: %d%n",
                totals.totalConnections.get(), totals.reconnections.get());

        // Detailed stats
        metrics.printSummary();
        System.out.println("\nPer-message CSV: metrics.csv\n");
    }

    // internals

    static class PhaseTotals {
        final AtomicLong acksOk = new AtomicLong();
        final AtomicLong sendsOk = new AtomicLong();
        final AtomicLong sendFails = new AtomicLong();
        final AtomicLong lostPendings = new AtomicLong();
        final AtomicInteger totalConnections = new AtomicInteger();
        final AtomicInteger reconnections = new AtomicInteger();
        Instant t0, t1; // send window
    }

    private static PhaseTotals runPhase(String label,
                                        String wsBase,
                                        int threads,
                                        int totalMessages,
                                        SenderWorker.Mode mode,
                                        boolean fixedRoomRoundRobin,
                                        MetricsCollector metrics,
                                        CountDownLatch allAcks) throws Exception {

        System.out.printf("%n=== %s Phase ===%n", label);
        System.out.printf("threads=%d totalMessages=%d mode=%s%n", threads, totalMessages, mode);

        PhaseTotals totals = new PhaseTotals();

        int queueCap = label.equals("Warmup") ? QUEUE_CAP_WARMUP : QUEUE_CAP_MAIN;
        BlockingQueue<String> queue = new ArrayBlockingQueue<>(queueCap);

        // Producer
        MessageGenerator generator = new MessageGenerator(queue, totalMessages);
        Thread genThread = new Thread(generator, label + "-generator");
        genThread.start();

        // Prefill so senders don’t starve at the start
        while (queue.size() < Math.min(queueCap / 3, Math.max(5000, totalMessages / 20))) {
            if (!genThread.isAlive()) break;
            Thread.sleep(5);
        }

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        List<Future<?>> futures = new ArrayList<>(threads);

        totals.t0 = Instant.now();
        if (metrics != null) metrics.setStartMillis(totals.t0.toEpochMilli()); // capture start time

        for (int i = 0; i < threads; i++) {
            int fixedRoom = fixedRoomRoundRobin ? ((i % ROOM_COUNT) + 1) : 1;
            int share     = totalMessages / threads + (i < (totalMessages % threads) ? 1 : 0);

            SenderWorker worker = new SenderWorker(
                    wsBase, queue, share, mode, fixedRoom,
                    totals.sendsOk, totals.sendFails,
                    totals.acksOk, totals.lostPendings,
                    totals.totalConnections, totals.reconnections,
                    allAcks,
                    metrics
            );
            futures.add(pool.submit(worker));
        }

        // Wait for senders and producer to finish pushing all messages
        for (Future<?> f : futures) f.get();
        genThread.join();
        totals.t1 = Instant.now(); // end of send window

        pool.shutdownNow();
        return totals;
    }
}
