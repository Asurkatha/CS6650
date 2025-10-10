package com.chatflow.client.part1;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Main class for the chat client performance test.
 * <p>
 * This client connects to a WebSocket chat server and sends messages to multiple chat rooms.
 * It supports two phases: a warmup phase with fixed room assignment per thread,
 * and a main phase with random room assignment per message.
 * Usage:
 * java ClientMain [wsBase] [warmThreads] [warmMsgsPerThread] [mainThreads] [mainTotalMessages]
 * Default parameters:
 * - wsBase: ws://localhost:8080 (can be changed to point to EC2 instance)
 * - warmThreads: 32
 * - warmMsgsPerThread: 1000 (total warmup messages = 32,000)
 * - mainThreads: 50
 * - mainTotalMessages: 500000
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
        final int mainTotalMessages   = args.length > 4 ? Integer.parseInt(args[4]) : 500000;

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
                true,         // fixed room per thread (round-robin)
                warmAcks      // ACK latch for warmup
        );

        boolean warmAllAcked = warmAcks.await(120, TimeUnit.SECONDS);
        Instant warmAckDone = Instant.now(); // time of last ACK or timeout
        if (!warmAllAcked) {
            System.out.printf("WARMUP WAIT: timed out with %,d ACKs still pending%n",//failed ECHOs
                    warmAcks.getCount());
        }
        long warmE2Ems  = Math.max(1, Duration.between(warm.t0, warmAckDone).toMillis());
        long warmAckCnt = warm.acksOk.get();
        double warmThr  = (warmAckCnt * 1000.0) / warmE2Ems;

        System.out.println();
        System.out.println("=== Warmup Results (ACK-based, end-to-end) ===");
        System.out.printf("ACKs Ok : %d%n", warmAckCnt);
        System.out.printf("Sends Ok: %d, Sends Failed: %d%n", warm.sendsOk.get(), warm.sendFails.get());
        System.out.printf("Runtime (send -> last ACK): %.3f s%n", warmE2Ems / 1000.0);
        System.out.printf("Throughput (ACKs / e2e): %.2f msg/s%n", warmThr);
        System.out.printf("Connections: %d, Reconnections: %d%n",
                warm.totalConnections.get(), warm.reconnections.get());

        // MAIN
        CountDownLatch mainAcks = new CountDownLatch(mainTotalMessages);
        PhaseTotals main = runPhase(
                "Main",
                wsBase,
                mainThreads,
                mainTotalMessages,
                SenderWorker.Mode.RANDOM_ROOM_PER_MESSAGE,
                false,
                mainAcks
        );

        boolean mainAllAcked = mainAcks.await(180, TimeUnit.SECONDS);
        Instant mainAckDone = Instant.now(); // time of last ACK or timeout
        if (!mainAllAcked) {
            System.out.printf("MAIN WAIT: timed out with %,d ACKs still pending%n", //failed ECHOs
                    mainAcks.getCount());
        }

        long mainE2Ems  = Math.max(1, Duration.between(main.t0, mainAckDone).toMillis());
        long mainAckCnt = main.acksOk.get();
        double mainThr  = (mainAckCnt * 1000.0) / mainE2Ems;

        System.out.println();
        System.out.println("=== Main Results (ACK-based, end-to-end) ===");
        System.out.printf("ACKs Ok : %d%n", mainAckCnt);
        System.out.printf("Sends Ok: %d, Sends Failed: %d%n", main.sendsOk.get(), main.sendFails.get());
        System.out.printf("Runtime (send -> last ACK): %.3f s%n", mainE2Ems / 1000.0);
        System.out.printf("Throughput (ACKs / e2e): %.2f msg/s%n", mainThr);
        System.out.printf("Connections: %d, Reconnections: %d%n",
                main.totalConnections.get(), main.reconnections.get());
    }

    // internals

    static class PhaseTotals {
        final AtomicLong acksOk = new AtomicLong();
        final AtomicLong sendsOk = new AtomicLong();
        final AtomicLong sendFails = new AtomicLong();
        final AtomicInteger totalConnections = new AtomicInteger();
        final AtomicInteger reconnections = new AtomicInteger();
        Instant t0, t1; // send window (kept for potential debugging)
    }

    private static PhaseTotals runPhase(String label,
                                        String wsBase,
                                        int threads,
                                        int totalMessages,
                                        SenderWorker.Mode mode,
                                        boolean fixedRoomRoundRobin,
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

        // Prefill to avoid initial starvation
        while (queue.size() < Math.min(queueCap / 3, Math.max(5000, totalMessages / 20))) {
            if (!genThread.isAlive()) break;
            Thread.sleep(5);
        }

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        List<Future<?>> futures = new ArrayList<>(threads);

        totals.t0 = Instant.now(); // start of send window

        for (int i = 0; i < threads; i++) {
            int fixedRoom = fixedRoomRoundRobin ? ((i % ROOM_COUNT) + 1) : 1;
            int share     = totalMessages / threads + (i < (totalMessages % threads) ? 1 : 0);

            SenderWorker worker = new SenderWorker(
                    wsBase, queue, share, mode, fixedRoom,
                    totals.sendsOk, totals.sendFails,
                    totals.totalConnections, totals.reconnections,
                    totals.acksOk,
                    allAcks
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
