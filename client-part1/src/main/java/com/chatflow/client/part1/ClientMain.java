package com.chatflow.client.part1;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.LoggerFactory;
/**
 * âœ… SIMPLIFIED: Single-phase chat client performance test
 *
 * Removed warmup phase - just runs one test phase with configurable parameters
 *
 * Usage:
 *   java ClientMain [wsBase] [threads] [totalMessages] [maxInFlight]
 *
 * Examples:
 *   java ClientMain ws://localhost:8080 50 5000 20000
 *   java ClientMain ws://44.252.101.120:8080 100 10000 50000
 *
 * Default parameters:
 *   - wsBase: ws://localhost:8080
 *   - threads: 50
 *   - totalMessages: 5000
 *   - maxInFlight: 50000
 */
public class ClientMain {
    static {LoggerFactory.getILoggerFactory();}
    private static final String DEFAULT_WS_BASE = "ws://localhost:8080";
    private static final int DEFAULT_THREADS = 64;
    private static final int DEFAULT_TOTAL_MESSAGES = 500000;
    private static final int DEFAULT_MAX_IN_FLIGHT = 50000;
    private static final int ROOM_COUNT = 20;
    private static final int QUEUE_CAPACITY = 1000000;
    public static void main(String[] args) throws Exception {
        // Parse arguments
        final String wsBase = args.length > 0 ? args[0] : DEFAULT_WS_BASE;
        final int threads = args.length > 1 ? Integer.parseInt(args[1]) : DEFAULT_THREADS;
        final int totalMessages = args.length > 2 ? Integer.parseInt(args[2]) : DEFAULT_TOTAL_MESSAGES;
        final int maxInFlight = args.length > 3 ? Integer.parseInt(args[3]) : DEFAULT_MAX_IN_FLIGHT;

        System.out.println("         ChatFlow Client - Performance Test                ");
        System.out.printf("WebSocket: %s%n", wsBase);
        System.out.printf("Threads: %d%n", threads);
        System.out.printf("Requested Messages: %,d%n", totalMessages);
        System.out.printf("Max In-Flight: %,d%n", maxInFlight);
        System.out.printf("Rooms: %d%n", ROOM_COUNT);
        System.out.println();

        // Generate messages once so we can track messageIds for ACK verification
        MessageGenerator generator = new MessageGenerator();
        List<String> messages = generator.generateMessages(totalMessages);
        int expectedMessages = messages.size();
        System.out.printf("Generated %,d messages%n", expectedMessages);

        ConcurrentMap<String, String> pendingAcks = new ConcurrentHashMap<>();

        for (String json : messages) {
            try {
                JsonObject obj = JsonParser.parseString(json).getAsJsonObject();
                if (obj.has("messageId")) {
                    pendingAcks.put(obj.get("messageId").getAsString(), json);
                }
            } catch (Exception ignore) {
                // Skip malformed entries
            }
        }

        // Prepare results container and progress logger
        CountDownLatch allAcks = new CountDownLatch(expectedMessages);

        TestResults results = new TestResults();
        results.pendingAcks = pendingAcks;

        ScheduledExecutorService progressLogger = startProgressLogger(results);

        // Run the main benchmark
        runTest(wsBase, threads, messages, allAcks, pendingAcks, maxInFlight, results);

        // Wait for ACKs
        boolean allAcked = allAcks.await(300, TimeUnit.SECONDS);
        Instant ackDone = Instant.now();

        if (!allAcked) {
            System.out.printf("TIMEOUT: %,d ACKs still pending%n", allAcks.getCount());
        }

        // Calculate metrics
        long runtimeMs = Math.max(1, Duration.between(results.t0, ackDone).toMillis());
        long ackCount = results.acksOk.get();
        long sendsOk = results.sendsOk.get();
        double throughput = (sendsOk * 1000.0) / runtimeMs;

        int missingAcks = results.pendingAcks != null ? results.pendingAcks.size() : 0;
        if (missingAcks > 0) {
            System.out.printf("Missing ACKs (unique messageIds not seen): %,d%n", missingAcks);
            int shown = 0;
            for (String messageId : results.pendingAcks.keySet()) {
                System.out.printf("  - %s%n", messageId);
                if (++shown >= Math.min(5, missingAcks)) {
                    break;
                }
            }
            if (missingAcks > 5) {
                System.out.println("  ...");
            }
        }

        try {
            // Print results
            System.out.println();
            System.out.println("                    TEST RESULTS                           ");
            System.out.printf("Messages Sent:     %,d%n", sendsOk);
            System.out.printf("Messages Failed:   %,d%n", results.sendFails.get());
            System.out.printf("ACKs Received:     %,d%n", ackCount);
            System.out.printf("Runtime:          %.3f s%n", runtimeMs / 1000.0);
            System.out.printf("Throughput:       %.2f msg/s%n", throughput);
            System.out.printf("Connections:      %,d%n", results.totalConnections.get());
            System.out.printf("Reconnections:    %,d%n", results.reconnections.get());
            System.out.printf("Conn Failures:    %,d%n", results.connectionFailures.get());
            System.out.printf("Send Retries:     %,d%n", results.retryAttempts.get());
        } finally {
            try {
                progressLogger.shutdownNow();
                progressLogger.awaitTermination(2, TimeUnit.SECONDS);
            } catch (InterruptedException ignore) {
                Thread.currentThread().interrupt();
            }
            SenderWorker.shutdownAllClients();
        }

        System.exit(0);
    }

    static class TestResults {
        final AtomicLong acksOk = new AtomicLong();
        final AtomicLong sendsOk = new AtomicLong();
        final AtomicLong sendFails = new AtomicLong();
        final AtomicInteger totalConnections = new AtomicInteger();
        final AtomicInteger reconnections = new AtomicInteger();
        final AtomicLong connectionFailures = new AtomicLong();
        final AtomicLong retryAttempts = new AtomicLong();
        ConcurrentMap<String, String> pendingAcks;
        Instant t0, t1;
    }

    private static ScheduledExecutorService startProgressLogger(TestResults results) {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(() -> {
            long sent = results.sendsOk.get();
            long failed = results.sendFails.get();
            long acks = results.acksOk.get();
            int pending = results.pendingAcks != null ? results.pendingAcks.size() : 0;
            long connFails = results.connectionFailures.get();
            long retries = results.retryAttempts.get();
            String pendingSample = "";
            if (pending > 0 && pending <= 10 && results.pendingAcks != null) {
                try {
                    pendingSample = results.pendingAcks.keySet().stream()
                            .limit(3)
                            .collect(Collectors.joining(","));
                } catch (Exception ignore) {
                    pendingSample = "n/a";
                }
            }
            System.out.printf(
                    "[CLIENT] Sent: %,d | Failed: %,d | ACKs: %,d | Pending: %,d | ConnFail: %,d | Retries: %,d%s%n",
                    sent, failed, acks, pending, connFails, retries,
                    pendingSample.isEmpty() ? "" : " | PendingIds: " + pendingSample
            );
        }, 5, 5, TimeUnit.SECONDS);
        return scheduler;
    }

    /**
     * Drives the main load phase and bounded retry loops until ACKs drain.
     */
    private static void runTest(String wsBase, int threads, List<String> messages,
                                CountDownLatch allAcks, ConcurrentMap<String, String> pendingAcks,
                                int maxInFlight, TestResults results) throws Exception {
        SenderWorker.configureMaxInFlight(maxInFlight);

        results.t0 = Instant.now();
        BlockingQueue<String> primaryQueue = new LinkedBlockingQueue<>(QUEUE_CAPACITY);
        primaryQueue.addAll(messages);

        runSendPhase(wsBase, threads, primaryQueue, allAcks, pendingAcks, results, true);

        int attempts = 1;
        while (!pendingAcks.isEmpty() && attempts <= 10) {
            int remaining = pendingAcks.size();
            SenderWorker.releasePermits(remaining);
            BlockingQueue<String> retryQueue = new LinkedBlockingQueue<>(QUEUE_CAPACITY);
            retryQueue.addAll(pendingAcks.values());
            runSendPhase(wsBase, threads, retryQueue, allAcks, pendingAcks, results, false);
            attempts++;
        }
        results.t1 = Instant.now();
        if (!pendingAcks.isEmpty()) {
            System.out.printf("[CLIENT] WARNING: %,d messages still pending after retries%n", pendingAcks.size());
        }        
    }

    /**
     * Sends a queue of messages using a worker pool honouring the inflight limits.
     */
    private static void runSendPhase(String wsBase,
                                     int threads,
                                     BlockingQueue<String> queue,
                                     CountDownLatch allAcks,
                                     ConcurrentMap<String, String> pendingAcks,
                                     TestResults results,
                                     boolean countAsNew) throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        for (int i = 0; i < threads; i++) {
            SenderWorker worker = new SenderWorker(
                    wsBase,
                    queue,
                    SenderWorker.Mode.RANDOM_ROOM_PER_MESSAGE,
                    i + 1,
                    results.sendsOk,
                    results.sendFails,
                    results.totalConnections,
                    results.reconnections,
                    results.acksOk,
                    allAcks,
                    pendingAcks,
                    results.connectionFailures,
                    results.retryAttempts,
                    countAsNew
            );
            executor.submit(worker);
        }
        executor.shutdown();
        executor.awaitTermination(180, TimeUnit.SECONDS);
    }    
}
