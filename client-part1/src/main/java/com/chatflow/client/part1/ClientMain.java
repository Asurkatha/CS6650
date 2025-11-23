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

public class ClientMain {
    static {LoggerFactory.getILoggerFactory();}
    private static final String DEFAULT_WS_BASE = "ws://localhost:8080";
    private static final int DEFAULT_THREADS = 64;
    private static final int DEFAULT_TOTAL_MESSAGES = 500000;
    private static final int DEFAULT_MAX_IN_FLIGHT = 200000; // Increased to prevent deadlock on large tests
    private static final int ROOM_COUNT = 20;
    private static final int QUEUE_CAPACITY = 10000000;

    // Endurance test configuration
    private static final int ENDURANCE_DURATION_MINUTES = 30;
    private static final int DEFAULT_ENDURANCE_RATE = 3000; // messages per second

    public static void main(String[] args) throws Exception {
        // Check for endurance mode
        if (args.length >= 2 && "endurance".equalsIgnoreCase(args[1])) {
            runEnduranceTest(args);
            return;
        }

        // Parse arguments for standard mode
        final String wsBase = args.length > 0 ? args[0] : DEFAULT_WS_BASE;
        final int threads = args.length > 1 ? Integer.parseInt(args[1]) : DEFAULT_THREADS;
        final int totalMessages = args.length > 2 ? Integer.parseInt(args[2]) : DEFAULT_TOTAL_MESSAGES;
        final int maxInFlight = args.length > 3 ? Integer.parseInt(args[3]) : DEFAULT_MAX_IN_FLIGHT;

        // Generate unique testId for this test run
        final String testId = Instant.now().toString();

        System.out.println("         ChatFlow Client - Performance Test                ");
        System.out.printf("WebSocket: %s%n", wsBase);
        System.out.printf("Test ID: %s%n", testId);
        System.out.printf("Threads: %d%n", threads);
        System.out.printf("Requested Messages: %,d%n", totalMessages);
        System.out.printf("Max In-Flight: %,d%n", maxInFlight);
        System.out.printf("Rooms: %d%n", ROOM_COUNT);
        System.out.println();

        // Generate messages once so we can track messageIds for ACK verification
        MessageGenerator generator = new MessageGenerator(testId);
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
        results.testId = testId;  // Store testId in results

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
            System.out.printf("Messages Failed:   %,d%n", results.sendsFailed.get());
            System.out.printf("ACKs Received:     %,d%n", ackCount);
            System.out.printf("Runtime:          %.3f s%n", runtimeMs / 1000.0);
            System.out.printf("Throughput:       %.2f msg/s%n", throughput);
            System.out.printf("Connections:      %,d%n", results.totalConnections.get());
            System.out.printf("Reconnections:    %,d%n", results.reconnections.get());
            System.out.printf("Conn Failures:    %,d%n", results.connectionFailures.get());
            System.out.printf("Send Retries:     %,d%n", results.retryAttempts.get());

            // Wait for DynamoDB writes to complete
            System.out.println();
            System.out.println("=".repeat(60));
            System.out.println("Waiting for DynamoDB writes to complete...");
            System.out.println("(Allowing 30 seconds for batch writer to flush all queues)");
            System.out.println("=".repeat(60));
            Thread.sleep(30000); // Wait 30 seconds for batch writer to finish

            // Automatically trigger query export on server
            System.out.println();
            System.out.println("Triggering automatic query export on server...");
            triggerQueryExport(wsBase, testId);

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
        final AtomicLong sendsFailed = new AtomicLong();  // Renamed from sendFails for consistency
        final AtomicLong timeouts = new AtomicLong();     // Track timeouts separately
        final AtomicInteger totalConnections = new AtomicInteger();
        final AtomicInteger reconnections = new AtomicInteger();
        final AtomicLong connectionFailures = new AtomicLong();
        final AtomicLong retryAttempts = new AtomicLong();
        ConcurrentMap<String, String> pendingAcks;
        String testId;  // Unique test identifier
        Instant t0, t1;
        Instant sendComplete;  // Track when all sends complete (for endurance test)
    }

    private static ScheduledExecutorService startProgressLogger(TestResults results) {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(() -> {
            long sent = results.sendsOk.get();
            long failed = results.sendsFailed.get();
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
                    results.sendsFailed,
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

    private static void fetchAndDisplayQueryResults(String wsBase, String testId) {
        try {
            // Convert ws://host:port to http://host:queryPort and add testId parameter
            String httpUrl = wsBase.replace("ws://", "http://").replace(":8080", ":8081")
                + "/queries?testId=" + testId;

            HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(30))  // Increased for slow connections
                .build();

            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(httpUrl))
                .timeout(Duration.ofMinutes(5))  // 5 minute timeout for large query results
                .GET()
                .build();

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() == 200) {
                JsonObject results = JsonParser.parseString(response.body()).getAsJsonObject();
                displayFormattedQueryResults(results);
            } else {
                System.out.println("Failed to fetch queries. Status: " + response.statusCode());
            }

        } catch (Exception e) {
            System.out.println("Error fetching query results: " + e.getMessage());
        }
    }

    private static void displayFormattedQueryResults(JsonObject results) {
        System.out.println("\n" + "=".repeat(70));
        System.out.println("CORE QUERIES");
        System.out.println("=".repeat(70));

        JsonObject core = results.getAsJsonObject("coreQueries");

        // 1. Room Messages
        JsonObject roomMsg = core.getAsJsonObject("roomMessages");
        System.out.println("\n[1] Get messages for room");
        System.out.printf("    Room ID: %s | Test ID: %s\n",
            roomMsg.get("roomId").getAsString(),
            roomMsg.get("testId").getAsString());
        JsonArray roomMessages = roomMsg.getAsJsonArray("messages");
        int roomMsgCount = roomMessages.size();
        System.out.printf("    Result: %d messages retrieved\n", roomMsgCount);
        if (roomMsgCount > 0) {
            System.out.println("    Messages:");
            for (int i = 0; i < roomMsgCount; i++) {
                JsonObject msg = roomMessages.get(i).getAsJsonObject();
                System.out.printf("      [%s] %s (%s): %s\n",
                    formatTime(msg.get("timestamp").getAsString()),
                    msg.get("username").getAsString(),
                    msg.get("messageType").getAsString(),
                    msg.get("message").getAsString());
            }
        }

        // 2. User History
        JsonObject userHist = core.getAsJsonObject("userHistory");
        System.out.println("\n[2] Get user's message history");
        System.out.printf("    User ID: %s | Test ID: %s\n",
            userHist.get("userId").getAsString(),
            userHist.get("testId").getAsString());
        JsonArray userMessages = userHist.getAsJsonArray("messages");
        int userMsgCount = userMessages.size();
        System.out.printf("    Result: %d messages across all rooms\n", userMsgCount);
        if (userMsgCount > 0) {
            System.out.println("    Messages:");
            for (int i = 0; i < userMsgCount; i++) {
                JsonObject msg = userMessages.get(i).getAsJsonObject();
                System.out.printf("      [%s] Room %s (%s): %s\n",
                    formatTime(msg.get("timestamp").getAsString()),
                    msg.get("roomId").getAsString(),
                    msg.get("messageType").getAsString(),
                    msg.get("message").getAsString());
            }
        }

        // 3. Active Users Count
        JsonObject activeUsers = core.getAsJsonObject("activeUsersCount");
        System.out.println("\n[3] Count active users");
        System.out.printf("    Test ID: %s\n", activeUsers.get("testId").getAsString());
        System.out.printf("    Result: %d unique active users\n", activeUsers.get("count").getAsInt());

        // 4. User Rooms
        JsonObject userRooms = core.getAsJsonObject("userRooms");
        System.out.println("\n[4] Get rooms user has participated in");
        System.out.printf("    User ID: %s | Test ID: %s\n",
            userRooms.get("userId").getAsString(),
            userRooms.get("testId").getAsString());
        JsonArray rooms = userRooms.getAsJsonArray("rooms");
        int roomCount = rooms.size();
        System.out.printf("    Result: Participated in %d room(s)\n", roomCount);
        if (roomCount > 0) {
            System.out.println("    Rooms:");
            for (int i = 0; i < roomCount; i++) {
                JsonObject room = rooms.get(i).getAsJsonObject();
                System.out.printf("      Room %s: %d messages\n",
                    room.get("roomId").getAsString(),
                    room.get("messageCount").getAsInt());
            }
        }

        // Analytics Queries
        System.out.println("\n" + "=".repeat(70));
        System.out.println("ANALYTICS QUERIES");
        System.out.println("=".repeat(70));

        JsonObject analytics = results.getAsJsonObject("analyticsQueries");

        // 1. Messages per hour
        JsonObject msgPerHour = analytics.getAsJsonObject("messagesPerHour");
        System.out.println("\n[1] Messages per hour/day statistics");
        int hourCount = msgPerHour.getAsJsonObject("hourlyStats").size();
        System.out.printf("    Result: Activity data for %d hour(s)\n", hourCount);

        // 2. Most Active Users
        JsonObject activeUsersTop = analytics.getAsJsonObject("mostActiveUsers");
        System.out.println("\n[2] Most active users");
        int topN = activeUsersTop.get("topN").getAsInt();
        int actualUsers = activeUsersTop.getAsJsonArray("users").size();
        System.out.printf("    Top %d users (found %d):\n", topN, actualUsers);
        int count = 0;
        for (JsonElement elem : activeUsersTop.getAsJsonArray("users")) {
            JsonObject user = elem.getAsJsonObject();
            System.out.printf("      #%d: %s (%d messages)\n",
                ++count,
                user.get("userId").getAsString(),
                user.get("messageCount").getAsInt());
            if (count >= 5) break; // Show top 5 only
        }
        if (actualUsers > 5) {
            System.out.printf("      ... and %d more\n", actualUsers - 5);
        }

        // 3. Most Active Rooms
        JsonObject activeRooms = analytics.getAsJsonObject("mostActiveRooms");
        System.out.println("\n[3] Most active rooms");
        int topNRooms = activeRooms.get("topN").getAsInt();
        int actualRooms = activeRooms.getAsJsonArray("rooms").size();
        System.out.printf("    Top %d rooms (found %d):\n", topNRooms, actualRooms);
        count = 0;
        for (JsonElement elem : activeRooms.getAsJsonArray("rooms")) {
            JsonObject room = elem.getAsJsonObject();
            System.out.printf("      #%d: Room %s (%d messages, %d unique users)\n",
                ++count,
                room.get("roomId").getAsString(),
                room.get("messageCount").getAsInt(),
                room.get("uniqueUsers").getAsInt());
            if (count >= 5) break; // Show top 5 only
        }
        if (actualRooms > 5) {
            System.out.printf("      ... and %d more\n", actualRooms - 5);
        }

        // 4. User Participation Patterns
        JsonObject participation = analytics.getAsJsonObject("userParticipation");
        JsonObject stats = participation.getAsJsonObject("stats");
        System.out.println("\n[4] User participation patterns");
        System.out.printf("    Total Users: %d\n", stats.get("totalUsers").getAsInt());
        System.out.printf("    Max Rooms per User: %d\n", stats.get("maxRoomsPerUser").getAsInt());
        System.out.printf("    Avg Rooms per User: %.2f\n", stats.get("avgRoomsPerUser").getAsDouble());

        System.out.println("\n" + "=".repeat(70));
    }

    private static String formatTime(String isoTimestamp) {
        // Extract just the time portion for readability
        if (isoTimestamp.contains("T")) {
            String[] parts = isoTimestamp.split("T");
            String timePart = parts[1].substring(0, 8); // HH:MM:SS
            return parts[0] + " " + timePart;
        }
        return isoTimestamp;
    }

    /**
     * Fetch query results from server and save them locally as JSON files.
     * Converts ws:// URL to http:// and fetches from query endpoints.
     */
    private static void triggerQueryExport(String wsUrl, String testId) {
        try {
            // Convert ws://host:port to http://host:8081
            String httpBase = wsUrl.replace("ws://", "http://").replace(":8080", ":8081");

            System.out.println("\n" + "=".repeat(60));
            System.out.println("Fetching Query Results from Server");
            System.out.println("=".repeat(60));

            HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(30))  // Increased for slow connections
                .build();

            // Create results directory
            java.nio.file.Path resultsDir = java.nio.file.Paths.get("results");
            java.nio.file.Files.createDirectories(resultsDir);

            // Sanitize testId for Windows filename (replace : with -)
            String safeTestId = testId.replace(":", "-");

            // Randomize selections
            java.util.Random random = new java.util.Random();

            // Query 1: Room messages (random room 1-20)
            int sampleRoom = random.nextInt(20) + 1;
            System.out.println("\n[1/3] Fetching messages for room " + sampleRoom + "...");
            String q1Url = httpBase + "/query/room-messages?testId=" + testId + "&roomId=" + sampleRoom;
            String q1Json = fetchJson(client, q1Url);
            if (q1Json != null) {
                String q1File = String.format("results/q1_room-%d_%s.json", sampleRoom, safeTestId);
                java.nio.file.Files.writeString(java.nio.file.Paths.get(q1File), q1Json);
                System.out.println("✓ Saved: " + q1File);
            }

            // Query 2: User history (random user from active users)
            // First fetch analytics to get a random active user
            String analyticsUrl = httpBase + "/query/analytics?testId=" + testId;
            String analyticsJson = fetchJson(client, analyticsUrl);

            // Parse to get a random user
            String sampleUser = "user1"; // fallback
            if (analyticsJson != null) {
                try {
                    com.google.gson.JsonObject json = com.google.gson.JsonParser.parseString(analyticsJson).getAsJsonObject();
                    com.google.gson.JsonArray topUsers = json.getAsJsonArray("q5_topUsers");
                    if (topUsers != null && topUsers.size() > 0) {
                        int randomUserIndex = random.nextInt(topUsers.size());
                        sampleUser = topUsers.get(randomUserIndex).getAsJsonObject().get("userId").getAsString();
                    }
                } catch (Exception e) {
                    // Use fallback user1
                }
            }

            System.out.println("\n[2/3] Fetching history for " + sampleUser + "...");
            String q2Url = httpBase + "/query/user-history?testId=" + testId + "&userId=" + sampleUser;
            String q2Json = fetchJson(client, q2Url);
            if (q2Json != null) {
                String q2File = String.format("results/q2_%s_%s.json", sampleUser, safeTestId);
                java.nio.file.Files.writeString(java.nio.file.Paths.get(q2File), q2Json);
                System.out.println("✓ Saved: " + q2File);
            }

            // Query 3-8: Analytics (use already fetched analytics or fetch if not available)
            System.out.println("\n[3/3] Fetching analytics...");
            if (analyticsJson == null) {
                String q3Url = httpBase + "/query/analytics?testId=" + testId;
                analyticsJson = fetchJson(client, q3Url);
            }
            if (analyticsJson != null) {
                String q3File = String.format("results/q3-8_analytics_%s.json", safeTestId);
                java.nio.file.Files.writeString(java.nio.file.Paths.get(q3File), analyticsJson);
                System.out.println("✓ Saved: " + q3File);
            }

            System.out.println("\n" + "=".repeat(60));
            System.out.println("✓ Query Results Saved Successfully!");
            System.out.println("=".repeat(60));
            System.out.println("Location: ./results/");
            System.out.println("Files:");
            System.out.println("  - q1_room-" + sampleRoom + "_" + safeTestId + ".json");
            System.out.println("  - q2_" + sampleUser + "_" + safeTestId + ".json");
            System.out.println("  - q3-8_analytics_" + safeTestId + ".json");
            System.out.println("=".repeat(60));

        } catch (Exception e) {
            System.err.println("✗ Failed to fetch query results: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Fetch JSON from server endpoint
     *
     * Timeout set to 5 minutes to accommodate large query results
     * that require multiple DynamoDB pagination cycles.
     */
    private static String fetchJson(HttpClient client, String url) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofMinutes(5))  // Increased from 30s to 5 minutes for large datasets
                .GET()
                .build();

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() == 200) {
                return response.body();
            } else {
                System.err.println("✗ Query failed. Status: " + response.statusCode());
                System.err.println("Response: " + response.body());
                return null;
            }
        } catch (java.net.http.HttpTimeoutException e) {
            System.err.println("✗ Request timed out after 5 minutes: " + e.getMessage());
            System.err.println("  This may indicate a very large result set or slow query performance.");
            return null;
        } catch (Exception e) {
            System.err.println("✗ Request failed: " + e.getMessage());
            return null;
        }
    }

    /**
     * Endurance Test Mode: Sustained rate for 30 minutes
     * Tests system stability under prolonged load
     */
    private static void runEnduranceTest(String[] args) throws Exception {
        final String wsBase = args.length > 0 ? args[0] : DEFAULT_WS_BASE;
        final int targetRate = args.length > 2 ? Integer.parseInt(args[2]) : DEFAULT_ENDURANCE_RATE;
        final int durationMinutes = ENDURANCE_DURATION_MINUTES;

        // Calculate total messages: rate × seconds
        final int totalMessages = targetRate * 60 * durationMinutes;

        // Calculate threads and maxInFlight to achieve target rate
        // Optimal: 64-128 threads regardless of rate to avoid context switching overhead
        final int threads = Math.max(64, targetRate / 50); // Cap at 128 threads
        final int maxInFlight = Math.max(50000, targetRate * 20); // 20s buffer

        final String testId = Instant.now().toString();

        System.out.println("=".repeat(70));
        System.out.println("         ENDURANCE TEST MODE - 30 MINUTE SUSTAINED LOAD");
        System.out.println("=".repeat(70));
        System.out.printf("WebSocket: %s%n", wsBase);
        System.out.printf("Test ID: %s%n", testId);
        System.out.printf("Target Rate: %,d messages/second%n", targetRate);
        System.out.printf("Duration: %d minutes%n", durationMinutes);
        System.out.printf("Total Messages: %,d%n", totalMessages);
        System.out.printf("Threads: %d%n", threads);
        System.out.printf("Max In-Flight: %,d%n", maxInFlight);
        System.out.printf("Rooms: %d%n", ROOM_COUNT);
        System.out.println("=".repeat(70));
        System.out.println();

        // Generate messages
        System.out.println("Generating messages...");
        MessageGenerator generator = new MessageGenerator(testId);
        List<String> messages = generator.generateMessages(totalMessages);
        int expectedMessages = messages.size();
        System.out.printf("✓ Generated %,d messages%n%n", expectedMessages);

        ConcurrentMap<String, String> pendingAcks = new ConcurrentHashMap<>();
        for (String json : messages) {
            try {
                JsonObject obj = JsonParser.parseString(json).getAsJsonObject();
                if (obj.has("messageId")) {
                    pendingAcks.put(obj.get("messageId").getAsString(), json);
                }
            } catch (Exception ignore) {}
        }

        CountDownLatch allAcks = new CountDownLatch(expectedMessages);
        TestResults results = new TestResults();
        results.pendingAcks = pendingAcks;
        results.testId = testId;

        // Start progress logger
        ScheduledExecutorService progressLogger = startProgressLogger(results);

        System.out.println("Starting endurance test...");
        System.out.println("Sending messages with rate limiting: " + targetRate + " msg/s");
        System.out.println();

        // Run test with rate limiting
        runEnduranceTestWithRateLimit(wsBase, threads, messages, allAcks, pendingAcks, maxInFlight, results, targetRate);

        System.out.println();
        System.out.println("=".repeat(70));
        System.out.println("All messages sent! Waiting for remaining ACKs...");
        System.out.println("=".repeat(70));

        // Wait for remaining ACKs with 2 minute timeout
        boolean allAcked = allAcks.await(2, TimeUnit.MINUTES);
        Instant ackDone = Instant.now();

        progressLogger.shutdown();

        if (!allAcked) {
            System.out.printf("TIMEOUT after 2 minutes: %,d ACKs still pending%n", allAcks.getCount());
            System.out.println("Proceeding with report generation...");
        } else {
            System.out.println("All ACKs received!");
        }

        // Calculate final metrics
        long totalRuntimeMs = Duration.between(results.t0, ackDone).toMillis();
        long sendDurationMs = results.sendComplete != null ? Duration.between(results.t0, results.sendComplete).toMillis() : totalRuntimeMs;
        long ackWaitMs = results.sendComplete != null ? Duration.between(results.sendComplete, ackDone).toMillis() : 0;

        long ackCount = results.acksOk.get();
        long sendsOk = results.sendsOk.get();
        double avgThroughput = (sendsOk * 1000.0) / sendDurationMs;
        double ackThroughput = (ackCount * 1000.0) / totalRuntimeMs;

        // Generate endurance test report
        System.out.println();
        System.out.println("=".repeat(70));
        System.out.println("         ENDURANCE TEST REPORT");
        System.out.println("=".repeat(70));
        System.out.printf("Test ID: %s%n", testId);
        System.out.printf("Target Rate: %,d msg/s%n", targetRate);
        System.out.printf("Duration: %d minutes%n", durationMinutes);
        System.out.println();
        System.out.println("--- SEND PHASE ---");
        System.out.printf("Messages Sent: %,d / %,d (%.1f%%)%n", sendsOk, expectedMessages, (sendsOk * 100.0) / expectedMessages);
        System.out.printf("Send Duration: %s%n", formatDuration(sendDurationMs));
        System.out.printf("Avg Throughput: %.1f msg/s%n", avgThroughput);
        System.out.printf("Target Achievement: %.1f%%%n", (avgThroughput / targetRate) * 100.0);
        System.out.println();
        System.out.println("--- ACK PHASE ---");
        System.out.printf("ACKs Received: %,d / %,d (%.1f%%)%n", ackCount, expectedMessages, (ackCount * 100.0) / expectedMessages);
        System.out.printf("ACK Wait Time: %s%n", formatDuration(ackWaitMs));
        System.out.printf("ACK Throughput: %.1f msg/s%n", ackThroughput);
        System.out.println();
        System.out.println("--- TOTAL ---");
        System.out.printf("Total Runtime: %s%n", formatDuration(totalRuntimeMs));
        System.out.printf("Missing ACKs: %,d%n", pendingAcks.size());
        System.out.printf("Send Failures: %,d%n", results.sendsFailed.get());
        System.out.printf("Timeouts: %,d%n", results.timeouts.get());
        System.out.println("=".repeat(70));

        // Write detailed report to file
        writeEnduranceReport(testId, targetRate, durationMinutes, expectedMessages, sendsOk, ackCount,
                            avgThroughput, ackThroughput, sendDurationMs, ackWaitMs, totalRuntimeMs, results);

        // Wait for DynamoDB writes to complete
        System.out.println();
        System.out.println("=".repeat(70));
        System.out.println("Waiting for DynamoDB writes to complete...");
        System.out.println("(Allowing 30 seconds for batch writer to flush all queues)");
        System.out.println("=".repeat(70));
        Thread.sleep(30000); // Wait 30 seconds for batch writer to finish

        // Automatically trigger query export on server
        System.out.println();
        System.out.println("Triggering automatic query export on server...");
        triggerQueryExport(wsBase, testId);

        System.out.println();
        System.out.println("=".repeat(70));
        System.out.println("Endurance test completed!");
        System.out.println("=".repeat(70));

        System.exit(0);
    }

    /**
     * Run test with rate limiting to achieve target messages/second
     */
    private static void runEnduranceTestWithRateLimit(
            String wsBase, int threads, List<String> messages,
            CountDownLatch allAcks, ConcurrentMap<String, String> pendingAcks,
            int maxInFlight, TestResults results, int targetRate) throws Exception {

        results.t0 = Instant.now();

        // Use a blocking queue with rate-limited producer
        BlockingQueue<String> messageQueue = new ArrayBlockingQueue<>(QUEUE_CAPACITY);

        // Start sender threads (consumers)
        ExecutorService senderPool = Executors.newFixedThreadPool(threads);
        for (int i = 0; i < threads; i++) {
            senderPool.submit(new SenderWorker(
                wsBase,
                messageQueue,
                SenderWorker.Mode.RANDOM_ROOM_PER_MESSAGE,
                i + 1,
                results.sendsOk,
                results.sendsFailed,
                results.totalConnections,
                results.reconnections,
                results.acksOk,
                allAcks,
                pendingAcks,
                results.connectionFailures,
                results.retryAttempts,
                true
            ));
        }

        // Rate-limited producer: send messages at target rate using batching for better precision
        final int batchSize = Math.max(10, targetRate / 100); // Send in batches for smoother rate limiting
        final long batchIntervalMs = (batchSize * 1000L) / targetRate; // milliseconds per batch
        long nextBatchTime = System.currentTimeMillis();
        int messageIndex = 0;

        while (messageIndex < messages.size()) {
            // Calculate how many messages to send in this batch
            int remainingMessages = messages.size() - messageIndex;
            int messagesToSend = Math.min(batchSize, remainingMessages);

            // Send batch
            for (int i = 0; i < messagesToSend; i++) {
                try {
                    messageQueue.put(messages.get(messageIndex++));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }

            // Rate limiting: sleep until next batch time
            long now = System.currentTimeMillis();
            nextBatchTime += batchIntervalMs;
            if (now < nextBatchTime) {
                long sleepMs = nextBatchTime - now;
                if (sleepMs > 0) {
                    Thread.sleep(sleepMs);
                }
            }
        }

        results.sendComplete = Instant.now();

        // Stop workers after all messages are sent
        senderPool.shutdown();
        senderPool.awaitTermination(5, TimeUnit.MINUTES);
    }

    private static String formatDuration(long ms) {
        long seconds = ms / 1000;
        long minutes = seconds / 60;
        long hours = minutes / 60;

        if (hours > 0) {
            return String.format("%d:%02d:%02d", hours, minutes % 60, seconds % 60);
        } else if (minutes > 0) {
            return String.format("%d:%02d", minutes, seconds % 60);
        } else {
            return String.format("%.1fs", ms / 1000.0);
        }
    }

    private static void writeEnduranceReport(String testId, int targetRate, int durationMinutes,
                                            int expectedMessages, long sendsOk, long ackCount,
                                            double avgThroughput, double ackThroughput,
                                            long sendDurationMs, long ackWaitMs, long totalRuntimeMs,
                                            TestResults results) {
        try {
            String safeTestId = testId.replace(":", "-");
            String filename = String.format("results/endurance_%s.txt", safeTestId);

            StringBuilder report = new StringBuilder();
            report.append("=".repeat(70)).append("\n");
            report.append("ENDURANCE TEST REPORT\n");
            report.append("=".repeat(70)).append("\n\n");
            report.append(String.format("Test ID: %s\n", testId));
            report.append(String.format("Target Rate: %,d msg/s\n", targetRate));
            report.append(String.format("Duration: %d minutes\n\n", durationMinutes));

            report.append("SEND PHASE:\n");
            report.append(String.format("  Messages Sent: %,d / %,d (%.1f%%)\n", sendsOk, expectedMessages, (sendsOk * 100.0) / expectedMessages));
            report.append(String.format("  Send Duration: %s\n", formatDuration(sendDurationMs)));
            report.append(String.format("  Avg Throughput: %.1f msg/s\n", avgThroughput));
            report.append(String.format("  Target Achievement: %.1f%%\n\n", (avgThroughput / targetRate) * 100.0));

            report.append("ACK PHASE:\n");
            report.append(String.format("  ACKs Received: %,d / %,d (%.1f%%)\n", ackCount, expectedMessages, (ackCount * 100.0) / expectedMessages));
            report.append(String.format("  ACK Wait Time: %s\n", formatDuration(ackWaitMs)));
            report.append(String.format("  ACK Throughput: %.1f msg/s\n\n", ackThroughput));

            report.append("TOTAL:\n");
            report.append(String.format("  Total Runtime: %s\n", formatDuration(totalRuntimeMs)));
            report.append(String.format("  Missing ACKs: %,d\n", results.pendingAcks.size()));
            report.append(String.format("  Send Failures: %,d\n", results.sendsFailed.get()));
            report.append(String.format("  Timeouts: %,d\n", results.timeouts.get()));
            report.append("=".repeat(70)).append("\n");

            java.nio.file.Files.writeString(java.nio.file.Paths.get(filename), report.toString());
            System.out.println("\n✓ Endurance report saved: " + filename);
        } catch (Exception e) {
            System.err.println("Failed to write endurance report: " + e.getMessage());
        }
    }
}
