package com.chatflow.server.http;

import com.chatflow.server.consumer.MessageConsumer;
import com.chatflow.server.storage.DynamoDbQueryService;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.io.OutputStream;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;

/**
 * HTTP endpoint to run all Assignment 3 queries and return JSON results.
 * Endpoint: GET /queries
 */
public class QueryHandler implements HttpHandler {

    private final DynamoDbQueryService queryService;
    private final Gson gson;

    public QueryHandler() {
        this.queryService = new DynamoDbQueryService();
        this.gson = new GsonBuilder().setPrettyPrinting().create();
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        if (!"GET".equals(exchange.getRequestMethod())) {
            sendResponse(exchange, 405, "{\"error\": \"Method not allowed\"}");
            return;
        }

        try {
            // Extract testId from query parameter
            String query = exchange.getRequestURI().getQuery();
            String testId = null;

            if (query != null) {
                Map<String, String> params = parseQueryParams(query);
                testId = params.get("testId");
            }

            if (testId == null || testId.isEmpty()) {
                sendResponse(exchange, 400, "{\"error\": \"Missing required parameter: testId\"}");
                return;
            }

            System.out.println("\n[Query Endpoint] Running all Assignment 3 queries for testId: " + testId + "\n");

            QueryResults results = executeAllQueries(testId);

            String jsonResponse = gson.toJson(results);
            sendResponse(exchange, 200, jsonResponse);

            System.out.println("\n[Query Endpoint] All queries completed successfully\n");

        } catch (Exception e) {
            e.printStackTrace();
            String errorJson = gson.toJson(Map.of(
                "error", e.getMessage(),
                "type", e.getClass().getSimpleName()
            ));
            sendResponse(exchange, 500, errorJson);
        }
    }

    private QueryResults executeAllQueries(String testId) {
        QueryResults results = new QueryResults();
        results.testId = testId;

        // Get random parameters
        Random rand = new Random();
        Set<String> userIds = MessageConsumer.getTrackedUserIds();
        String randomUserId = pickRandomUserId(userIds, rand);
        int randomRoomId = rand.nextInt(20) + 1;

        Map<String, String> executionTimes = new LinkedHashMap<>();

        // Core Queries
        System.out.println("=".repeat(70));
        System.out.println("CORE QUERIES");
        System.out.println("=".repeat(70));

        // 1. Get messages for a room
        System.out.println("\n[1/8] Room Messages Query (roomId=" + randomRoomId + ", testId=" + testId + ")");
        long q1Start = System.nanoTime();
        results.coreQueries.roomMessages = new RoomMessagesResult(
            String.valueOf(randomRoomId),
            testId,
            queryService.getMessagesForRoom(String.valueOf(randomRoomId), testId, 10000)
        );
        double q1Time = (System.nanoTime() - q1Start) / 1_000_000_000.0;
        executionTimes.put("q1_roomMessages", String.format("%.3f", q1Time));
        System.out.printf("  ✓ Completed in %.3f seconds\n", q1Time);

        // 2. Get user message history
        System.out.println("\n[2/8] User History Query (userId=" + randomUserId + ", testId=" + testId + ")");
        long q2Start = System.nanoTime();
        results.coreQueries.userHistory = new UserHistoryResult(
            randomUserId,
            testId,
            queryService.getUserMessageHistory(randomUserId, testId, 10000)
        );
        double q2Time = (System.nanoTime() - q2Start) / 1_000_000_000.0;
        executionTimes.put("q2_userHistory", String.format("%.3f", q2Time));
        System.out.printf("  ✓ Completed in %.3f seconds\n", q2Time);

        // 3. Count active users
        System.out.println("\n[3/8] Active Users Count Query (testId=" + testId + ")");
        long q3Start = System.nanoTime();
        results.coreQueries.activeUsersCount = new ActiveUsersResult(
            testId,
            queryService.countActiveUsers(testId)
        );
        double q3Time = (System.nanoTime() - q3Start) / 1_000_000_000.0;
        executionTimes.put("q3_activeUsersCount", String.format("%.3f", q3Time));
        System.out.printf("  ✓ Completed in %.3f seconds\n", q3Time);

        // 4. Get user rooms
        System.out.println("\n[4/8] User Rooms Query (userId=" + randomUserId + ", testId=" + testId + ")");
        long q4Start = System.nanoTime();
        results.coreQueries.userRooms = new UserRoomsResult(
            randomUserId,
            testId,
            queryService.getUserRooms(randomUserId, testId)
        );
        double q4Time = (System.nanoTime() - q4Start) / 1_000_000_000.0;
        executionTimes.put("q4_userRooms", String.format("%.3f", q4Time));
        System.out.printf("  ✓ Completed in %.3f seconds\n", q4Time);

        // Analytics Queries
        System.out.println("\n" + "=".repeat(70));
        System.out.println("ANALYTICS QUERIES");
        System.out.println("=".repeat(70));

        // 5. Messages per hour
        System.out.println("\n[5/8] Messages Per Hour Query (testId=" + testId + ")");
        long q5Start = System.nanoTime();
        results.analyticsQueries.messagesPerHour = new MessagesPerHourResult(
            testId,
            queryService.getMessagesPerHour(testId)
        );
        double q5Time = (System.nanoTime() - q5Start) / 1_000_000_000.0;
        executionTimes.put("q5_messagesPerHour", String.format("%.3f", q5Time));
        System.out.printf("  ✓ Completed in %.3f seconds\n", q5Time);

        // 6. Most active users (top 10)
        System.out.println("\n[6/8] Most Active Users Query (top 10, testId=" + testId + ")");
        long q6Start = System.nanoTime();
        results.analyticsQueries.mostActiveUsers = new MostActiveUsersResult(
            testId,
            10,
            queryService.getMostActiveUsers(testId, 10)
        );
        double q6Time = (System.nanoTime() - q6Start) / 1_000_000_000.0;
        executionTimes.put("q6_mostActiveUsers", String.format("%.3f", q6Time));
        System.out.printf("  ✓ Completed in %.3f seconds\n", q6Time);

        // 7. Most active rooms (top 10)
        System.out.println("\n[7/8] Most Active Rooms Query (top 10, testId=" + testId + ")");
        long q7Start = System.nanoTime();
        results.analyticsQueries.mostActiveRooms = new MostActiveRoomsResult(
            testId,
            10,
            queryService.getMostActiveRooms(testId, 10)
        );
        double q7Time = (System.nanoTime() - q7Start) / 1_000_000_000.0;
        executionTimes.put("q7_mostActiveRooms", String.format("%.3f", q7Time));
        System.out.printf("  ✓ Completed in %.3f seconds\n", q7Time);

        // 8. User participation patterns
        System.out.println("\n[8/8] User Participation Patterns Query (testId=" + testId + ")");
        long q8Start = System.nanoTime();
        Map<String, Integer> participation = queryService.getUserParticipationPatterns(testId);
        results.analyticsQueries.userParticipation = new UserParticipationResult(
            testId,
            participation,
            calculateParticipationStats(participation)
        );
        double q8Time = (System.nanoTime() - q8Start) / 1_000_000_000.0;
        executionTimes.put("q8_userParticipation", String.format("%.3f", q8Time));
        System.out.printf("  ✓ Completed in %.3f seconds\n", q8Time);

        // Add execution times to results
        results.executionTimes = executionTimes;

        // Calculate total time
        double totalTime = executionTimes.values().stream()
            .mapToDouble(s -> Double.parseDouble(s))
            .sum();
        results.totalExecutionTimeSeconds = String.format("%.3f", totalTime);

        System.out.println("\n" + "=".repeat(70));
        System.out.printf("Total execution time: %.3f seconds\n", totalTime);
        System.out.println("=".repeat(70));

        return results;
    }

    private Map<String, String> parseQueryParams(String query) {
        Map<String, String> params = new HashMap<>();
        if (query == null || query.isEmpty()) {
            return params;
        }

        for (String param : query.split("&")) {
            String[] pair = param.split("=");
            if (pair.length == 2) {
                params.put(pair[0], pair[1]);
            }
        }
        return params;
    }

    private String pickRandomUserId(Set<String> userIds, Random rand) {
        if (userIds.isEmpty()) {
            return "user" + (rand.nextInt(100) + 1);
        }
        List<String> userList = new ArrayList<>(userIds);
        return userList.get(rand.nextInt(userList.size()));
    }

    private ParticipationStats calculateParticipationStats(Map<String, Integer> participation) {
        if (participation.isEmpty()) {
            return new ParticipationStats(0, 0, 0.0);
        }

        int totalUsers = participation.size();
        int maxRooms = participation.values().stream().max(Integer::compareTo).orElse(0);
        double avgRooms = participation.values().stream()
            .mapToInt(Integer::intValue)
            .average()
            .orElse(0.0);

        return new ParticipationStats(totalUsers, maxRooms, avgRooms);
    }

    private void sendResponse(HttpExchange exchange, int statusCode, String response) throws IOException {
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");

        byte[] bytes = response.getBytes();
        exchange.sendResponseHeaders(statusCode, bytes.length);

        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }

    // Result classes
    private static class QueryResults {
        String testId;
        CoreQueries coreQueries = new CoreQueries();
        AnalyticsQueries analyticsQueries = new AnalyticsQueries();
        Map<String, String> executionTimes;
        String totalExecutionTimeSeconds;
    }

    private static class CoreQueries {
        RoomMessagesResult roomMessages;
        UserHistoryResult userHistory;
        ActiveUsersResult activeUsersCount;
        UserRoomsResult userRooms;
    }

    private static class AnalyticsQueries {
        MessagesPerHourResult messagesPerHour;
        MostActiveUsersResult mostActiveUsers;
        MostActiveRoomsResult mostActiveRooms;
        UserParticipationResult userParticipation;
    }

    private record RoomMessagesResult(
        String roomId,
        String testId,
        List<DynamoDbQueryService.ChatMessage> messages
    ) {}

    private record UserHistoryResult(
        String userId,
        String testId,
        List<DynamoDbQueryService.ChatMessage> messages
    ) {}

    private record ActiveUsersResult(
        String testId,
        int count
    ) {}

    private record UserRoomsResult(
        String userId,
        String testId,
        List<DynamoDbQueryService.UserRoomInfo> rooms
    ) {}

    private record MessagesPerHourResult(
        String testId,
        Map<String, Integer> hourlyStats
    ) {}

    private record MostActiveUsersResult(
        String testId,
        int topN,
        List<DynamoDbQueryService.UserStats> users
    ) {}

    private record MostActiveRoomsResult(
        String testId,
        int topN,
        List<DynamoDbQueryService.RoomStats> rooms
    ) {}

    private record UserParticipationResult(
        String testId,
        Map<String, Integer> participation,
        ParticipationStats stats
    ) {}

    private record ParticipationStats(
        int totalUsers,
        int maxRoomsPerUser,
        double avgRoomsPerUser
    ) {}
}
