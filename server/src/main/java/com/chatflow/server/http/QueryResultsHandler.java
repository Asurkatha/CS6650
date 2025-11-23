package com.chatflow.server.http;

import com.chatflow.server.storage.DynamoDbQueryService;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * HTTP endpoint to return query results as JSON for client to download.
 *
 * Endpoints:
 * - GET /query/room-messages?testId=X&roomId=Y
 * - GET /query/user-history?testId=X&userId=Y
 * - GET /query/analytics?testId=X
 */
public class QueryResultsHandler implements HttpHandler {

    private final DynamoDbQueryService queryService;
    private final Gson gson;

    public QueryResultsHandler() {
        this.queryService = new DynamoDbQueryService();
        this.gson = new GsonBuilder().setPrettyPrinting().create();
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        if (!"GET".equals(exchange.getRequestMethod())) {
            sendResponse(exchange, 405, "{\"error\": \"Method not allowed\"}");
            return;
        }

        String path = exchange.getRequestURI().getPath();
        String query = exchange.getRequestURI().getQuery();
        Map<String, String> params = parseQueryParams(query);

        try {
            if (path.endsWith("/query/room-messages")) {
                handleRoomMessages(exchange, params);
            } else if (path.endsWith("/query/user-history")) {
                handleUserHistory(exchange, params);
            } else if (path.endsWith("/query/analytics")) {
                handleAnalytics(exchange, params);
            } else {
                sendResponse(exchange, 404, "{\"error\": \"Endpoint not found\"}");
            }
        } catch (Exception e) {
            System.err.println("[QueryResults] Error: " + e.getMessage());
            e.printStackTrace();
            String errorResponse = String.format(
                "{\"error\": \"%s\"}",
                e.getMessage().replace("\"", "'")
            );
            sendResponse(exchange, 500, errorResponse);
        }
    }

    private void handleRoomMessages(HttpExchange exchange, Map<String, String> params) throws IOException {
        String testId = params.get("testId");
        String roomIdStr = params.get("roomId");

        if (testId == null || roomIdStr == null) {
            sendResponse(exchange, 400, "{\"error\": \"Missing testId or roomId\"}");
            return;
        }

        String roomId = roomIdStr;
        System.out.printf("[QueryResults] Room messages: testId=%s, roomId=%s\n", testId, roomId);

        // Track query execution time
        long startTime = System.nanoTime();

        // No limit - return ALL messages for accurate count
        List<DynamoDbQueryService.ChatMessage> messages = queryService.getMessagesForRoom(roomId, testId, 0);

        long endTime = System.nanoTime();
        double executionTimeSeconds = (endTime - startTime) / 1_000_000_000.0;

        Map<String, Object> result = new HashMap<>();
        result.put("query", "room-messages");
        result.put("testId", testId);
        result.put("roomId", roomId);
        result.put("messageCount", messages.size());
        result.put("executionTimeSeconds", String.format("%.3f", executionTimeSeconds));
        result.put("messages", messages);

        System.out.printf("[QueryResults] Q1 completed in %.3f seconds (%d messages)\n",
            executionTimeSeconds, messages.size());

        String json = gson.toJson(result);
        sendJsonResponse(exchange, 200, json);
    }

    private void handleUserHistory(HttpExchange exchange, Map<String, String> params) throws IOException {
        String testId = params.get("testId");
        String userId = params.get("userId");

        if (testId == null || userId == null) {
            sendResponse(exchange, 400, "{\"error\": \"Missing testId or userId\"}");
            return;
        }

        System.out.printf("[QueryResults] User history: testId=%s, userId=%s\n", testId, userId);

        // Track query execution time
        long startTime = System.nanoTime();

        // No limit - return ALL messages for accurate count
        List<DynamoDbQueryService.ChatMessage> history = queryService.getUserMessageHistory(userId, testId, 0);

        long endTime = System.nanoTime();
        double executionTimeSeconds = (endTime - startTime) / 1_000_000_000.0;

        Map<String, Object> result = new HashMap<>();
        result.put("query", "user-history");
        result.put("testId", testId);
        result.put("userId", userId);
        result.put("messageCount", history.size());
        result.put("executionTimeSeconds", String.format("%.3f", executionTimeSeconds));
        result.put("messages", history);

        System.out.printf("[QueryResults] Q2 completed in %.3f seconds (%d messages)\n",
            executionTimeSeconds, history.size());

        String json = gson.toJson(result);
        sendJsonResponse(exchange, 200, json);
    }

    private void handleAnalytics(HttpExchange exchange, Map<String, String> params) throws IOException {
        String testId = params.get("testId");

        if (testId == null) {
            sendResponse(exchange, 400, "{\"error\": \"Missing testId\"}");
            return;
        }

        System.out.printf("[QueryResults] Analytics: testId=%s\n", testId);

        // Track overall analytics execution time
        long analyticsStartTime = System.nanoTime();

        Map<String, Object> analytics = new LinkedHashMap<>();
        analytics.put("query", "analytics");
        analytics.put("testId", testId);

        Map<String, String> queryTimes = new LinkedHashMap<>();

        // Query 3: Count active users
        long q3Start = System.nanoTime();
        int activeUsers = queryService.countActiveUsers(testId);
        queryTimes.put("q3_activeUsersTime", String.format("%.3f", (System.nanoTime() - q3Start) / 1_000_000_000.0));
        analytics.put("q3_activeUsers", activeUsers);

        // Query 4: Get rooms user participated in (pick a random user from active users)
        long q4Start = System.nanoTime();
        List<DynamoDbQueryService.UserStats> topUsers = queryService.getMostActiveUsers(testId, 100);
        String sampleUserId = topUsers.isEmpty() ? "user1" : topUsers.get(new Random().nextInt(topUsers.size())).userId();

        List<DynamoDbQueryService.UserRoomInfo> userRooms = queryService.getUserRooms(sampleUserId, testId);

        // Get all messages for this user ONCE (no limit)
        List<DynamoDbQueryService.ChatMessage> allUserMessages = queryService.getUserMessageHistory(sampleUserId, testId, 0);

        // Format Query 4 output: just room numbers and last message per room
        List<Map<String, Object>> q4_formatted = new ArrayList<>();
        for (DynamoDbQueryService.UserRoomInfo roomInfo : userRooms) {
            // Find the last message for this specific room
            String lastMessage = null;
            for (DynamoDbQueryService.ChatMessage msg : allUserMessages) {
                if (msg.roomId().equals(roomInfo.roomId())) {
                    lastMessage = msg.message();
                    break; // Messages are sorted by timestamp descending
                }
            }

            Map<String, Object> roomData = new LinkedHashMap<>();
            roomData.put("roomId", roomInfo.roomId());
            roomData.put("lastMessage", lastMessage != null ? lastMessage : "N/A");
            q4_formatted.add(roomData);
        }
        queryTimes.put("q4_userRoomsTime", String.format("%.3f", (System.nanoTime() - q4Start) / 1_000_000_000.0));

        analytics.put("q4_userId", sampleUserId);
        analytics.put("q4_roomsParticipated", q4_formatted);

        // Query 5: Most active users (top 10)
        long q5Start = System.nanoTime();
        List<DynamoDbQueryService.UserStats> top10Users = queryService.getMostActiveUsers(testId, 10);
        queryTimes.put("q5_topUsersTime", String.format("%.3f", (System.nanoTime() - q5Start) / 1_000_000_000.0));
        analytics.put("q5_topUsers", top10Users);

        // Query 6: Most active rooms (top 10)
        long q6Start = System.nanoTime();
        List<DynamoDbQueryService.RoomStats> topRooms = queryService.getMostActiveRooms(testId, 10);
        queryTimes.put("q6_topRoomsTime", String.format("%.3f", (System.nanoTime() - q6Start) / 1_000_000_000.0));
        analytics.put("q6_topRooms", topRooms);

        // Query 7: Messages per hour (hourly breakdown)
        long q7Start = System.nanoTime();
        Map<String, Integer> hourlyStats = queryService.getMessagesPerHour(testId);
        queryTimes.put("q7_messagesPerHourTime", String.format("%.3f", (System.nanoTime() - q7Start) / 1_000_000_000.0));
        analytics.put("q7_messagesPerHour", hourlyStats);

        // Query 8: Total messages per day (daily aggregation)
        long q8Start = System.nanoTime();
        Map<String, Integer> dailyStats = aggregateByDay(hourlyStats);
        queryTimes.put("q8_messagesPerDayTime", String.format("%.3f", (System.nanoTime() - q8Start) / 1_000_000_000.0));
        analytics.put("q8_messagesPerDay", dailyStats);

        // Add individual query times and total execution time
        double totalExecutionTime = (System.nanoTime() - analyticsStartTime) / 1_000_000_000.0;
        analytics.put("queryExecutionTimes", queryTimes);
        analytics.put("totalExecutionTimeSeconds", String.format("%.3f", totalExecutionTime));

        System.out.printf("[QueryResults] Q3-8 Analytics completed in %.3f seconds\n", totalExecutionTime);
        for (Map.Entry<String, String> entry : queryTimes.entrySet()) {
            System.out.printf("  - %s: %s seconds\n", entry.getKey(), entry.getValue());
        }

        String json = gson.toJson(analytics);
        sendJsonResponse(exchange, 200, json);
    }

    /**
     * Aggregate hourly stats into daily totals for Query 8
     */
    private Map<String, Integer> aggregateByDay(Map<String, Integer> hourlyStats) {
        Map<String, Integer> dailyStats = new LinkedHashMap<>();

        for (Map.Entry<String, Integer> entry : hourlyStats.entrySet()) {
            String hourBucket = entry.getKey();
            int count = entry.getValue();

            // Extract date from hour bucket (format: "2025-11-21T12")
            String day = hourBucket.substring(0, 10); // "2025-11-21"

            dailyStats.merge(day, count, Integer::sum);
        }

        return dailyStats;
    }

    private Map<String, String> parseQueryParams(String query) {
        Map<String, String> params = new HashMap<>();
        if (query == null || query.isEmpty()) {
            return params;
        }

        for (String param : query.split("&")) {
            String[] parts = param.split("=", 2);
            if (parts.length == 2) {
                params.put(parts[0], parts[1]);
            }
        }
        return params;
    }

    private void sendJsonResponse(HttpExchange exchange, int statusCode, String json) throws IOException {
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(statusCode, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }

    private void sendResponse(HttpExchange exchange, int statusCode, String response) throws IOException {
        sendJsonResponse(exchange, statusCode, response);
    }
}
