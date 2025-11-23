package com.chatflow.server.storage;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Query service for DynamoDB to support Assignment 3 requirements.
 *
 * Core Queries:
 * 1. Get messages for a room in time range
 * 2. Get user's message history
 * 3. Count active users in time window
 * 4. Get rooms user has participated in
 *
 * Analytics Queries:
 * 1. Messages per hour/day statistics
 * 2. Most active users
 * 3. Most active rooms
 */
public class DynamoDbQueryService {

    private final DynamoDbClient ddb;
    private final String messagesTable;
    private final String userRoomsTable;
    private final String activityBucketsTable;

    public DynamoDbQueryService() {
        this.ddb = DynamoDbClientProvider.getClient();
        this.messagesTable = System.getProperty("DDB_MESSAGES_TABLE", "Messages");
        this.userRoomsTable = System.getProperty("DDB_USERROOMS_TABLE", "UserRoomStats");
        this.activityBucketsTable = System.getProperty("DDB_ACTIVITY_BUCKETS_TABLE", "HourlyMessageStats");
    }

    // ============================================================================
    // Core Query 1: Get messages for a room in time range
    // Performance target: < 100ms for 1000 messages
    // ============================================================================

    /**
     * Query ALL messages for ANY room in a test (PRODUCTION SCHEMA).
     * Uses testId_roomId composite partition key - direct query, no GSI needed!
     *
     * @param roomId Room to query (1-20, user provides ANY roomId)
     * @param testId Test run identifier
     * @param limit 0 for uncapped, > 0 for limit
     * @return All messages for the room in this test
     */
    public List<ChatMessage> getMessagesForRoom(String roomId, String testId, int limit) {
        long queryStart = System.currentTimeMillis();
        List<ChatMessage> messages = new ArrayList<>();

        try {
            // PRODUCTION SCHEMA: testId#roomId as partition key (direct query!)
            String compositeKey = testId + "#" + roomId;

            Map<String, AttributeValue> expressionValues = new HashMap<>();
            expressionValues.put(":key", AttributeValue.builder().s(compositeKey).build());

            Map<String, AttributeValue> lastEvaluatedKey = null;
            int pageCount = 0;

            do {
                QueryRequest.Builder requestBuilder = QueryRequest.builder()
                    .tableName(messagesTable)
                    .keyConditionExpression("testId_roomId = :key")
                    .expressionAttributeValues(expressionValues)
                    .scanIndexForward(false); // Descending order - most recent first

                // Handle pagination
                if (lastEvaluatedKey != null) {
                    requestBuilder.exclusiveStartKey(lastEvaluatedKey);
                }

                // Only apply limit if explicitly provided and > 0
                if (limit > 0) {
                    requestBuilder.limit(limit);
                }

                QueryRequest request = requestBuilder.build();
                QueryResponse response = ddb.query(request);
                pageCount++;

                for (Map<String, AttributeValue> item : response.items()) {
                    messages.add(parseChatMessage(item));
                }

                lastEvaluatedKey = response.lastEvaluatedKey();

                // If limit is set and we've reached it, stop pagination
                if (limit > 0 && messages.size() >= limit) {
                    break;
                }

            } while (lastEvaluatedKey != null && !lastEvaluatedKey.isEmpty());

            long duration = System.currentTimeMillis() - queryStart;
            System.out.printf("[Query 1] Room %s (testId=%s): %d messages, %d pages, %dms\n",
                roomId, testId, messages.size(), pageCount, duration);

            return messages;

        } catch (DynamoDbException e) {
            System.err.println("[Query 1] Failed: " + e.getMessage());
            return messages;
        }
    }

    // ============================================================================
    // Core Query 2: Get user's message history
    // Performance target: < 200ms
    // ============================================================================

    /**
     * Query ALL messages for ANY user in a test (production-ready).
     * Uses composite key GSI for optimal performance (no filter expressions).
     *
     * @param userId User to query (user provides ANY userId)
     * @param testId Test run identifier
     * @param limit 0 for uncapped, > 0 for limit
     * @return All messages by this user in this test
     */
    public List<ChatMessage> getUserMessageHistory(String userId, String testId, int limit) {
        long queryStart = System.currentTimeMillis();
        List<ChatMessage> messages = new ArrayList<>();

        try {
            // Build composite key: "testId#userId"
            String compositeKey = testId + "#" + userId;

            Map<String, AttributeValue> expressionValues = new HashMap<>();
            expressionValues.put(":key", AttributeValue.builder().s(compositeKey).build());

            Map<String, AttributeValue> lastEvaluatedKey = null;
            int pageCount = 0;

            do {
                QueryRequest.Builder requestBuilder = QueryRequest.builder()
                    .tableName(messagesTable)
                    .indexName("GSI_TestUser")  // Use composite key GSI
                    .keyConditionExpression("testId_userId = :key")
                    .expressionAttributeValues(expressionValues)
                    .scanIndexForward(false); // Descending order (most recent first)

                // Handle pagination
                if (lastEvaluatedKey != null) {
                    requestBuilder.exclusiveStartKey(lastEvaluatedKey);
                }

                // Only apply limit if explicitly provided and > 0
                if (limit > 0) {
                    requestBuilder.limit(limit);
                }

                QueryRequest request = requestBuilder.build();
                QueryResponse response = ddb.query(request);
                pageCount++;

                for (Map<String, AttributeValue> item : response.items()) {
                    messages.add(parseChatMessage(item));
                }

                lastEvaluatedKey = response.lastEvaluatedKey();

                // If limit is set and we've reached it, stop pagination
                if (limit > 0 && messages.size() >= limit) {
                    break;
                }

            } while (lastEvaluatedKey != null && !lastEvaluatedKey.isEmpty());

            long duration = System.currentTimeMillis() - queryStart;
            System.out.printf("[Query 2] User %s (testId=%s): %d messages, %d pages, %dms\n",
                userId, testId, messages.size(), pageCount, duration);

            return messages;

        } catch (DynamoDbException e) {
            System.err.println("[Query 2] Failed: " + e.getMessage());
            return messages;
        }
    }

    // ============================================================================
    // Core Query 3: Count active users in time window
    // Performance target: < 500ms
    // ============================================================================

    /**
     * Count unique users active in a testId.
     * Queries Messages table directly using GSI_TestUser for accuracy.
     */
    public int countActiveUsers(String testId) {
        long queryStart = System.currentTimeMillis();
        Set<String> uniqueUsers = new HashSet<>();

        try {
            // Query Messages table to count distinct users (source of truth)
            for (int roomId = 1; roomId <= 20; roomId++) {
                String testId_roomId = testId + "#" + roomId;

                Map<String, AttributeValue> exprVals = new HashMap<>();
                exprVals.put(":key", AttributeValue.builder().s(testId_roomId).build());

                Map<String, AttributeValue> lastEvaluatedKey = null;
                int pageCount = 0;

                do {
                    QueryRequest.Builder queryBuilder = QueryRequest.builder()
                        .tableName(messagesTable)
                        .keyConditionExpression("testId_roomId = :key")
                        .expressionAttributeValues(exprVals)
                        .projectionExpression("userId");

                    if (lastEvaluatedKey != null) {
                        queryBuilder.exclusiveStartKey(lastEvaluatedKey);
                    }

                    QueryResponse queryResponse = ddb.query(queryBuilder.build());
                    pageCount++;

                    for (Map<String, AttributeValue> item : queryResponse.items()) {
                        if (item.containsKey("userId")) {
                            uniqueUsers.add(item.get("userId").s());
                        }
                    }

                    lastEvaluatedKey = queryResponse.lastEvaluatedKey();
                } while (lastEvaluatedKey != null && !lastEvaluatedKey.isEmpty());

                if (pageCount > 1) {
                    System.out.printf("[DDB] Room %d required %d pages\n", roomId, pageCount);
                }
            }

            long duration = System.currentTimeMillis() - queryStart;
            System.out.printf("[DDB Query] Active users: count=%d, testId=%s, time=%dms\n",
                uniqueUsers.size(), testId, duration);

            return uniqueUsers.size();

        } catch (Exception e) {
            System.err.println("[DDB] Active users count failed: " + e.getMessage());
            e.printStackTrace();
            return 0;
        }
    }

    // ============================================================================
    // Core Query 4: Get rooms user has participated in
    // Performance target: < 50ms
    // ============================================================================

    /**
     * Get all rooms a user has participated in (PRODUCTION SCHEMA).
     * Uses UserRoomStats with testId_userId composite key for direct query.
     * Performance: < 50ms
     */
    public List<UserRoomInfo> getUserRooms(String userId, String testId) {
        long queryStart = System.currentTimeMillis();
        List<UserRoomInfo> rooms = new ArrayList<>();

        try {
            // PRODUCTION SCHEMA: Query UserRoomStats by testId_userId composite key
            String compositeKey = testId + "#" + userId;

            Map<String, AttributeValue> key = new HashMap<>();
            key.put(":compositeKey", AttributeValue.builder().s(compositeKey).build());

            QueryRequest request = QueryRequest.builder()
                .tableName(userRoomsTable)
                .keyConditionExpression("testId_userId = :compositeKey")
                .expressionAttributeValues(key)
                .build();

            QueryResponse response = ddb.query(request);

            for (Map<String, AttributeValue> item : response.items()) {
                String roomId = item.get("roomId").s();
                long lastActivity = Long.parseLong(item.get("lastActivityTimestamp").s());
                int messageCount = item.containsKey("messageCount") ?
                    Integer.parseInt(item.get("messageCount").n()) : 0;
                String lastMessageId = item.containsKey("lastMessageId") ?
                    item.get("lastMessageId").s() : null;

                rooms.add(new UserRoomInfo(roomId, lastActivity, messageCount, lastMessageId));
            }

            // Fallback: If UserRoomStats is empty, query Messages table via GSI_TestUser
            if (rooms.isEmpty()) {
                System.out.println("[DDB Query] UserRoomStats empty for " + userId + ", falling back to Messages GSI");

                Map<String, AttributeValue> gsiKey = new HashMap<>();
                gsiKey.put(":compositeKey", AttributeValue.builder().s(compositeKey).build());

                QueryRequest gsiRequest = QueryRequest.builder()
                    .tableName(messagesTable)
                    .indexName("GSI_TestUser")
                    .keyConditionExpression("testId_userId = :compositeKey")
                    .expressionAttributeValues(gsiKey)
                    .projectionExpression("roomId, msgTimestamp")
                    .build();

                QueryResponse gsiResponse = ddb.query(gsiRequest);

                // Group by roomId and count messages
                Map<String, Integer> roomCounts = new HashMap<>();
                Map<String, Long> roomLastActivity = new HashMap<>();

                for (Map<String, AttributeValue> item : gsiResponse.items()) {
                    String roomId = item.get("roomId").s();
                    String timestamp = item.get("msgTimestamp").s();

                    // Parse timestamp: format is "ISO8601#messageId", extract ISO part
                    long ts = 0;
                    try {
                        String isoTimestamp = timestamp.split("#")[0];
                        ts = Instant.parse(isoTimestamp).toEpochMilli();
                    } catch (Exception e) {
                        // Skip if timestamp parsing fails
                    }

                    roomCounts.put(roomId, roomCounts.getOrDefault(roomId, 0) + 1);
                    roomLastActivity.put(roomId, Math.max(roomLastActivity.getOrDefault(roomId, 0L), ts));
                }

                for (Map.Entry<String, Integer> entry : roomCounts.entrySet()) {
                    rooms.add(new UserRoomInfo(
                        entry.getKey(),
                        roomLastActivity.get(entry.getKey()),
                        entry.getValue(),
                        null
                    ));
                }
            }

            // Sort by last activity (most recent first)
            rooms.sort((a, b) -> Long.compare(b.lastActivityTimestamp(), a.lastActivityTimestamp()));

            long duration = System.currentTimeMillis() - queryStart;
            System.out.printf("[DDB Query] User rooms: %s, count=%d, time=%dms\n",
                userId, rooms.size(), duration);

            return rooms;

        } catch (DynamoDbException e) {
            System.err.println("[DDB] User rooms query failed: " + e.getMessage());
            e.printStackTrace();
            return rooms;
        }
    }

    // ============================================================================
    // Analytics Query 1: Messages per hour/day statistics
    // ============================================================================

    /**
     * Get message count statistics grouped by hour, filtered by testId.
     * Scans UserActivityBuckets table with testId filter.
     */
    public Map<String, Integer> getMessagesPerHour(String testId) {
        long queryStart = System.currentTimeMillis();
        Map<String, Integer> hourlyStats = new HashMap<>();

        try {
            // Query HourlyMessageStats by testId (partition key)
            Map<String, AttributeValue> exprVals = new HashMap<>();
            exprVals.put(":testId", AttributeValue.builder().s(testId).build());

            QueryRequest request = QueryRequest.builder()
                .tableName(activityBucketsTable)  // Actually HourlyMessageStats
                .keyConditionExpression("testId = :testId")
                .expressionAttributeValues(exprVals)
                .build();

            QueryResponse response = ddb.query(request);

            // Extract hourBucket and messageCount from each item
            for (Map<String, AttributeValue> item : response.items()) {
                if (item.containsKey("hourBucket") && item.containsKey("messageCount")) {
                    String bucket = item.get("hourBucket").s();
                    int count = Integer.parseInt(item.get("messageCount").n());
                    hourlyStats.put(bucket, count);
                }
            }

            long duration = System.currentTimeMillis() - queryStart;
            System.out.printf("[DDB Query] Messages per hour: testId=%s, buckets=%d, time=%dms\n",
                testId, hourlyStats.size(), duration);

            return hourlyStats;

        } catch (Exception e) {
            System.err.println("[DDB] Messages per hour query failed: " + e.getMessage());
            return hourlyStats;
        }
    }

    // ============================================================================
    // Analytics Query 2: Most active users (top N)
    // ============================================================================

    /**
     * Get most active users by querying Messages table directly.
     * Source of truth for accurate counts.
     */
    public List<UserStats> getMostActiveUsers(String testId, int topN) {
        long queryStart = System.currentTimeMillis();
        Map<String, UserStatsBuilder> userStats = new HashMap<>();

        try {
            // Query all 20 rooms from Messages table
            for (int roomId = 1; roomId <= 20; roomId++) {
                String testId_roomId = testId + "#" + roomId;

                Map<String, AttributeValue> exprVals = new HashMap<>();
                exprVals.put(":key", AttributeValue.builder().s(testId_roomId).build());

                Map<String, AttributeValue> lastEvaluatedKey = null;
                int pageCount = 0;

                do {
                    QueryRequest.Builder queryBuilder = QueryRequest.builder()
                        .tableName(messagesTable)
                        .keyConditionExpression("testId_roomId = :key")
                        .expressionAttributeValues(exprVals)
                        .projectionExpression("userId, msgTimestamp");

                    if (lastEvaluatedKey != null) {
                        queryBuilder.exclusiveStartKey(lastEvaluatedKey);
                    }

                    QueryResponse queryResponse = ddb.query(queryBuilder.build());
                    pageCount++;

                    for (Map<String, AttributeValue> item : queryResponse.items()) {
                        String userId = item.get("userId").s();
                        String timestamp = item.get("msgTimestamp").s();

                        // Extract timestamp
                        long timestampMs = 0;
                        try {
                            String isoTimestamp = timestamp.split("#")[0];
                            timestampMs = Instant.parse(isoTimestamp).toEpochMilli();
                        } catch (Exception e) {
                            // Skip if parsing fails
                        }

                        UserStatsBuilder builder = userStats.getOrDefault(userId, new UserStatsBuilder(userId));
                        builder.addMessages(1);
                        builder.updateActivity(timestampMs);
                        userStats.put(userId, builder);
                    }

                    lastEvaluatedKey = queryResponse.lastEvaluatedKey();
                } while (lastEvaluatedKey != null && !lastEvaluatedKey.isEmpty());

                if (pageCount > 1) {
                    System.out.printf("[DDB] Room %d required %d pages for user stats\n", roomId, pageCount);
                }
            }

            List<UserStats> result = userStats.values().stream()
                .map(UserStatsBuilder::build)
                .sorted((a, b) -> Integer.compare(b.messageCount(), a.messageCount()))
                .limit(topN)
                .collect(Collectors.toList());

            long duration = System.currentTimeMillis() - queryStart;
            System.out.printf("[DDB Query] Most active users: top=%d, time=%dms\n", topN, duration);

            return result;

        } catch (DynamoDbException e) {
            System.err.println("[DDB] Most active users query failed: " + e.getMessage());
            return new ArrayList<>();
        }
    }

    // ============================================================================
    // Analytics Query 3: Most active rooms (top N)
    // ============================================================================

    /**
     * Get most active rooms by querying Messages table directly.
     * Source of truth for accurate counts.
     */
    public List<RoomStats> getMostActiveRooms(String testId, int topN) {
        long queryStart = System.currentTimeMillis();
        Map<String, RoomStatsBuilder> roomStats = new HashMap<>();

        try {
            // Query all 20 rooms from Messages table
            for (int roomId = 1; roomId <= 20; roomId++) {
                String testId_roomId = testId + "#" + roomId;

                Map<String, AttributeValue> exprVals = new HashMap<>();
                exprVals.put(":key", AttributeValue.builder().s(testId_roomId).build());

                RoomStatsBuilder builder = roomStats.getOrDefault(String.valueOf(roomId), new RoomStatsBuilder(String.valueOf(roomId)));

                Map<String, AttributeValue> lastEvaluatedKey = null;
                int pageCount = 0;

                do {
                    QueryRequest.Builder queryBuilder = QueryRequest.builder()
                        .tableName(messagesTable)
                        .keyConditionExpression("testId_roomId = :key")
                        .expressionAttributeValues(exprVals)
                        .projectionExpression("userId");

                    if (lastEvaluatedKey != null) {
                        queryBuilder.exclusiveStartKey(lastEvaluatedKey);
                    }

                    QueryResponse queryResponse = ddb.query(queryBuilder.build());
                    pageCount++;

                    for (Map<String, AttributeValue> item : queryResponse.items()) {
                        String userId = item.get("userId").s();
                        builder.addMessages(1);
                        builder.addUser(userId);
                    }

                    lastEvaluatedKey = queryResponse.lastEvaluatedKey();
                } while (lastEvaluatedKey != null && !lastEvaluatedKey.isEmpty());

                if (pageCount > 1) {
                    System.out.printf("[DDB] Room %d required %d pages for room stats\n", roomId, pageCount);
                }

                roomStats.put(String.valueOf(roomId), builder);
            }

            List<RoomStats> result = roomStats.values().stream()
                .map(RoomStatsBuilder::build)
                .sorted((a, b) -> Integer.compare(b.messageCount(), a.messageCount()))
                .limit(topN)
                .collect(Collectors.toList());

            long duration = System.currentTimeMillis() - queryStart;
            System.out.printf("[DDB Query] Most active rooms: top=%d, time=%dms\n", topN, duration);

            return result;

        } catch (DynamoDbException e) {
            System.err.println("[DDB] Most active rooms query failed: " + e.getMessage());
            return new ArrayList<>();
        }
    }

    // ============================================================================
    // Analytics Query 4: User participation patterns
    // ============================================================================

    /**
     * Get user participation statistics: how many rooms each user participates in, filtered by testId.
     */
    public Map<String, Integer> getUserParticipationPatterns(String testId) {
        long queryStart = System.currentTimeMillis();
        Map<String, Integer> participation = new HashMap<>();

        try {
            Map<String, AttributeValue> exprVals = new HashMap<>();
            exprVals.put(":testId", AttributeValue.builder().s(testId).build());

            ScanRequest request = ScanRequest.builder()
                .tableName(userRoomsTable)
                .filterExpression("testId = :testId")
                .expressionAttributeValues(exprVals)
                .projectionExpression("userId")
                .build();

            ScanResponse response = ddb.scan(request);

            for (Map<String, AttributeValue> item : response.items()) {
                String userId = item.get("userId").s();
                participation.put(userId, participation.getOrDefault(userId, 0) + 1);
            }

            long duration = System.currentTimeMillis() - queryStart;
            System.out.printf("[DDB Query] User participation patterns: users=%d, time=%dms\n",
                participation.size(), duration);

            return participation;

        } catch (DynamoDbException e) {
            System.err.println("[DDB] User participation query failed: " + e.getMessage());
            return new HashMap<>();
        }
    }

    // ============================================================================
    // Helper classes for aggregation
    // ============================================================================

    private static class UserStatsBuilder {
        private final String userId;
        private int totalMessages = 0;
        private long firstSeen = Long.MAX_VALUE;
        private long lastSeen = 0;

        UserStatsBuilder(String userId) {
            this.userId = userId;
        }

        void addMessages(int count) {
            totalMessages += count;
        }

        void updateActivity(long timestamp) {
            firstSeen = Math.min(firstSeen, timestamp);
            lastSeen = Math.max(lastSeen, timestamp);
        }

        UserStats build() {
            return new UserStats(
                userId,
                totalMessages,
                Instant.ofEpochMilli(firstSeen).toString(),
                Instant.ofEpochMilli(lastSeen).toString()
            );
        }
    }

    private static class RoomStatsBuilder {
        private final String roomId;
        private int totalMessages = 0;
        private final Set<String> uniqueUsers = new HashSet<>();

        RoomStatsBuilder(String roomId) {
            this.roomId = roomId;
        }

        void addMessages(int count) {
            totalMessages += count;
        }

        void addUser(String userId) {
            uniqueUsers.add(userId);
        }

        RoomStats build() {
            return new RoomStats(roomId, totalMessages, uniqueUsers.size());
        }
    }

    // ============================================================================
    // Helper Methods
    // ============================================================================

    /**
     * Generate list of hourly buckets between start and end time
     */
    private List<String> generateHourlyBuckets(Instant start, Instant end) {
        List<String> buckets = new ArrayList<>();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH");

        Instant current = start.atOffset(ZoneOffset.UTC)
            .withMinute(0).withSecond(0).withNano(0).toInstant();

        while (!current.isAfter(end)) {
            buckets.add(current.atOffset(ZoneOffset.UTC).format(formatter));
            current = current.plusSeconds(3600); // Add 1 hour
        }

        return buckets;
    }

    /**
     * Parse DynamoDB item into ChatMessage
     */
    private ChatMessage parseChatMessage(Map<String, AttributeValue> item) {
        return new ChatMessage(
            item.get("messageId").s(),
            item.get("roomId").s(),
            item.get("userId").s(),
            item.get("username").s(),
            item.get("message").s(),
            item.get("msgTimestamp").s(),
            item.get("messageType").s(),
            item.containsKey("serverId") ? item.get("serverId").s() : null,
            item.containsKey("clientIp") ? item.get("clientIp").s() : null
        );
    }

    // ============================================================================
    // Data Classes
    // ============================================================================

    public record ChatMessage(
        String messageId,
        String roomId,
        String userId,
        String username,
        String message,
        String timestamp,
        String messageType,
        String serverId,
        String clientIp
    ) {}

    public record UserRoomInfo(
        String roomId,
        long lastActivityTimestamp,
        int messageCount,
        String lastMessageId
    ) {}

    public record UserStats(
        String userId,
        int messageCount,
        String firstSeen,
        String lastSeen
    ) {}

    public record RoomStats(
        String roomId,
        int messageCount,
        int uniqueUsers
    ) {}
}
