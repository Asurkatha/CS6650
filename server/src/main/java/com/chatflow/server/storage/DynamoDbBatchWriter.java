package com.chatflow.server.storage;

import com.chatflow.server.mq.QueueMessage;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * High-performance batch writer for DynamoDB.
 * Implements write-behind pattern with configurable batching.
 *
 * Assignment 3 Requirements:
 * - Batch writes for optimal throughput
 * - Separate thread pool for database writes
 * - Idempotent writes using messageId
 * - Circuit breaker for resilience
 */
public class DynamoDbBatchWriter {

    private final DynamoDbClient ddb;
    private final String messagesTable;
    private final String userRoomsTable;
    private final String activityBucketsTable;
    private final String hourlyStatsTable;
    private final String dailyAggregatesTable;

    // Batching configuration
    private final int batchSize;
    private final long flushIntervalMs;

    // Write queues
    private final BlockingQueue<WriteRequest> messageQueue;
    private final BlockingQueue<UpdateRequest> userRoomQueue;
    private final BlockingQueue<ActivityBucketRequest> activityQueue;
    private final BlockingQueue<HourlyStatsRequest> hourlyStatsQueue;
    private final BlockingQueue<DailyAggregateRequest> dailyAggregatesQueue;

    // Thread pools
    private final ExecutorService writerExecutor;
    private final ScheduledExecutorService flushScheduler;
    private final int writerThreadCount;

    // Metrics
    private final AtomicLong messagesWritten = new AtomicLong(0);
    private final AtomicLong userRoomsUpdated = new AtomicLong(0);
    private final AtomicLong activityBucketsWritten = new AtomicLong(0);
    private final AtomicLong hourlyStatsWritten = new AtomicLong(0);
    private final AtomicLong dailyAggregatesWritten = new AtomicLong(0);
    private final AtomicLong writeFailures = new AtomicLong(0);
    private final AtomicLong duplicatesSkipped = new AtomicLong(0);

    // Track userIds efficiently (keep max 1000 for random selection)
    private final Set<String> trackedUserIds = ConcurrentHashMap.newKeySet();
    private static final int MAX_TRACKED_USERS = 1000;

    // Circuit breaker state
    private volatile boolean circuitOpen = false;
    private final AtomicLong consecutiveFailures = new AtomicLong(0);
    private static final int CIRCUIT_FAILURE_THRESHOLD = 10;
    private static final long CIRCUIT_RESET_MS = 30000;
    private volatile long circuitOpenedAt = 0;

    private volatile boolean running = true;

    public DynamoDbBatchWriter(int batchSize, long flushIntervalMs, int writerThreads) {
        this.ddb = DynamoDbClientProvider.getClient();
        this.messagesTable = System.getProperty("DDB_MESSAGES_TABLE", "Messages");
        this.userRoomsTable = System.getProperty("DDB_USERROOMS_TABLE", "UserRoomStats");
        this.activityBucketsTable = System.getProperty("DDB_ACTIVITY_BUCKETS_TABLE", "UserActivityBuckets");
        this.hourlyStatsTable = System.getProperty("DDB_HOURLY_STATS_TABLE", "HourlyMessageStats");
        this.dailyAggregatesTable = System.getProperty("DDB_DAILY_AGGREGATES_TABLE", "DailyAggregates");

        this.batchSize = batchSize;
        this.flushIntervalMs = flushIntervalMs;
        this.writerThreadCount = writerThreads;

        // Queues sized to handle bursts - increased for 500K-1M load testing
        this.messageQueue = new LinkedBlockingQueue<>(500000);  // 200K capacity - DOUBLED for load testing
        this.userRoomQueue = new LinkedBlockingQueue<>(500000);  // 100K capacity - DOUBLED for load testing
        this.activityQueue = new LinkedBlockingQueue<>(100000);  // 100K capacity - DOUBLED for load testing
        this.hourlyStatsQueue = new LinkedBlockingQueue<>(100000);  // 100K capacity - DOUBLED for load testing
        this.dailyAggregatesQueue = new LinkedBlockingQueue<>(100000);  // 100K capacity - DOUBLED for load testing

        this.writerExecutor = Executors.newFixedThreadPool(writerThreads,
            r -> new Thread(r, "DDB-Writer"));
        this.flushScheduler = Executors.newScheduledThreadPool(1,
            r -> new Thread(r, "DDB-Flush-Scheduler"));

        startWriters();
        startFlushScheduler();

        System.out.println("[DynamoDB] BatchWriter initialized: batchSize=" + batchSize +
                          ", flushInterval=" + flushIntervalMs + "ms, threads=" + writerThreads);
    }

    /**
     * Async write of a message (called from consumer after successful broadcast)
     */
    public boolean writeMessage(QueueMessage msg) {
        if (circuitOpen && !checkCircuitReset()) {
            writeFailures.incrementAndGet();
            return false;
        }

        // Track userId for query randomization (limited to MAX_TRACKED_USERS)
        if (trackedUserIds.size() < MAX_TRACKED_USERS) {
            trackedUserIds.add(msg.userId());
        }

        try {
            WriteRequest req = new WriteRequest(msg);
            // Block until space available - don't drop messages!
            messageQueue.put(req);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            writeFailures.incrementAndGet();
            return false;
        }
    }

    /**
     * Start background writer threads (multiple workers per queue for parallelism)
     */
    private void startWriters() {
        // Message batch writers (scale with thread pool size for high throughput)
        int messageWriters = Math.max(28, writerThreadCount / 3);
        for (int i = 0; i < messageWriters; i++) {
            writerExecutor.submit(() -> {
            List<WriteRequest> batch = new ArrayList<>(batchSize);
            while (running) {
                try {
                    WriteRequest req = messageQueue.poll(100, TimeUnit.MILLISECONDS);
                    if (req != null) {
                        batch.add(req);
                        messageQueue.drainTo(batch, batchSize - batch.size());

                        if (batch.size() >= batchSize) {
                            flushMessageBatch(batch);
                            batch.clear();
                        }
                    } else if (!batch.isEmpty()) {
                        // Timeout flush
                        flushMessageBatch(batch);
                        batch.clear();
                    }
                } catch (Exception e) {
                    System.err.println("[DDB] Message writer error: " + e.getMessage());
                }
            }
            // Final flush
            if (!batch.isEmpty()) {
                flushMessageBatch(batch);
            }
            });
        }

        // UserRoom batch writers (scale with thread pool size)
        int userRoomWriters = Math.max(32, writerThreadCount / 2);
        for (int i = 0; i < userRoomWriters; i++) {
            writerExecutor.submit(() -> {
            List<UpdateRequest> batch = new ArrayList<>(batchSize);
            while (running) {
                try {
                    UpdateRequest req = userRoomQueue.poll(100, TimeUnit.MILLISECONDS);
                    if (req != null) {
                        batch.add(req);
                        userRoomQueue.drainTo(batch, batchSize - batch.size());

                        if (batch.size() >= batchSize / 2) { // Updates are more expensive
                            flushUserRoomBatch(batch);
                            batch.clear();
                        }
                    } else if (!batch.isEmpty()) {
                        flushUserRoomBatch(batch);
                        batch.clear();
                    }
                } catch (Exception e) {
                    System.err.println("[DDB] UserRoom writer error: " + e.getMessage());
                }
            }
            if (!batch.isEmpty()) {
                flushUserRoomBatch(batch);
            }
            });
        }


        // HourlyStats writers (4 parallel workers)
        for (int k = 0; k < 4; k++) {
            writerExecutor.submit(() -> {
            List<HourlyStatsRequest> batch = new ArrayList<>(batchSize);
            while (running) {
                try {
                    HourlyStatsRequest req = hourlyStatsQueue.poll(100, TimeUnit.MILLISECONDS);
                    if (req != null) {
                        batch.add(req);
                        hourlyStatsQueue.drainTo(batch, batchSize - batch.size());

                        if (batch.size() >= batchSize / 2) {
                            flushHourlyStatsBatch(batch);
                            batch.clear();
                        }
                    } else if (!batch.isEmpty()) {
                        flushHourlyStatsBatch(batch);
                        batch.clear();
                    }
                } catch (Exception e) {
                    System.err.println("[DDB] HourlyStats writer error: " + e.getMessage());
                }
            }
            if (!batch.isEmpty()) {
                flushHourlyStatsBatch(batch);
            }
            });
        }

    }

    /**
     * Periodic flush to ensure low latency even with small volumes
     */
    private void startFlushScheduler() {
        flushScheduler.scheduleAtFixedRate(() -> {
            int msgQDepth = messageQueue.size();
            int userRoomQDepth = userRoomQueue.size();
            int activityQDepth = activityQueue.size();

            int hourlyQDepth = hourlyStatsQueue.size();
            int dailyQDepth = dailyAggregatesQueue.size();

            // Log queue depths every interval
            System.out.printf("[DDB Queues] Msgs: %d | UserRooms: %d | Activity: %d | Hourly: %d | Daily: %d | Written: %d | Failures: %d\n",
                msgQDepth, userRoomQDepth, activityQDepth, hourlyQDepth, dailyQDepth, messagesWritten.get(), writeFailures.get());

            // Warn if queues are getting full
            if (msgQDepth > 50000) {
                System.err.println("[DDB] WARNING: Message queue is " + (msgQDepth * 100 / 100000) + "% full!");
            }
        }, flushIntervalMs, flushIntervalMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Flush a batch of messages to DynamoDB
     * DynamoDB limit: max 25 items per batch, so split into chunks
     */
    private void flushMessageBatch(List<WriteRequest> batch) {
        if (batch.isEmpty()) return;

        // DynamoDB max batch size is 25, split if needed
        final int MAX_BATCH_SIZE = 25;

        for (int i = 0; i < batch.size(); i += MAX_BATCH_SIZE) {
            int end = Math.min(i + MAX_BATCH_SIZE, batch.size());
            List<WriteRequest> chunk = batch.subList(i, end);
            flushMessageChunk(chunk);
        }
    }

    /**
     * Flush a single chunk (max 25 items) to DynamoDB
     */
    private void flushMessageChunk(List<WriteRequest> chunk) {
        if (chunk.isEmpty()) return;

        try {
            Map<String, List<software.amazon.awssdk.services.dynamodb.model.WriteRequest>> requestItems = new HashMap<>();
            List<software.amazon.awssdk.services.dynamodb.model.WriteRequest> writes = new ArrayList<>();

            // Deduplicate by messageId (unique per message)
            Set<String> seenKeys = new HashSet<>();

            for (WriteRequest req : chunk) {
                String messageId = req.msg.messageId();

                // Skip duplicates within this batch
                if (!seenKeys.add(messageId)) {
                    duplicatesSkipped.incrementAndGet();
                    continue;
                }

                // PRODUCTION SCHEMA: Use testId#roomId as partition key
                String testId_roomId = req.msg.testId() + "#" + req.msg.roomId();
                String testId_userId = req.msg.testId() + "#" + req.msg.userId();

                // Make msgTimestamp unique by appending messageId
                // Format: "2025-11-18T15:25:55.123Z#msgId" - preserves chronological order
                String uniqueTimestamp = req.msg.timestamp() + "#" + req.msg.messageId();

                Map<String, AttributeValue> item = new HashMap<>();
                // Primary key: testId_roomId (HASH) + msgTimestamp (RANGE)
                item.put("testId_roomId", AttributeValue.builder().s(testId_roomId).build());
                item.put("msgTimestamp", AttributeValue.builder().s(uniqueTimestamp).build());

                // GSI_TestUser composite key: testId_userId
                item.put("testId_userId", AttributeValue.builder().s(testId_userId).build());

                // Data attributes
                item.put("messageId", AttributeValue.builder().s(req.msg.messageId()).build());
                item.put("roomId", AttributeValue.builder().s(req.msg.roomId()).build());
                item.put("userId", AttributeValue.builder().s(req.msg.userId()).build());
                item.put("username", AttributeValue.builder().s(req.msg.username()).build());
                item.put("message", AttributeValue.builder().s(req.msg.message()).build());
                item.put("messageType", AttributeValue.builder().s(req.msg.messageType()).build());
                item.put("serverId", AttributeValue.builder().s(req.msg.serverId()).build());
                item.put("clientIp", AttributeValue.builder().s(req.msg.clientIp()).build());
                item.put("testId", AttributeValue.builder().s(req.msg.testId()).build());

                // Time bucket attributes for analytics
                String timestamp = req.msg.timestamp();
                item.put("hourBucket", AttributeValue.builder().s(extractHourBucket(timestamp)).build());
                item.put("dayBucket", AttributeValue.builder().s(extractDayBucket(timestamp)).build());

                // TTL: 60 minutes from message timestamp
                try {
                    long epochSeconds = Instant.parse(timestamp).getEpochSecond();
                    long ttl = epochSeconds + (60 * 60); // 60 minutes
                    item.put("ttl", AttributeValue.builder().n(String.valueOf(ttl)).build());
                } catch (Exception e) {
                    // If timestamp parsing fails, use current time + 60 minutes
                    long ttl = Instant.now().getEpochSecond() + (60 * 60);
                    item.put("ttl", AttributeValue.builder().n(String.valueOf(ttl)).build());
                }

                writes.add(software.amazon.awssdk.services.dynamodb.model.WriteRequest.builder()
                    .putRequest(PutRequest.builder().item(item).build())
                    .build());
            }

            requestItems.put(messagesTable, writes);

            BatchWriteItemRequest batchRequest = BatchWriteItemRequest.builder()
                .requestItems(requestItems)
                .build();

            BatchWriteItemResponse response = ddb.batchWriteItem(batchRequest);

            // Handle unprocessed items with exponential backoff retry + jitter
            Map<String, List<software.amazon.awssdk.services.dynamodb.model.WriteRequest>> unprocessed = response.unprocessedItems();
            int retryCount = 0;
            long baseBackoffMs = 50;
            final int MAX_RETRIES = 5;  // Increased from 3 to 5

            while (unprocessed != null && !unprocessed.isEmpty() && retryCount < MAX_RETRIES) {
                int unprocessedCount = unprocessed.values().stream().mapToInt(List::size).sum();
                System.out.println("[DDB] Retrying " + unprocessedCount + " unprocessed items (attempt " + (retryCount + 1) + ")");

                // Exponential backoff with jitter to avoid thundering herd
                long backoffMs = baseBackoffMs * (1L << retryCount); // 50, 100, 200, 400, 800 ms
                long jitter = (long) (backoffMs * 0.3 * Math.random()); // ±30% jitter
                long sleepMs = backoffMs + jitter;

                try {
                    Thread.sleep(sleepMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }

                BatchWriteItemRequest retryRequest = BatchWriteItemRequest.builder()
                    .requestItems(unprocessed)
                    .build();

                response = ddb.batchWriteItem(retryRequest);
                unprocessed = response.unprocessedItems();

                retryCount++;
            }

            if (unprocessed != null && !unprocessed.isEmpty()) {
                int finalUnprocessed = unprocessed.values().stream().mapToInt(List::size).sum();
                System.err.println("[DDB] Failed to write " + finalUnprocessed + " items after " + MAX_RETRIES + " retries");
                writeFailures.addAndGet(finalUnprocessed);
            }

            messagesWritten.addAndGet(chunk.size() - (unprocessed != null && unprocessed.containsKey(messagesTable) ? unprocessed.get(messagesTable).size() : 0));
            consecutiveFailures.set(0);

            // PRODUCTION SCHEMA: Async updates to pre-aggregated tables
            // UserRoomStats and HourlyMessageStats are batched and updated in background
            // Write amplification: ~1.02x (only unique user-room combos and hourly buckets)
            for (WriteRequest req : chunk) {
                updateUserRoomAsync(req.msg);
                updateHourlyStatsAsync(req.msg);
            }

        } catch (DynamoDbException e) {
            System.err.println("[DDB] Batch write failed: " + e.getMessage());
            writeFailures.addAndGet(chunk.size());
            handleFailure();
        }
    }

    /**
     * Flush UserRoom updates (PRODUCTION SCHEMA)
     * PK: testId_userId, SK: roomId
     * GSI_TestTopUsers: PK=testId, SK=messageCount (DESC)
     * GSI_RoomTopUsers: PK=roomId, SK=messageCount (DESC)
     *
     * OPTIMIZED: Aggregate updates in memory first to reduce API calls
     */
    private void flushUserRoomBatch(List<UpdateRequest> batch) {
        if (batch.isEmpty()) return;

        // Aggregate by testId_userId#roomId to reduce update calls
        Map<String, UserRoomAggregate> aggregates = new HashMap<>();

        for (UpdateRequest req : batch) {
            String testId_userId = req.testId + "#" + req.userId;
            String key = testId_userId + "#" + req.roomId;

            UserRoomAggregate agg = aggregates.get(key);
            if (agg == null) {
                agg = new UserRoomAggregate(testId_userId, req.roomId, req.testId, req.userId);
                aggregates.put(key, agg);
            }

            agg.messageCount++;
            agg.lastTimestamp = Math.max(agg.lastTimestamp, req.timestamp);
            agg.lastMessageId = req.messageId;
            agg.ttl = Math.max(agg.ttl, (req.timestamp / 1000) + (60 * 60));
        }

        // Now flush aggregated updates
        for (UserRoomAggregate agg : aggregates.values()) {
            try {
                Map<String, AttributeValue> key = Map.of(
                    "testId_userId", AttributeValue.builder().s(agg.testId_userId).build(),
                    "roomId", AttributeValue.builder().s(agg.roomId).build()
                );

                Map<String, AttributeValue> exprVals = new HashMap<>();
                exprVals.put(":ts", AttributeValue.builder().s(Long.toString(agg.lastTimestamp)).build());
                exprVals.put(":mid", AttributeValue.builder().s(agg.lastMessageId).build());
                exprVals.put(":count", AttributeValue.builder().n(String.valueOf(agg.messageCount)).build());
                exprVals.put(":testId", AttributeValue.builder().s(agg.testId).build());
                exprVals.put(":userId", AttributeValue.builder().s(agg.userId).build());
                exprVals.put(":ttl", AttributeValue.builder().n(String.valueOf(agg.ttl)).build());

                UpdateItemRequest updateReq = UpdateItemRequest.builder()
                    .tableName(userRoomsTable)
                    .key(key)
                    .updateExpression("SET lastActivityTimestamp = :ts, lastMessageId = :mid, testId = :testId, userId = :userId, #ttl = :ttl ADD messageCount :count")
                    .expressionAttributeNames(Map.of("#ttl", "ttl"))
                    .expressionAttributeValues(exprVals)
                    .build();

                ddb.updateItem(updateReq);
                userRoomsUpdated.addAndGet(agg.messageCount);

            } catch (DynamoDbException e) {
                // Non-critical, just log
                System.err.println("[DDB] UserRoom update failed: " + e.getMessage());
            }
        }
    }

    /**
     * Helper class for aggregating UserRoom updates
     */
    private static class UserRoomAggregate {
        final String testId_userId;
        final String roomId;
        final String testId;
        final String userId;
        int messageCount = 0;
        long lastTimestamp = 0;
        String lastMessageId = null;
        long ttl = 0;

        UserRoomAggregate(String testId_userId, String roomId, String testId, String userId) {
            this.testId_userId = testId_userId;
            this.roomId = roomId;
            this.testId = testId;
            this.userId = userId;
        }
    }

    /**
     * Flush activity bucket writes
     * DynamoDB limit: max 25 items per batch, so split into chunks
     */
    private void flushActivityBatch(List<ActivityBucketRequest> batch) {
        if (batch.isEmpty()) return;

        final int MAX_BATCH_SIZE = 25;

        for (int i = 0; i < batch.size(); i += MAX_BATCH_SIZE) {
            int end = Math.min(i + MAX_BATCH_SIZE, batch.size());
            List<ActivityBucketRequest> chunk = batch.subList(i, end);
            flushActivityChunk(chunk);
        }
    }

    private void flushActivityChunk(List<ActivityBucketRequest> chunk) {
        if (chunk.isEmpty()) return;

        try {
            Map<String, List<software.amazon.awssdk.services.dynamodb.model.WriteRequest>> requestItems = new HashMap<>();
            List<software.amazon.awssdk.services.dynamodb.model.WriteRequest> writes = new ArrayList<>();

            // Deduplicate by bucket+userId (keep latest timestamp)
            Map<String, ActivityBucketRequest> deduped = new HashMap<>();
            for (ActivityBucketRequest req : chunk) {
                String key = req.bucket + "#" + req.userId;
                ActivityBucketRequest existing = deduped.get(key);
                if (existing == null || req.timestamp > existing.timestamp) {
                    deduped.put(key, req);
                }
            }

            for (ActivityBucketRequest req : deduped.values()) {
                Map<String, AttributeValue> item = new HashMap<>();
                item.put("hourBucket", AttributeValue.builder().s(req.bucket).build());
                item.put("userId", AttributeValue.builder().s(req.userId).build());
                item.put("lastSeen", AttributeValue.builder().n(Long.toString(req.timestamp)).build());
                item.put("testId", AttributeValue.builder().s(req.testId).build()); // Add testId
                if (req.ttl != null) {
                    item.put("ttl", AttributeValue.builder().n(Long.toString(req.ttl)).build());
                }

                writes.add(software.amazon.awssdk.services.dynamodb.model.WriteRequest.builder()
                    .putRequest(PutRequest.builder().item(item).build())
                    .build());
            }

            requestItems.put(activityBucketsTable, writes);

            BatchWriteItemRequest batchRequest = BatchWriteItemRequest.builder()
                .requestItems(requestItems)
                .build();

            ddb.batchWriteItem(batchRequest);
            activityBucketsWritten.addAndGet(chunk.size());

        } catch (DynamoDbException e) {
            // Non-critical for activity tracking
            System.err.println("[DDB] Activity batch write failed: " + e.getMessage());
        }
    }

    /**
     * Queue UserRoom update asynchronously
     */
    private void updateUserRoomAsync(QueueMessage msg) {
        try {
            long timestamp = Instant.parse(msg.timestamp()).toEpochMilli();
            UpdateRequest req = new UpdateRequest(msg.userId(), msg.roomId(), timestamp, msg.messageId(), msg.testId());
            // CRITICAL: Use put() not offer() - blocks instead of dropping data
            userRoomQueue.put(req);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            writeFailures.incrementAndGet();
        } catch (Exception e) {
            writeFailures.incrementAndGet();
        }
    }

    /**
     * Queue activity bucket update asynchronously
     */
    private void updateActivityBucketAsync(QueueMessage msg) {
        try {
            Instant instant = Instant.parse(msg.timestamp());
            String bucket = generateHourlyBucket(instant);
            long timestamp = instant.toEpochMilli();
            long ttl = instant.plus(7, ChronoUnit.DAYS).getEpochSecond(); // 7 day TTL

            ActivityBucketRequest req = new ActivityBucketRequest(bucket, msg.userId(), timestamp, ttl, msg.testId());
            // CRITICAL: Use put() not offer() - blocks instead of dropping data
            activityQueue.put(req);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            writeFailures.incrementAndGet();
        } catch (Exception e) {
            writeFailures.incrementAndGet();
        }
    }

    /**
     * Flush HourlyStats updates using UpdateItem with ADD for counters
     */
    private void flushHourlyStatsBatch(List<HourlyStatsRequest> batch) {
        if (batch.isEmpty()) return;

        // Aggregate by testId + hourBucket (matches HourlyMessageStats schema)
        Map<String, HourlyStatsAggregate> aggregates = new HashMap<>();

        for (HourlyStatsRequest req : batch) {
            String key = req.testId + "#" + req.hourBucket;
            HourlyStatsAggregate agg = aggregates.computeIfAbsent(key,
                k -> new HourlyStatsAggregate(req.testId, req.hourBucket, req.statType, req.entityId, req.ttl, req.testId));

            agg.messageCount++;
            agg.lastSeen = Math.max(agg.lastSeen, req.timestamp);

            // Count by message type (not needed for simple schema, but keep for future)
            switch (req.messageType) {
                case "TEXT": agg.textMessages++; break;
                case "JOIN": agg.joinMessages++; break;
                case "LEAVE": agg.leaveMessages++; break;
            }
        }

        // Write each aggregate using UpdateItem (HourlyMessageStats schema: testId + hourBucket)
        for (HourlyStatsAggregate agg : aggregates.values()) {
            try {
                // Use testId as partition key, hourBucket as sort key
                Map<String, AttributeValue> key = Map.of(
                    "testId", AttributeValue.builder().s(agg.testId).build(),
                    "hourBucket", AttributeValue.builder().s(agg.hourBucket).build()
                );

                Map<String, AttributeValue> exprVals = new HashMap<>();
                exprVals.put(":msgCount", AttributeValue.builder().n(String.valueOf(agg.messageCount)).build());
                exprVals.put(":lastSeen", AttributeValue.builder().n(String.valueOf(agg.lastSeen)).build());
                exprVals.put(":ttl", AttributeValue.builder().n(String.valueOf(agg.ttl)).build());

                UpdateItemRequest updateReq = UpdateItemRequest.builder()
                    .tableName(hourlyStatsTable)
                    .key(key)
                    .updateExpression("SET lastSeen = :lastSeen, #ttl = :ttl ADD messageCount :msgCount")
                    .expressionAttributeNames(Map.of("#ttl", "ttl"))
                    .expressionAttributeValues(exprVals)
                    .build();

                ddb.updateItem(updateReq);
                hourlyStatsWritten.incrementAndGet();

            } catch (DynamoDbException e) {
                System.err.println("[DDB] HourlyStats update failed: " + e.getMessage());
            }
        }
    }

    /**
     * Helper class for aggregating hourly stats
     */
    private static class HourlyStatsAggregate {
        final String entityKey;
        final String hourBucket;
        final String statType;
        final String entityId;
        final Long ttl;
        final String testId;
        int messageCount = 0;
        int textMessages = 0;
        int joinMessages = 0;
        int leaveMessages = 0;
        long lastSeen = 0;

        HourlyStatsAggregate(String entityKey, String hourBucket, String statType, String entityId, Long ttl, String testId) {
            this.entityKey = entityKey;
            this.hourBucket = hourBucket;
            this.statType = statType;
            this.entityId = entityId;
            this.ttl = ttl;
            this.testId = testId;
        }
    }

    /**
     * Flush DailyAggregates updates using UpdateItem with ADD for counters
     */
    private void flushDailyAggregatesBatch(List<DailyAggregateRequest> batch) {
        if (batch.isEmpty()) return;

        // Aggregate by partitionKey + entityId + testId
        Map<String, DailyAggregateAggregate> aggregates = new HashMap<>();

        for (DailyAggregateRequest req : batch) {
            String key = req.partitionKey + "#" + req.entityId + "#" + req.testId;
            DailyAggregateAggregate agg = aggregates.computeIfAbsent(key,
                k -> new DailyAggregateAggregate(req.partitionKey, req.entityId, req.entityType, req.dayBucket, req.ttl, req.testId));

            agg.messageCount++;
            agg.relatedIds.add(req.relatedId);
            agg.firstSeen = agg.firstSeen == 0 ? req.timestamp : Math.min(agg.firstSeen, req.timestamp);
            agg.lastSeen = Math.max(agg.lastSeen, req.timestamp);
        }

        // Write each aggregate using UpdateItem
        for (DailyAggregateAggregate agg : aggregates.values()) {
            try {
                // Create sortKey with padded message count for lexicographic sorting
                String sortKey = String.format("%08d#%s", agg.messageCount, agg.entityId);

                Map<String, AttributeValue> key = Map.of(
                    "partitionKey", AttributeValue.builder().s(agg.partitionKey).build(),
                    "sortKey", AttributeValue.builder().s(sortKey).build()
                );

                Map<String, AttributeValue> exprVals = new HashMap<>();
                exprVals.put(":msgCount", AttributeValue.builder().n(String.valueOf(agg.messageCount)).build());
                exprVals.put(":entityType", AttributeValue.builder().s(agg.entityType).build());
                exprVals.put(":dayBucket", AttributeValue.builder().s(agg.dayBucket).build());
                exprVals.put(":entityId", AttributeValue.builder().s(agg.entityId).build());
                exprVals.put(":uniqueCount", AttributeValue.builder().n(String.valueOf(agg.relatedIds.size())).build());
                exprVals.put(":firstSeen", AttributeValue.builder().n(String.valueOf(agg.firstSeen)).build());
                exprVals.put(":lastSeen", AttributeValue.builder().n(String.valueOf(agg.lastSeen)).build());
                exprVals.put(":ttl", AttributeValue.builder().n(String.valueOf(agg.ttl)).build());
                exprVals.put(":testId", AttributeValue.builder().s(agg.testId).build());

                String uniqueField = agg.entityType.equals("USER") ? "uniqueRooms" : "uniqueUsers";

                UpdateItemRequest updateReq = UpdateItemRequest.builder()
                    .tableName(dailyAggregatesTable)
                    .key(key)
                    .updateExpression("SET entityType = :entityType, dayBucket = :dayBucket, entityId = :entityId, " +
                                    "messageCount = :msgCount, " + uniqueField + " = :uniqueCount, " +
                                    "firstSeen = :firstSeen, lastSeen = :lastSeen, #ttl = :ttl, testId = :testId")
                    .expressionAttributeNames(Map.of("#ttl", "ttl"))
                    .expressionAttributeValues(exprVals)
                    .build();

                ddb.updateItem(updateReq);
                dailyAggregatesWritten.incrementAndGet();

            } catch (DynamoDbException e) {
                System.err.println("[DDB] DailyAggregates update failed: " + e.getMessage());
            }
        }
    }

    /**
     * Helper class for aggregating daily stats
     */
    private static class DailyAggregateAggregate {
        final String partitionKey;
        final String entityId;
        final String entityType;
        final String dayBucket;
        final Long ttl;
        final String testId;
        int messageCount = 0;
        Set<String> relatedIds = new HashSet<>();
        long firstSeen = 0;
        long lastSeen = 0;

        DailyAggregateAggregate(String partitionKey, String entityId, String entityType, String dayBucket, Long ttl, String testId) {
            this.partitionKey = partitionKey;
            this.entityId = entityId;
            this.entityType = entityType;
            this.dayBucket = dayBucket;
            this.ttl = ttl;
            this.testId = testId;
        }
    }

    /**
     * Queue HourlyStats update asynchronously
     */
    private void updateHourlyStatsAsync(QueueMessage msg) {
        try {
            Instant instant = Instant.parse(msg.timestamp());
            String hourBucket = extractHourBucket(msg.timestamp());
            long timestamp = instant.toEpochMilli();
            long ttl = instant.plus(1, ChronoUnit.HOURS).getEpochSecond(); // 60 minute TTL

            // HourlyMessageStats schema: testId (PK) + hourBucket (SK)
            HourlyStatsRequest req = new HourlyStatsRequest(
                msg.testId(),      // Use testId as partition key
                hourBucket,        // hourBucket as sort key
                "ALL",             // statType (not used in key)
                "ALL",             // entityId (not used in key)
                msg.messageType(),
                timestamp,
                ttl,
                msg.testId()
            );

            hourlyStatsQueue.put(req);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            writeFailures.incrementAndGet();
        } catch (Exception e) {
            writeFailures.incrementAndGet();
        }
    }

    /**
     * Queue DailyAggregates update asynchronously
     */
    private void updateDailyAggregatesAsync(QueueMessage msg) {
        try {
            Instant instant = Instant.parse(msg.timestamp());
            String dayBucket = extractDayBucket(msg.timestamp());
            long timestamp = instant.toEpochMilli();
            long ttl = instant.plus(30, ChronoUnit.DAYS).getEpochSecond(); // 30 day TTL

            // Create aggregates for: USER and ROOM
            DailyAggregateRequest userReq = new DailyAggregateRequest("USER#" + dayBucket, msg.userId(), "USER", dayBucket, msg.roomId(), timestamp, ttl, msg.testId());
            DailyAggregateRequest roomReq = new DailyAggregateRequest("ROOM#" + dayBucket, msg.roomId(), "ROOM", dayBucket, msg.userId(), timestamp, ttl, msg.testId());

            dailyAggregatesQueue.put(userReq);
            dailyAggregatesQueue.put(roomReq);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            writeFailures.incrementAndGet();
        } catch (Exception e) {
            writeFailures.incrementAndGet();
        }
    }

    /**
     * Generate hourly bucket string (e.g., "2025-11-16T15")
     */
    private String generateHourlyBucket(Instant instant) {
        return instant.atOffset(ZoneOffset.UTC)
            .format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH"));
    }

    /**
     * Circuit breaker: handle failure
     */
    private void handleFailure() {
        long failures = consecutiveFailures.incrementAndGet();
        if (failures >= CIRCUIT_FAILURE_THRESHOLD) {
            circuitOpen = true;
            circuitOpenedAt = System.currentTimeMillis();
            System.err.println("[DDB] Circuit breaker OPEN after " + failures + " failures");
        }
    }

    /**
     * Circuit breaker: check if we should reset
     */
    private boolean checkCircuitReset() {
        if (!circuitOpen) return true;

        if (System.currentTimeMillis() - circuitOpenedAt > CIRCUIT_RESET_MS) {
            circuitOpen = false;
            consecutiveFailures.set(0);
            System.out.println("[DDB] Circuit breaker RESET");
            return true;
        }
        return false;
    }

    /**
     * Graceful shutdown
     */
    public void shutdown() {
        System.out.println("[DDB] Shutting down batch writer...");
        running = false;

        flushScheduler.shutdown();
        writerExecutor.shutdown();

        try {
            if (!writerExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                writerExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            writerExecutor.shutdownNow();
        }

        printStats();
    }

    /**
     * Get metrics
     */
    public void printStats() {
        System.out.println("\n[DynamoDB Batch Writer Stats]");
        System.out.println("  Messages written: " + messagesWritten.get());
        System.out.println("  UserRooms updated: " + userRoomsUpdated.get());
        System.out.println("  Activity buckets: " + activityBucketsWritten.get());
        System.out.println("  HourlyStats written: " + hourlyStatsWritten.get());
        System.out.println("  DailyAggregates written: " + dailyAggregatesWritten.get());
        System.out.println("  Write failures: " + writeFailures.get());
        System.out.println("  Duplicates skipped: " + duplicatesSkipped.get());
        System.out.println("  Queue depths - Msgs: " + messageQueue.size() +
                          ", UserRooms: " + userRoomQueue.size() +
                          ", Hourly: " + hourlyStatsQueue.size() );
    }

    public long getMessagesWritten() { return messagesWritten.get(); }
    public long getWriteFailures() { return writeFailures.get(); }
    public int getQueueDepth() { return messageQueue.size(); }
    public Set<String> getTrackedUserIds() { return new HashSet<>(trackedUserIds); }

    /**
     * Extract hour bucket from ISO-8601 timestamp
     * Example: "2025-11-18T05:30:45.123Z" -> "2025-11-18T05"
     */
    private String extractHourBucket(String isoTimestamp) {
        try {
            if (isoTimestamp.length() >= 13) {
                return isoTimestamp.substring(0, 13); // YYYY-MM-DDTHH
            }
        } catch (Exception e) {
            System.err.println("[DDB] Error extracting hour bucket: " + e.getMessage());
        }
        return Instant.now().truncatedTo(ChronoUnit.HOURS).toString().substring(0, 13);
    }

    /**
     * Extract day bucket from ISO-8601 timestamp
     * Example: "2025-11-18T05:30:45.123Z" -> "2025-11-18"
     */
    private String extractDayBucket(String isoTimestamp) {
        try {
            if (isoTimestamp.length() >= 10) {
                return isoTimestamp.substring(0, 10); // YYYY-MM-DD
            }
        } catch (Exception e) {
            System.err.println("[DDB] Error extracting day bucket: " + e.getMessage());
        }
        return Instant.now().atZone(ZoneOffset.UTC).toLocalDate().toString();
    }

    // Internal request objects
    private static class WriteRequest {
        final QueueMessage msg;
        WriteRequest(QueueMessage msg) { this.msg = msg; }
    }

    private static class UpdateRequest {
        final String userId;
        final String roomId;
        final long timestamp;
        final String messageId;
        final String testId;

        UpdateRequest(String userId, String roomId, long timestamp, String messageId, String testId) {
            this.userId = userId;
            this.roomId = roomId;
            this.timestamp = timestamp;
            this.messageId = messageId;
            this.testId = testId;
        }
    }

    private static class ActivityBucketRequest {
        final String bucket;
        final String userId;
        final long timestamp;
        final Long ttl;
        final String testId;

        ActivityBucketRequest(String bucket, String userId, long timestamp, Long ttl, String testId) {
            this.bucket = bucket;
            this.userId = userId;
            this.timestamp = timestamp;
            this.ttl = ttl;
            this.testId = testId;
        }
    }

    private static class HourlyStatsRequest {
        final String entityKey;    // e.g., "GLOBAL#ALL" or "ROOM#5"
        final String hourBucket;   // e.g., "2025-11-18T05"
        final String statType;     // GLOBAL, ROOM, USER
        final String entityId;     // ALL, roomId, userId
        final String messageType;  // TEXT, JOIN, LEAVE
        final long timestamp;
        final Long ttl;
        final String testId;

        HourlyStatsRequest(String entityKey, String hourBucket, String statType, String entityId, String messageType, long timestamp, Long ttl, String testId) {
            this.entityKey = entityKey;
            this.hourBucket = hourBucket;
            this.statType = statType;
            this.entityId = entityId;
            this.messageType = messageType;
            this.timestamp = timestamp;
            this.ttl = ttl;
            this.testId = testId;
        }
    }

    private static class DailyAggregateRequest {
        final String partitionKey;  // e.g., "USER#2025-11-18" or "ROOM#2025-11-18"
        final String entityId;      // userId or roomId
        final String entityType;    // USER or ROOM
        final String dayBucket;     // e.g., "2025-11-18"
        final String relatedId;     // roomId (for USER) or userId (for ROOM)
        final long timestamp;
        final Long ttl;
        final String testId;

        DailyAggregateRequest(String partitionKey, String entityId, String entityType, String dayBucket, String relatedId, long timestamp, Long ttl, String testId) {
            this.partitionKey = partitionKey;
            this.entityId = entityId;
            this.entityType = entityType;
            this.dayBucket = dayBucket;
            this.relatedId = relatedId;
            this.timestamp = timestamp;
            this.ttl = ttl;
            this.testId = testId;
        }
    }
}
