package com.chatflow.server;

import com.chatflow.server.api.HealthServer;
import com.chatflow.server.api.MonitorEndpoint;
import com.chatflow.server.consumer.MessageConsumer;
import com.chatflow.server.http.QueryHandler;
import com.chatflow.server.http.ExportQueryHandler;
import com.chatflow.server.http.QueryResultsHandler;
import com.chatflow.server.metrics.MetricsTracker;
import com.chatflow.server.metrics.QueueStatsTracker;
import com.chatflow.server.mq.ChannelPool;
import com.chatflow.server.mq.MessagePublisher;
import com.chatflow.server.mq.QueueInitializer;
import com.chatflow.server.ws.ChatEndpoint;
import com.chatflow.server.ws.RoomManager;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

/**
 * Main application class for the ChatFlow server.
 *
 * Responsibilities:
 * - Initialize RabbitMQ connections and channel pool
 * - Start WebSocket server for client connections
 * - Start HTTP server for health checks and query endpoints
 * - Initialize DynamoDB batch writer for message persistence
 * - Start consumer threads for message processing
 * - Handle graceful shutdown
 */
public class App {

    private static final int ROOM_COUNT = 20;

    private static List<MessageConsumer> consumers = new ArrayList<>();
    private static List<Thread> consumerThreads = new ArrayList<>();
    private static Thread queueMetricsThread;

    public static void main(String[] args) throws Exception {
        System.out.println("=".repeat(60));
        System.out.println("ChatFlow Server - Assignment 3");
        System.out.println("=".repeat(60));

        // Configuration from system properties or defaults
        int wsPort = getIntProp("WS_PORT", 8080);
        int httpPort = getIntProp("HTTP_PORT", 8081);
        String rabbitHost = System.getProperty("RABBIT_HOST", "172.31.43.127");
        int rabbitPort = getIntProp("RABBIT_PORT", 5672);
        String rabbitUser = System.getProperty("RABBIT_USER", "asurkatha");
        String rabbitPass = System.getProperty("RABBIT_PASS", "SecurePassword123");
        int channelPoolSize = getIntProp("CHANNEL_POOL_SIZE", 200);
        int consumerThreadCount = getIntProp("CONSUMER_THREADS", 80);
        int prefetchCount = getIntProp("PREFETCH_COUNT", 1000);
        int queueMaxLength = getIntProp("QUEUE_MAX_LENGTH", 1000000);
        boolean queueLazyMode = Boolean.parseBoolean(System.getProperty("QUEUE_LAZY_MODE", "true"));
        int publisherWorkers = getIntProp("PUBLISHER_WORKERS", 64);
        int publisherQueueCapacity = getIntProp("PUBLISHER_QUEUE_CAPACITY", 1000000);
        int publisherConfirmBatch = getIntProp("PUBLISHER_CONFIRM_BATCH", 100);
        String serverId = System.getProperty("SERVER_ID",
                "server-" + UUID.randomUUID().toString().substring(0, 8));

        // DynamoDB configuration - optimized for 5000 msg/s throughput
        // At 5000 msg/s: 500 msgs per 100ms flush, split into batches of 500
        boolean enableDynamoDB = Boolean.parseBoolean(System.getProperty("ENABLE_DYNAMODB", "true"));
        int ddbBatchSize = getIntProp("DDB_BATCH_SIZE", 1000);
        long ddbFlushIntervalMs = Long.parseLong(System.getProperty("DDB_FLUSH_INTERVAL", "150"));
        int ddbWriterThreads = getIntProp("DDB_WRITER_THREADS", 64);

        System.out.println("Configuration:");
        System.out.println("  Server ID: " + serverId);
        System.out.println("  WebSocket Port: " + wsPort);
        System.out.println("  Health Port: " + httpPort);
        System.out.println("  RabbitMQ: " + rabbitHost + ":" + rabbitPort);
        System.out.println("  Consumer Threads: " + consumerThreadCount);
        System.out.println("  Channel Pool Size: " + channelPoolSize);
        System.out.println("  Prefetch Count: " + prefetchCount);
        System.out.println("  Queue Max Length: " + queueMaxLength);
        System.out.println("  Queue Lazy Mode: " + queueLazyMode);
        System.out.println("  Publisher Workers: " + publisherWorkers);
        System.out.println("  Publisher Queue Capacity: " + publisherQueueCapacity);
        System.out.println("  Publisher Confirm Batch: " + publisherConfirmBatch);
        System.out.println("  DynamoDB Enabled: " + enableDynamoDB);
        if (enableDynamoDB) {
            System.out.println("  DynamoDB Batch Size: " + ddbBatchSize);
            System.out.println("  DynamoDB Flush Interval: " + ddbFlushIntervalMs + "ms");
            System.out.println("  DynamoDB Writer Threads: " + ddbWriterThreads);
        }

        // Create RabbitMQ connections
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(rabbitHost);
        factory.setPort(rabbitPort);
        factory.setUsername(rabbitUser);
        factory.setPassword(rabbitPass);
        factory.setRequestedHeartbeat(30);
        factory.setConnectionTimeout(10000);

        Connection publisherConnection = factory.newConnection("PublisherConnection");
        Connection consumerConnection = factory.newConnection("ConsumerConnection");
        System.out.println("RabbitMQ connections established");

        // Initialize components
        ChannelPool channelPool = new ChannelPool(
                rabbitHost, rabbitPort, rabbitUser, rabbitPass,
                channelPoolSize, "chat.exchange"
        );
        QueueStatsTracker queueStatsTracker = new QueueStatsTracker();
        QueueInitializer.initialize(channelPool, ROOM_COUNT, queueMaxLength, queueLazyMode);
        System.out.println("Queues initialized (maxLength=" + queueMaxLength + ", lazyMode=" + queueLazyMode + ")");

        queueMetricsThread = startQueueSampler(channelPool, queueStatsTracker);

        MessagePublisher publisher = new MessagePublisher(
                channelPool, publisherWorkers, publisherQueueCapacity, publisherConfirmBatch);
        System.out.println("Publisher initialized with channel pool");

        RoomManager roomManager = new RoomManager();

        // Start WebSocket server
        ChatEndpoint chatServer = new ChatEndpoint(wsPort, publisher, roomManager, serverId);
        chatServer.start();
        System.out.println("WebSocket server started on port " + wsPort);

        // Start health server with query endpoints
        HealthServer healthServer = new HealthServer(httpPort);
        healthServer.addContext("/queries", new QueryHandler());
        healthServer.addContext("/export-queries", new ExportQueryHandler());

        // Query results endpoints for client to fetch JSON
        QueryResultsHandler queryResultsHandler = new QueryResultsHandler();
        healthServer.addContext("/query/room-messages", queryResultsHandler);
        healthServer.addContext("/query/user-history", queryResultsHandler);
        healthServer.addContext("/query/analytics", queryResultsHandler);
        healthServer.start();
        System.out.println("Health check server started on port " + httpPort);
        System.out.println("Query endpoint available at http://localhost:" + httpPort + "/queries");
        System.out.println("Export endpoint available at http://localhost:" + httpPort + "/export-queries");

        // Start monitoring dashboard
        int monitorPort = getIntProp("MONITOR_PORT", 8083);
        MonitorEndpoint monitor = new MonitorEndpoint(monitorPort, roomManager);
        monitor.start();
        boolean enableDashboardLogging = "true".equals(
                System.getProperty("ENABLE_DASHBOARD_LOGGING", "true")
        );
        MonitorEndpoint.enableLogging(enableDashboardLogging);
        System.out.println("Monitoring dashboard on port " + monitorPort);

        // Initialize DynamoDB batch writer
        if (enableDynamoDB) {
            MessageConsumer.initializeDynamoDb(ddbBatchSize, ddbFlushIntervalMs, ddbWriterThreads);
            System.out.println("DynamoDB batch writer initialized");
        }

        // Start consumer threads
        AtomicLong messagesProcessed = new AtomicLong(0);
        startConsumers(
                consumerConnection,
                roomManager,
                consumerThreadCount,
                prefetchCount,
                messagesProcessed
        );
        System.out.println(consumers.size() + " consumer threads started");

        // Start metrics logger
        Thread metricsThread = startMetricsLogger(roomManager, messagesProcessed, queueStatsTracker);

        System.out.println("=".repeat(60));
        System.out.println("Server ready!");
        System.out.println("=".repeat(60));

        // Graceful shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\n" + "=".repeat(60));
            System.out.println("GRACEFUL SHUTDOWN INITIATED");
            System.out.println("=".repeat(60));

            try {
                // Stop accepting new WebSocket connections
                System.out.println("[1/7] Stopping WebSocket server...");
                chatServer.stop(1000);
                System.out.println("WebSocket server stopped");

                // Stop health check (removes from load balancer)
                System.out.println("[2/7] Stopping health check...");
                healthServer.stop();
                System.out.println("Health check stopped");

                // Wait for load balancer to drain connections
                System.out.println("[3/7] Waiting for connection draining (5s)...");
                Thread.sleep(5000);

                // Stop all consumers
                System.out.println("[4/7] Stopping consumers...");
                consumers.forEach(MessageConsumer::stop);
                System.out.println("Consumer stop signal sent");

                // Wait for in-flight messages to be processed
                System.out.println("[5/7] Waiting for in-flight messages...");
                long shutdownStart = System.currentTimeMillis();
                int shutdownComplete = 0;

                for (MessageConsumer consumer : consumers) {
                    boolean completed = consumer.awaitShutdown(30000);
                    if (completed) {
                        shutdownComplete++;
                    }
                }

                long shutdownDuration = System.currentTimeMillis() - shutdownStart;
                System.out.println("Consumers shutdown: " + shutdownComplete + "/" + consumers.size() +
                        " (took " + shutdownDuration + "ms)");

                // Shutdown DynamoDB writer
                System.out.println("[6/7] Shutting down DynamoDB writer...");
                MessageConsumer.shutdownDynamoDb();
                System.out.println("DynamoDB writer shutdown complete");

                // Close connections
                System.out.println("[7/7] Closing connections...");
                publisher.shutdown();
                channelPool.close();
                publisherConnection.close();
                consumerConnection.close();
                System.out.println("All connections closed");

                // Stop monitoring
                monitor.stop();
                metricsThread.interrupt();
                if (queueMetricsThread != null) {
                    queueMetricsThread.interrupt();
                }

                System.out.println("=".repeat(60));
                System.out.println("SHUTDOWN COMPLETE");
                System.out.println("=".repeat(60));

            } catch (Exception e) {
                System.err.println("Error during shutdown: " + e.getMessage());
                e.printStackTrace();
            }
        }));

        // Keep main thread alive
        Thread.currentThread().join();
    }

    /**
     * Start consumer threads distributed across rooms.
     */
    private static void startConsumers(Connection connection,
                                       RoomManager roomManager,
                                       int numThreads,
                                       int prefetchCount,
                                       AtomicLong messagesProcessed) {
        int consumersPerRoom = Math.max(3, numThreads / ROOM_COUNT);
        final int totalConsumers = ROOM_COUNT * consumersPerRoom;

        System.out.println("Starting consumers: " + consumersPerRoom + " per room");

        IntStream.range(0, totalConsumers).forEach(i -> {
            int roomNum = (i / consumersPerRoom) + 1;
            int consumerNum = i % consumersPerRoom;
            String queueName = "room." + roomNum;

            MessageConsumer consumer = new MessageConsumer(
                    queueName,
                    roomManager,
                    connection,
                    prefetchCount,
                    messagesProcessed
            );

            Thread thread = new Thread(consumer, "Consumer-" + queueName + "-" + consumerNum);
            thread.setDaemon(false);
            thread.start();

            consumers.add(consumer);
            consumerThreads.add(thread);
        });
    }

    /**
     * Start metrics logging thread that prints statistics every 5 seconds.
     */
    private static Thread startMetricsLogger(RoomManager roomManager,
                                             AtomicLong messagesProcessed,
                                             QueueStatsTracker queueStatsTracker) {
        Thread metricsThread = new Thread(() -> {
            long lastReceived = 0;
            long lastPublished = 0;
            long lastConsumed = 0;
            long lastBroadcast = 0;
            long lastDuplicates = 0;
            long lastFailures = 0;
            long lastRetries = 0;

            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(5000);

                    long received = MetricsTracker.getTotalReceived();
                    long published = MessagePublisher.getPublishedCount();
                    long consumed = messagesProcessed.get();
                    long broadcast = roomManager.getMessagesBroadcast();
                    MetricsTracker.MetricsSnapshot snapshot = MetricsTracker.getSnapshot();
                    QueueStatsTracker.QueueSnapshot queueSnapshot = queueStatsTracker.snapshot();

                    // Calculate rates (per second)
                    long recvRate = (received - lastReceived) / 5;
                    long pubRate = (published - lastPublished) / 5;
                    long consumeRate = (consumed - lastConsumed) / 5;
                    long broadcastRate = (broadcast - lastBroadcast) / 5;
                    long duplicateRate = (snapshot.totalDuplicates - lastDuplicates) / 5;
                    long failureRate = (snapshot.totalFailures - lastFailures) / 5;
                    long retryRate = (snapshot.totalRetries - lastRetries) / 5;

                    long consumerLag = Math.max(0L, snapshot.totalPublished - snapshot.totalConsumed);

                    System.out.printf(
                            "[METRICS] Recv: %d (%d/s) | Pub: %d (%d/s) | Consume: %d (%d/s) | Broadcast: %d (%d/s) | " +
                                    "Lag: %d | Dup: %d (+%d/s) | Fail: %d (+%d/s) | Retry: %d (+%d/s) | " +
                                    "Queue: %d (avg: %d, peak: %d) | Conn: %d | Rooms: %d%n",
                            received, recvRate,
                            published, pubRate,
                            consumed, consumeRate,
                            broadcast, broadcastRate,
                            consumerLag,
                            snapshot.totalDuplicates, duplicateRate,
                            snapshot.totalFailures, failureRate,
                            snapshot.totalRetries, retryRate,
                            queueSnapshot.latestTotalDepth,
                            queueSnapshot.averageDepth,
                            queueSnapshot.peakDepth,
                            roomManager.getTotalConnections(),
                            roomManager.getRoomCount()
                    );

                    lastReceived = received;
                    lastPublished = published;
                    lastConsumed = consumed;
                    lastBroadcast = broadcast;
                    lastDuplicates = snapshot.totalDuplicates;
                    lastFailures = snapshot.totalFailures;
                    lastRetries = snapshot.totalRetries;

                } catch (InterruptedException e) {
                    break;
                }
            }
        }, "MetricsLogger");
        metricsThread.setDaemon(true);
        metricsThread.start();
        return metricsThread;
    }

    /**
     * Start background thread that samples RabbitMQ queue depths.
     */
    private static Thread startQueueSampler(ChannelPool channelPool,
                                            QueueStatsTracker tracker) {
        Thread sampler = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                Channel channel = null;
                try {
                    channel = channelPool.borrowChannel();
                    for (int room = 1; room <= ROOM_COUNT; room++) {
                        String queue = "room." + room;
                        AMQP.Queue.DeclareOk ok = channel.queueDeclarePassive(queue);
                        tracker.record(queue, ok.getMessageCount());
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    System.err.println("[QueueSampler] Failed to sample queue depth: " + e.getMessage());
                } finally {
                    if (channel != null) {
                        channelPool.returnChannel(channel);
                    }
                }

                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }, "QueueSampler");
        sampler.setDaemon(true);
        sampler.start();
        return sampler;
    }

    /**
     * Parse integer property with fallback to default.
     */
    private static int getIntProp(String key, int def) {
        try {
            return Integer.parseInt(System.getProperty(key, String.valueOf(def)));
        } catch (Exception e) {
            return def;
        }
    }
}
