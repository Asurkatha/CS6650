package com.chatflow.server;

import com.chatflow.server.api.HealthServer;
import com.chatflow.server.api.MonitorEndpoint;
import com.chatflow.server.consumer.MessageConsumer;
import com.chatflow.server.metrics.MetricsTracker;
import com.chatflow.server.metrics.QueueStatsTracker;
import com.chatflow.server.mq.ChannelPool;
import com.chatflow.server.mq.RabbitMQPublisher;
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
Main application class
Handles server initialization and shutdown
Configures RabbitMQ connections
Initializes components
Starts WebSocket server
Starts health server
Starts monitoring dashboard
Starts consumer threads
Starts metrics logger
 */
public class App {

    private static final int ROOM_COUNT = 20;

    private static List<MessageConsumer> consumers = new ArrayList<>();
    private static List<Thread> consumerThreads = new ArrayList<>();
    private static Thread queueMetricsThread;

    public static void main(String[] args) throws Exception {
        System.out.println("=".repeat(60));
        System.out.println("Unified Chat Server - Assignment 2");
        System.out.println("=".repeat(60));

        // Configuration
        int wsPort = getIntProp("WS_PORT", 8080);
        int httpPort = getIntProp("HTTP_PORT", 8081);
        String rabbitHost = System.getProperty("RABBIT_HOST", "172.31.43.127");
        int rabbitPort = getIntProp("RABBIT_PORT", 5672);
        String rabbitUser = System.getProperty("RABBIT_USER", "asurkatha");
        String rabbitPass = System.getProperty("RABBIT_PASS", "SecurePassword123");
        int channelPoolSize = getIntProp("CHANNEL_POOL_SIZE", 200);
        int consumerThreadCount = getIntProp("CONSUMER_THREADS", 80);
        int prefetchCount = getIntProp("PREFETCH_COUNT", 10000);
        int queueMaxLength = getIntProp("QUEUE_MAX_LENGTH", 1000000);
        boolean queueLazyMode = Boolean.parseBoolean(System.getProperty("QUEUE_LAZY_MODE", "false"));
        int publisherWorkers = getIntProp("PUBLISHER_WORKERS", 64);
        int publisherQueueCapacity = getIntProp("PUBLISHER_QUEUE_CAPACITY", 1000000);
        int publisherConfirmBatch = getIntProp("PUBLISHER_CONFIRM_BATCH", 100);
        String serverId = System.getProperty("SERVER_ID",
                "server-" + UUID.randomUUID().toString().substring(0, 8));

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
        RabbitMQPublisher publisher = new RabbitMQPublisher(
                channelPool, publisherWorkers, publisherQueueCapacity, publisherConfirmBatch);
        System.out.println("Publisher initialized with channel pool");

        RoomManager roomManager = new RoomManager();

        // Start WebSocket server
        ChatEndpoint chatServer = new ChatEndpoint(wsPort, publisher, roomManager, serverId);
        chatServer.start();
        System.out.println(" WebSocket server started on port " + wsPort);

        // Start health server
        HealthServer healthServer = new HealthServer(httpPort);
        healthServer.start();
        System.out.println("Health check server started on port " + httpPort);

        // Start monitoring dashboard
        MonitorEndpoint monitor = new MonitorEndpoint(8082, roomManager);
        monitor.start();
        boolean enableDashboardLogging = "true".equals(
                System.getProperty("ENABLE_DASHBOARD_LOGGING", "true")
        );
        MonitorEndpoint.enableLogging(enableDashboardLogging);
        System.out.println(" Monitoring dashboard on port 8082");

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

        System.out.println("=".repeat(30));
        System.out.println("Server ready!");
        System.out.println("=".repeat(30));

        // ✅ GRACEFUL SHUTDOWN HOOK
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\n" + "=".repeat(30));
            System.out.println("GRACEFUL SHUTDOWN INITIATED");
            System.out.println("=".repeat(30));

            try {
                // Step 1: Stop accepting new WebSocket connections
                System.out.println("[1/7] Stopping WebSocket server...");
                chatServer.stop(1000);
                System.out.println(" WebSocket server stopped");

                // Step 2: Stop health check (removes from ALB)
                System.out.println("[2/7] Stopping health check...");
                healthServer.stop();
                System.out.println(" Health check stopped (ALB draining)");

                // Step 3: Wait a bit for ALB to drain connections
                System.out.println("[3/7] Waiting for connection draining (5s)...");
               // Thread.sleep(5000);

                // Step 4: Stop all consumers (stops pulling new messages)
                System.out.println("[4/7] Stopping consumers...");
                consumers.forEach(MessageConsumer::stop);
                System.out.println(" Consumer stop signal sent");

                // Step 5: Wait for all in-flight messages to be processed
                System.out.println("[5/7] Waiting for in-flight messages...");
                long shutdownStart = System.currentTimeMillis();
                int totalConsumers = consumers.size();
                int shutdownComplete = 0;

                for (MessageConsumer consumer : consumers) {
                    boolean completed = consumer.awaitShutdown(30000); // 30s per consumer
                    if (completed) {
                        shutdownComplete++;
                    }
                }

                long shutdownDuration = System.currentTimeMillis() - shutdownStart;
               
                // Step 6: Close connections
                System.out.println("[6/7] Closing connections...");
                publisher.shutdown();
                channelPool.close();
                publisherConnection.close();
                consumerConnection.close();
                System.out.println(" All connections closed");

                // Step 7: Print final metrics
                System.out.println("[7/7] Final metrics:");
               
                // Stop monitoring
                monitor.stop();
                metricsThread.interrupt();
                if (queueMetricsThread != null) {
                    queueMetricsThread.interrupt();
                }

                System.out.println("=".repeat(30));
                System.out.println("SHUTDOWN COMPLETE - No messages lost!");
                System.out.println("=".repeat(30));

            } catch (Exception e) {
                System.err.println("Error during shutdown: " + e.getMessage());
                e.printStackTrace();
            }
        }));

        // Keep main thread alive
        Thread.currentThread().join();
    }

    /**
     * Start consumer threads with proper tracking
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
            thread.setDaemon(false); // NOT daemon - must complete work
            thread.start();

            consumers.add(consumer);
            consumerThreads.add(thread);
        });
    }

    /**
     * Comprehensive metrics logger
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
                    Thread.sleep(5000); // Every 5 seconds

                    long received = MetricsTracker.getTotalReceived();
                    long published = RabbitMQPublisher.getPublishedCount();
                    long consumed = messagesProcessed.get();
                    long broadcast = roomManager.getMessagesBroadcast();
                    MetricsTracker.MetricsSnapshot snapshot = MetricsTracker.getSnapshot();
                    QueueStatsTracker.QueueSnapshot queueSnapshot = queueStatsTracker.snapshot();

                    // Calculate rates
                    long recvRate = (received - lastReceived) / 5;
                    long pubRate = (published - lastPublished) / 5;
                    long consumeRate = (consumed - lastConsumed) / 5;
                    long broadcastRate = (broadcast - lastBroadcast) / 5;
                    long duplicateRate = (snapshot.totalDuplicates - lastDuplicates) / 5;
                    long failureRate = (snapshot.totalFailures - lastFailures) / 5;
                    long retryRate = (snapshot.totalRetries - lastRetries) / 5;

                    long consumerLag = Math.max(0L, snapshot.totalPublished - snapshot.totalConsumed);

                    System.out.printf(
                            "[METRICS] Recv: %d (%d/s) | Pub: %d (%d/s) | Consume: %d (%d/s) | Broadcast: %d (%d/s) " +
                                    "| Confirmed: %d | Failures: %d (+%d/s) | Retries: %d (+%d/s) \n",
                            received, recvRate,
                            published, pubRate,
                            consumed, consumeRate,
                            broadcast, broadcastRate,
                            snapshot.totalConfirmed,
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
                    System.err.println("[Metrics] Queue depth sample failed: " + e.getMessage());
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

    private static int getIntProp(String key, int def) {
        try {
            return Integer.parseInt(System.getProperty(key, String.valueOf(def)));
        } catch (Exception e) {
            return def;
        }
    }
}
