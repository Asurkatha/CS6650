package com.chatflow.server.consumer;

import com.chatflow.server.api.MonitorEndpoint;
import com.chatflow.server.metrics.MetricsTracker;
import com.chatflow.server.mq.QueueMessage;
import com.chatflow.server.ws.RoomManager;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.rabbitmq.client.*;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
MessageConsumer class handles the consumption of messages from a RabbitMQ queue.
It implements the Runnable interface and is used to process messages in a separate thread.
It uses a deduplication cache to handle at-least-once delivery and implements retry logic for failed broadcasts.
It also tracks in-flight messages for graceful shutdown and provides per-message metrics tracking.
 */
public class MessageConsumer implements Runnable {

    private static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();
    private final String queueName;
    private final RoomManager roomManager;
    private final Connection connection;
    private final int prefetchCount;
    private final AtomicLong messagesProcessed;
    private volatile boolean running = true;
    private final CountDownLatch shutdownComplete = new CountDownLatch(1);

    // Deduplication cache
    private static final ConcurrentHashMap<String, Long> processedMessages = new ConcurrentHashMap<>();
    private static final long DEDUP_WINDOW_MS = 15000; // 15 seconds
    private static final int MAX_BROADCAST_RETRIES = 3;

    // Metrics
    private final AtomicLong duplicatesSkipped = new AtomicLong(0);
    private final AtomicLong broadcastRetries = new AtomicLong(0);
    private final AtomicLong processFailures = new AtomicLong(0);

    // Track in-flight messages for graceful shutdown
    private final AtomicLong inFlightMessages = new AtomicLong(0);

    public MessageConsumer(String queueName, RoomManager roomManager,
                           Connection connection, int prefetchCount,
                           AtomicLong messagesProcessed) {
        this.queueName = queueName;
        this.roomManager = roomManager;
        this.connection = connection;
        this.prefetchCount = prefetchCount;
        this.messagesProcessed = messagesProcessed;

        // Start cache cleanup thread
        startDedupCacheCleanup();
    }

    @Override
    public void run() {
        while (running) {
            Channel channel = null;
            String consumerTag = null;
            try {
                channel = connection.createChannel();
                if (channel == null) {
                    System.err.println("[Consumer] Failed to create channel for " + queueName);
                    break;
                }

                channel.basicQos(prefetchCount);
                System.out.println("[Consumer] Started: " + queueName + " (prefetch=" + prefetchCount + ")");

                final Channel finalChannel = channel;
                DeliverCallback deliverCallback = (tag, delivery) -> handleDelivery(finalChannel, delivery);
                CancelCallback cancelCallback = tag -> {
                    if (running) {
                        System.err.println("[Consumer] Broker cancelled consumer " + queueName + " (tag=" + tag + ")");
                    }
                };

                consumerTag = channel.basicConsume(queueName, false, deliverCallback, cancelCallback);

                while (running && channel.isOpen()) {
                    Thread.sleep(200);
                }

                if (!running) {
                    break;
                }

                System.err.println("[Consumer] Channel closed for " + queueName + ", attempting to recover...");
            } catch (Exception e) {
                if (running) {
                    System.err.println("[Consumer] Error on " + queueName + ": " + e.getMessage());
                }
                try {
                    Thread.sleep(500);
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                    break;
                }
            } finally {
                if (channel != null) {
                    try {
                        if (consumerTag != null && channel.isOpen()) {
                            channel.basicCancel(consumerTag);
                        }
                    } catch (Exception ignore) {}
                    try {
                        channel.close();
                    } catch (Exception ignore) {}
                }
            }
        }

        System.out.println("[Consumer] Shutting down: " + queueName);
        waitForInflightDrain();
        shutdownComplete.countDown();
    }

    private void handleDelivery(Channel channel, Delivery delivery) {
        inFlightMessages.incrementAndGet();
        long startTime = System.nanoTime();

        try {
            String messageBody = new String(delivery.getBody(), StandardCharsets.UTF_8);
            QueueMessage msg = GSON.fromJson(messageBody, QueueMessage.class);

            if (isDuplicate(msg.messageId())) {
                duplicatesSkipped.incrementAndGet();
                MetricsTracker.recordDuplicate(msg.roomId());
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                return;
            }

            MonitorEndpoint.logMessage(msg, "BROADCAST");

            int recipientCount = broadcastWithRetry(msg, MAX_BROADCAST_RETRIES);
            MetricsTracker.recordConsume(msg.roomId());
            if (recipientCount >= 0) {
                markAsProcessed(msg.messageId());
                messagesProcessed.incrementAndGet();
                MetricsTracker.recordBroadcast(msg.roomId(), recipientCount);
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);

                long duration = (System.nanoTime() - startTime) / 1_000_000;
            } else {
                processFailures.incrementAndGet();
                MetricsTracker.recordFailure(msg.roomId());
                channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, false);
                System.err.println("[Consumer] Failed all retries: " + msg.messageId());
            }
        } catch (Exception e) {
            processFailures.incrementAndGet();
            try {
                channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, false);
            } catch (IOException ex) {
                System.err.println("[Consumer] NACK failed: " + ex.getMessage());
            }
            System.err.println("[Consumer] Error: " + e.getMessage());
        } finally {
            inFlightMessages.decrementAndGet();
        }
    }

    private void waitForInflightDrain() {
        long shutdownStart = System.currentTimeMillis();
        while (inFlightMessages.get() > 0) {
            if (System.currentTimeMillis() - shutdownStart > 30000) {
                System.err.println("[Consumer] Shutdown timeout - " +
                        inFlightMessages.get() + " messages still in-flight");
                break;
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    /**
     * Broadcast with exponential backoff retry
     */
    private int broadcastWithRetry(QueueMessage msg, int maxRetries) {
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                return roomManager.broadcastToRoom(msg);
            } catch (Exception e) {
                if (attempt < maxRetries) {
                    broadcastRetries.incrementAndGet();
                    MetricsTracker.recordRetry(msg.roomId());
                    try {
                        Thread.sleep(50L * attempt); // 50ms, 100ms, 150ms
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return -1;
                    }
                }
            }
        }
        return -1;
    }

    /**
     * Check if message was already processed (deduplication)
     */
    private boolean isDuplicate(String messageId) {
        Long timestamp = processedMessages.get(messageId);
        if (timestamp != null) {
            return (System.currentTimeMillis() - timestamp) < DEDUP_WINDOW_MS;
        }
        return false;
    }

    /**
     * Mark message as processed for deduplication
     */
    private void markAsProcessed(String messageId) {
        processedMessages.put(messageId, System.currentTimeMillis());
    }

    /**
     * Cleanup old entries from dedup cache
     */
    private void startDedupCacheCleanup() {
        Thread cleanupThread = new Thread(() -> {
            while (running) {
                try {
                    Thread.sleep(1000); // Every second
                    long now = System.currentTimeMillis();
                    processedMessages.entrySet().removeIf(entry ->
                            (now - entry.getValue()) > DEDUP_WINDOW_MS
                    );
                } catch (InterruptedException e) {
                    break;
                }
            }
        }, "DedupCleanup-" + queueName);
        cleanupThread.setDaemon(true);
        cleanupThread.start();
    }

    /**
     * Graceful shutdown - waits for in-flight messages
     */
    public void stop() {
        running = false;
    }


    public boolean awaitShutdown(long timeoutMs) throws InterruptedException {
        return shutdownComplete.await(timeoutMs, TimeUnit.MILLISECONDS);
    }

    // Metrics getters
    public long getDuplicatesSkipped() { return duplicatesSkipped.get(); }
    public long getBroadcastRetries() { return broadcastRetries.get(); }
    public long getProcessFailures() { return processFailures.get(); }
    public long getInFlightCount() { return inFlightMessages.get(); }
}
