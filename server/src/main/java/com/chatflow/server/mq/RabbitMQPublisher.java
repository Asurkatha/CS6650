package com.chatflow.server.mq;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.chatflow.server.metrics.MetricsTracker;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.MessageProperties;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Asynchronous RabbitMQ publisher with publisher-confirms.
 *
 * Moves the blocking publish + confirm sequence off the WebSocket IO thread and
 * maintains a pool of worker threads that share the channel pool. Each worker
 * takes messages from a bounded queue, publishes them, waits for confirms, and
 * updates metrics.
 */
public class RabbitMQPublisher implements MessagePublisher {

    private static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();
    private static final int MAX_RETRIES = 3;
    private static final long CONFIRM_TIMEOUT_MS = 5000;

    private final ChannelPool channelPool;
    private final String exchangeName;
    private final BlockingQueue<QueueMessage> outboundQueue;
    private final ExecutorService workerPool;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final int confirmBatchSize;

    // Metrics tracking (lock-free for performance)
    private static final AtomicLong publishedCount = new AtomicLong(0);
    private static final AtomicLong confirmedCount = new AtomicLong(0);
    private static final AtomicLong failedCount = new AtomicLong(0);

    public RabbitMQPublisher(ChannelPool channelPool) {
        this(channelPool,
                Runtime.getRuntime().availableProcessors() * 2,
                200_000,
                Integer.getInteger("PUBLISHER_CONFIRM_BATCH", 20));
    }

    public RabbitMQPublisher(ChannelPool channelPool, int workerCount,
                             int queueCapacity, int confirmBatchSize) {
        this.channelPool = channelPool;
        this.exchangeName = channelPool.getExchangeName();
        this.outboundQueue = new LinkedBlockingQueue<>(queueCapacity);
        this.workerPool = createWorkerPool(workerCount);
        this.confirmBatchSize = Math.max(1, confirmBatchSize);
        startWorkers(workerCount);
    }

    @Override
    public void publish(QueueMessage msg) throws IOException {
        if (msg == null) {
            throw new IllegalArgumentException("Message cannot be null");
        }

        try {
            outboundQueue.put(msg);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while enqueueing message for publish", e);
        }
    }

    private ExecutorService createWorkerPool(int workerCount) {
        ThreadFactory factory = new ThreadFactory() {
            private final AtomicInteger seq = new AtomicInteger(1);
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "PublisherWorker-" + seq.getAndIncrement());
                t.setDaemon(true);
                return t;
            }
        };
        return Executors.newFixedThreadPool(workerCount, factory);
    }

    private void startWorkers(int workerCount) {
        for (int i = 0; i < workerCount; i++) {
            workerPool.submit(this::workerLoop);
        }
    }

    public void shutdown() {
        running.set(false);
        workerPool.shutdownNow();
    }

    public static long getPublishedCount() { return publishedCount.get(); }
    public static long getConfirmedCount() { return confirmedCount.get(); }
    public static long getFailedCount() { return failedCount.get(); }

    public static void resetMetrics() {
        publishedCount.set(0);
        confirmedCount.set(0);
        failedCount.set(0);
    }

    private void workerLoop() {
        List<QueueMessage> batch = new ArrayList<>(confirmBatchSize);
        Channel channel = null;
        try {
            while (running.get() && !Thread.currentThread().isInterrupted()) {
                try {
                    if (channel == null || !channel.isOpen()) {
                        channel = channelPool.borrowChannel();
                    }

                    QueueMessage msg = outboundQueue.poll(200, TimeUnit.MILLISECONDS);
                    if (msg != null) {
                        publishWithoutConfirm(channel, msg);
                        batch.add(msg);
                        if (batch.size() >= confirmBatchSize) {
                            flushBatch(channel, batch);
                        }
                        continue;
                    }

                    flushBatch(channel, batch);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    failedCount.addAndGet(batch.size());
                    requeueBatch(batch);
                    batch.clear();
                    safeClose(channel);
                    channel = null;
                    System.err.println("[Publisher] Worker recovering from error: " + e.getMessage());
                }
            }
        } finally {
            try {
                flushBatch(channel, batch);
            } catch (Exception e) {
                failedCount.addAndGet(batch.size());
                requeueBatch(batch);
                safeClose(channel);
                channel = null;
            }

            if (channel != null && channel.isOpen()) {
                channelPool.returnChannel(channel);
            }
        }
    }

    private void publishWithoutConfirm(Channel channel, QueueMessage msg) throws IOException {
        String json = GSON.toJson(msg);
        channel.basicPublish(
                exchangeName,
                "room." + msg.roomId(),
                MessageProperties.PERSISTENT_TEXT_PLAIN,
                json.getBytes(StandardCharsets.UTF_8)
        );
        publishedCount.incrementAndGet();
        MetricsTracker.recordPublish(msg.roomId());
    }

    private void flushBatch(Channel channel, List<QueueMessage> batch) throws IOException, InterruptedException {
        if (batch.isEmpty()) {
            return;
        }

        try {
            boolean confirmed = channel.waitForConfirms(CONFIRM_TIMEOUT_MS);
            if (confirmed) {
                confirmedCount.addAndGet(batch.size());
                for (QueueMessage msg : batch) {
                    MetricsTracker.recordConfirm(msg.roomId());
                }
                batch.clear();
                return;
            }
            throw new IOException("Broker NACKed batch");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw e;
        } catch (IOException | TimeoutException e) {
            failedCount.addAndGet(batch.size());
            requeueBatch(batch);
            batch.clear();
            throw (e instanceof IOException) ? (IOException) e : new IOException(e);
        }
    }

    private void requeueBatch(List<QueueMessage> batch) {
        for (QueueMessage msg : batch) {
            if (msg == null) continue;
            MetricsTracker.recordRetry(msg.roomId());
            MetricsTracker.recordFailure(msg.roomId());
            try {
                outboundQueue.put(msg);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        batch.clear();
    }

    private void safeClose(Channel channel) {
        if (channel != null) {
            try {
                channel.close();
            } catch (Exception ignore) {
            }
        }
    }
}
