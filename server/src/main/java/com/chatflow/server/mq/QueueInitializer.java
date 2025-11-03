package com.chatflow.server.mq;

import com.chatflow.server.mq.ChannelPool;
import com.rabbitmq.client.Channel;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Ensures the broker queues exist with the desired configuration before we start publishing/consuming.
 */
public final class QueueInitializer {

    private QueueInitializer() {}

    public static void initialize(ChannelPool channelPool,
                                  int roomCount,
                                  int maxQueueLength,
                                  boolean lazyMode) throws IOException {
        Channel channel = borrowChannel(channelPool);
        try {
            channel.exchangeDeclare(channelPool.getExchangeName(), "topic", true);

            Map<String, Object> args = new HashMap<>();
            if (maxQueueLength > 0) {
                args.put("x-max-length", maxQueueLength);
                args.put("x-overflow", "drop-head");
            }
            if (lazyMode) {
                args.put("x-queue-mode", "lazy");
            }

            for (int room = 1; room <= roomCount; room++) {
                String queueName = "room." + room;
                declareQueue(channel, queueName, args);
                channel.queueBind(queueName, channelPool.getExchangeName(), "room." + room);
                try {
                    channel.queuePurge(queueName);
                } catch (IOException purgeError) {
                    System.err.println("[QueueInit] Failed to purge queue " + queueName + ": " + purgeError.getMessage());
                }
            }
        } catch (IOException e) {
            throw e;
        } finally {
            if (channel != null) {
                channelPool.returnChannel(channel);
            }
        }
    }

    private static Channel borrowChannel(ChannelPool pool) throws IOException {
        try {
            Channel channel = pool.borrowChannel();
            if (channel == null) {
                throw new IOException("Failed to borrow channel for queue initialization");
            }
            return channel;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while borrowing channel", e);
        }
    }

    private static void declareQueue(Channel channel,
                                     String queueName,
                                     Map<String, Object> args) throws IOException {
        try {
            channel.queueDeclare(queueName, true, false, false, args);
        } catch (IOException e) {
            if (isPreconditionFailed(e)) {
                System.err.println("[QueueInit] Queue " + queueName +
                        " already exists with different properties. Keeping existing configuration.");
                channel.queueDeclarePassive(queueName);
            } else {
                throw e;
            }
        }
    }

    private static boolean isPreconditionFailed(IOException e) {
        Throwable cause = e.getCause();
        while (cause != null) {
            if (cause instanceof com.rabbitmq.client.ShutdownSignalException shutdown) {
                Object reason = shutdown.getReason();
                if (reason instanceof com.rabbitmq.client.AMQP.Channel.Close close &&
                        close.getReplyCode() == com.rabbitmq.client.AMQP.PRECONDITION_FAILED) {
                    return true;
                }
            }
            cause = cause.getCause();
        }
        return false;
    }
}
