package com.chatflow.server.mq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Thread-safe pool for RabbitMQ channels.
 * Manages a single connection and pools channels for reuse.
 */
public class ChannelPool {
    private final BlockingQueue<Channel> pool;
    private final Connection connection;
    private final int maxChannels;
    private final String exchangeName;
    private volatile boolean closed = false;

    /**
     * Creates a channel pool.
     *
     * @param rabbitHost RabbitMQ host
     * @param rabbitPort RabbitMQ port
     * @param username RabbitMQ username
     * @param password RabbitMQ password
     * @param poolSize Number of channels to pre-create
     * @param exchangeName Name of the topic exchange
     */
    public ChannelPool(String rabbitHost, int rabbitPort, String username,
                       String password, int poolSize, String exchangeName) throws IOException, TimeoutException {
        this.maxChannels = poolSize;
        this.exchangeName = exchangeName;
        this.pool = new LinkedBlockingQueue<>(poolSize);

        // Create connection
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(rabbitHost);
        factory.setPort(rabbitPort);
        factory.setUsername(username);
        factory.setPassword(password);

        // Connection tuning for high throughput
        factory.setRequestedHeartbeat(30);
        factory.setConnectionTimeout(10000);
        factory.setRequestedChannelMax(poolSize);

        this.connection = factory.newConnection();
        System.out.println("RabbitMQ connection established to " + rabbitHost + ":" + rabbitPort);

        // Declare exchange once
        try (Channel setupChannel = connection.createChannel()) {
            setupChannel.exchangeDeclare(exchangeName, "topic", true);
            System.out.println("Exchange declared: " + exchangeName);
        }

        // Pre-create channels
        for (int i = 0; i < poolSize; i++) {
            pool.offer(createNewChannel());
        }
        System.out.println("Channel pool initialized with " + poolSize + " channels");
    }

    private Channel createNewChannel() throws IOException {
        Channel channel = connection.createChannel();
        // Enable publisher confirms for reliability
        channel.confirmSelect();
        return channel;
    }

    /**
     * Borrow a channel from the pool.
     * Blocks if no channels are available.
     */
    public Channel borrowChannel() throws InterruptedException, IOException {
        if (closed) {
            throw new IllegalStateException("Channel pool is closed");
        }

        Channel channel = pool.poll(5, TimeUnit.SECONDS);
        if (channel == null) {
            throw new IOException("Failed to obtain channel from pool - timeout");
        }

        // Verify channel is still open
        if (!channel.isOpen()) {
            System.err.println("Channel was closed, creating new one");
            channel = createNewChannel();
        }

        return channel;
    }

    /**
     * Return a channel to the pool.
     */
    public void returnChannel(Channel channel) {
        if (channel != null && channel.isOpen() && !closed) {
            boolean returned = pool.offer(channel);
            if (!returned) {
                // Pool is full, close the channel
                try {
                    channel.close();
                } catch (Exception e) {
                    System.err.println("Error closing excess channel: " + e.getMessage());
                }
            }
        } else if (channel != null && !channel.isOpen()) {
            // Channel was closed, try to create a new one to maintain pool size
            try {
                pool.offer(createNewChannel());
            } catch (IOException e) {
                System.err.println("Failed to create replacement channel: " + e.getMessage());
            }
        }
    }

    /**
     * Close all channels and the connection.
     */
    public void close() {
        closed = true;

        // Close all channels in pool
        Channel channel;
        while ((channel = pool.poll()) != null) {
            try {
                if (channel.isOpen()) {
                    channel.close();
                }
            } catch (Exception e) {
                System.err.println("Error closing channel: " + e.getMessage());
            }
        }

        // Close connection
        try {
            if (connection.isOpen()) {
                connection.close();
            }
            System.out.println("Channel pool and connection closed");
        } catch (IOException e) {
            System.err.println("Error closing connection: " + e.getMessage());
        }
    }

    public Connection getConnection() {
        return connection;
    }

    public String getExchangeName() {
        return exchangeName;
    }

    public boolean isClosed() {
        return closed;
    }

    public Channel getChannel() {
        try {
            return borrowChannel();
        } catch (Exception e) {
            System.err.println("Error borrowing channel: " + e.getMessage());
            return null;
        }
    }
}
