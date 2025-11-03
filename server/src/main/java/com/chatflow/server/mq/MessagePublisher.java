package com.chatflow.server.mq;

/**
 * Minimal publisher interface used by the server to publish messages to the message broker.
 */
public interface MessagePublisher {
    void publish(QueueMessage msg) throws Exception;
}

