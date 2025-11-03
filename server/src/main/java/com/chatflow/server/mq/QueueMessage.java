package com.chatflow.server.mq;

/**
 * Message sent from server to the message broker (and consumed by consumer).
 * Fields are public via record accessors: messageId(), roomId(), userId(), username(), message(), timestamp(), messageType(), serverId(), origin()
 */
public record QueueMessage(
        String messageId,
        String roomId,
        String userId,
        String username,
        String message,
        String timestamp,
        String messageType,
        String serverId,
        String clientIp
) {
}
