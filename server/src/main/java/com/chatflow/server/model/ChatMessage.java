package com.chatflow.server.model;

/**
 * Model class representing a chat message.
 * Contains user ID, username, message content, timestamp, and message type.
 */
public record ChatMessage(
        String type,
        String roomId,
        String userId,      
        String username,
        String message,
        Long timestamp,
        String messageType
) {

    public ChatMessage {
    }

    @Override
    public String toString() {
        return "ChatMessage{userId=" + userId + ", username='" + username + "', type=" + messageType + "}";
    }
}
