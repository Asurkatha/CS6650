package com.chatflow.server.model;

/**
 * Model class representing a chat message.
 * Contains user ID, username, message content, timestamp, and message type.
 */
public class ChatMessage {
    // Required by spec
    public Integer userId;          // 1..100000
    public String username;         // 3..20 alnum
    public String message;          // 1..500
    public String timestamp;        // ISO-8601
    public String messageType;      // TEXT|JOIN|LEAVE

    // no-args constructor for Gson
    public ChatMessage(Integer userId, String username, String message, String timestamp, String messageType) {}

    @Override
    public String toString() {
        return "ChatMessage{userId=" + userId + ", username='" + username + "', type=" + messageType + "}";
    }
}
