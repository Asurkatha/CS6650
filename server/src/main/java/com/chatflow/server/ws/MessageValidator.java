package com.chatflow.server.ws;

import com.chatflow.server.model.ChatMessage;
import com.chatflow.server.model.MessageType;

import java.time.Instant;

/**
 * Validates incoming chat messages to ensure they meet required criteria.
 * Checks for null values, valid ranges, formats, and allowed types.
 */
final class MessageValidator {

    static String validate(ChatMessage m) {
        if (m == null) return "Body is null";
        if (m.userId == null || m.userId < 1 || m.userId > 100000) return "userId must be 1..100000";
        if (m.username == null || !m.username.matches("^[A-Za-z0-9]{3,20}$")) return "username must be 3-20 alphanumeric";
        if (m.message == null || m.message.length() < 1 || m.message.length() > 500) return "message must be 1..500 chars";
        if (m.timestamp == null) return "timestamp missing";
        try { Instant.parse(m.timestamp); } catch (Exception e) { return "timestamp must be ISO-8601"; }
        if (m.messageType == null || !MessageType.isValid(m.messageType)) return "messageType must be TEXT|JOIN|LEAVE";
        return null; // OK
    }

    private MessageValidator() {}
}
