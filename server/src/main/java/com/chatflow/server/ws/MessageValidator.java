package com.chatflow.server.ws;

import com.google.gson.JsonObject;

/**
 * Validates incoming messages against requirements.
 *
 * Requirements:
 * - username: 3-20 alphanumeric characters [A-Za-z0-9]
 * - roomId: required, non-empty
 * - userId: required, non-empty
 * - messageType: TEXT, JOIN, or LEAVE
 * - timestamp: required, valid long
 * - message: required for TEXT, optional for JOIN/LEAVE
 */
final class MessageValidator {

    /**
     * Validates message format and content.
     * @return "clear" if valid, error message if invalid
     */
    static String validate(JsonObject msg) {
        if (msg == null) {
            return "Message is null";
        }

        if (!msg.has("roomId") || msg.get("roomId").getAsString().trim().isEmpty()) {
            return "roomId is required";
        }

        if (!msg.has("userId") || msg.get("userId").getAsString().trim().isEmpty()) {
            return "userId is required";
        }

        if (!msg.has("username")) {
            return "username is required";
        }

        String username = msg.get("username").getAsString();
        if (username == null || username.isEmpty()) {
            return "username cannot be empty";
        }

        if (!username.matches("^[A-Za-z0-9]{3,20}$")) {
            return "username must be 3-20 alphanumeric characters (letters and numbers only)";
        }

        if (!msg.has("messageType") || msg.get("messageType").getAsString().trim().isEmpty()) {
            return "messageType is required";
        }

        String messageType = msg.get("messageType").getAsString().toUpperCase();
        if (messageType.equals("CONTROL")) {
            if (!msg.has("control")) {
                return "control command is required for CONTROL messages";
            }
            if (!msg.has("sessionId")) {
                return "sessionId is required for CONTROL messages";
            }
            return "clear";
        }

        if (!messageType.equals("TEXT") && !messageType.equals("JOIN") && !messageType.equals("LEAVE")) {
            return "messageType must be TEXT, JOIN, or LEAVE";
        }

        if (messageType.equals("TEXT")) {
            if (!msg.has("message") || msg.get("message").getAsString().trim().isEmpty()) {
                return "message content is required for TEXT messageType";
            }

            String messageContent = msg.get("message").getAsString();
            if (messageContent.length() > 500) {
                return "message cannot exceed 500 characters";
            }
        }

        if (!msg.has("timestamp")) {
            return "timestamp is required";
        }

        try {
            long ts = msg.get("timestamp").getAsLong();
            if (ts <= 0) {
                return "timestamp must be a positive number";
            }
        } catch (Exception e) {
            return "timestamp must be a valid number (milliseconds)";
        }

        return "clear";
    }

    private MessageValidator() {
    }
}
