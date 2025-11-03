package com.chatflow.server.model;


/**
 * Enum representing the type of chat message.
 * Valid types are TEXT, JOIN, and LEAVE.
 */
public enum MessageType {
    TEXT, JOIN, LEAVE;

    public static boolean isValid(String t) {
        if (t == null) return false;
        try {
            MessageType.valueOf(t);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }
}
