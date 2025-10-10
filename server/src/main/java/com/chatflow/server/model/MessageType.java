package com.chatflow.server.model;


/**
 * Enum representing the type of chat message.
 * Valid types are TEXT, JOIN, and LEAVE.
 */
public enum MessageType {
    TEXT, JOIN, LEAVE;

    public static boolean isValid(String s) {
        for (MessageType t : values()) if (t.name().equals(s)) return true;
        return false;
    }
}
