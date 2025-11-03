package com.chatflow.client.part1;

import com.google.gson.JsonObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/**
 * Generates valid messages for all types
 *
 * Per Assignment 2 requirements:
 * - Generates JOIN, TEXT, LEAVE messages for each room
 * - username must be 3-20 ALPHANUMERIC characters (NO underscores, NO special chars)
 * - roomId: 1-20
 * - userId: unique per message
 * - messageType: TEXT, JOIN, LEAVE
 * - timestamp: milliseconds since epoch
 */

class MessageGenerator {

    private final Random rnd = new Random();

    private static final String[] POOL = {
            "Hello", "How are you?", "Nice to meet you",
            "Testing", "Ping", "Pong", "What's up?", "Good day",
            "All set", "Ready", "Acknowledged", "Confirming", "Thanks",
            "Cheers", "Let's go", "Checking in", "Quick note", "FYI", "Update",
            "Done", "Hello room", "Streaming", "Latency test", "Throughput",
            "Benchmark", "Warmup", "Client msg", "Server echo", "Random text",
            "Lorem ipsum", "Edge case", "Retrying", "Backoff", "Reconnect",
            "Stable", "OK", "Fine", "Yo", "Hey", "Hi", "Hola", "Bonjour",
            "Namaste", "Konnichiwa", "Annyeong", "Guten Tag", "Ciao", "Hej"
    };

    private static final int ROOM_COUNT = 20;

    /**
     * Generate messages for all rooms
     * Returns JSON strings with JOIN, TEXT, LEAVE messages
     */
    public List<String> generateMessages(int totalMessages) {
        List<String> messages = new ArrayList<>();
        if (totalMessages <= 0) {
            return messages;
        }
        for (int room = 1; room <= ROOM_COUNT && messages.size() < totalMessages; room++) {
            JsonObject joinMsg = createMessage(room, "JOIN", "");
            messages.add(joinMsg.toString());
        }
        int remaining = totalMessages - messages.size();
        if (remaining <= 0) {
            return messages;
        }
        int leavesPossible = Math.min(ROOM_COUNT, remaining);
        int textSlots = remaining - leavesPossible;
        if (textSlots < 0) {
            textSlots = 0;
            leavesPossible = remaining;
        }
        for (int i = 0; i < textSlots && messages.size() < totalMessages; i++) {
            int room = (i % ROOM_COUNT) + 1;
            JsonObject textMsg = createMessage(room, "TEXT", getRandomMessage());
            messages.add(textMsg.toString());
        }
        remaining = totalMessages - messages.size();
        leavesPossible = Math.min(ROOM_COUNT, remaining);
        for (int room = 1; room <= ROOM_COUNT && leavesPossible > 0 && messages.size() < totalMessages; room++) {
            JsonObject leaveMsg = createMessage(room, "LEAVE", "");
            messages.add(leaveMsg.toString());
            leavesPossible--;
        }
        while (messages.size() < totalMessages) {
            int room = (messages.size() % ROOM_COUNT) + 1;
            JsonObject textMsg = createMessage(room, "TEXT", getRandomMessage());
            messages.add(textMsg.toString());
        }
        return messages;
    }

    private JsonObject createMessage(int room, String messageType, String text) {
        JsonObject msg = new JsonObject();

        // Generate unique userId (just a number string)
        int uniqueId = rnd.nextInt(100000);

        msg.addProperty("messageId", UUID.randomUUID().toString());
        msg.addProperty("roomId", String.valueOf(room));
        msg.addProperty("userId", "user" + uniqueId);  
        msg.addProperty("username", "user" + uniqueId);  
        msg.addProperty("message", text);
        msg.addProperty("messageType", messageType);
        msg.addProperty("timestamp", System.currentTimeMillis());

        return msg;
    }

    private String getRandomMessage() {
        return POOL[rnd.nextInt(POOL.length)];
    }
}
