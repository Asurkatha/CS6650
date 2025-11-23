package com.chatflow.client.part1;

import com.google.gson.JsonObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/**
 * Generates realistic user session messages
 *
 * Simulates real user behavior:
 * - Each user performs: JOIN → multiple TEXT messages → LEAVE
 * - Same userId stays in same room for their entire session
 * - Multiple users can be in same room simultaneously
 * - userName matches userId for consistency
 */

class MessageGenerator {

    private final Random rnd = new Random();
    private final String testId;

    public MessageGenerator(String testId) {
        this.testId = testId;
    }

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
     * Generate messages simulating realistic user sessions
     * Each user: JOIN → N text messages → LEAVE
     *
     * @param totalMessages Total messages to generate
     * @return List of JSON message strings
     */
    public List<String> generateMessages(int totalMessages) {
        List<String> messages = new ArrayList<>();
        if (totalMessages <= 0) {
            return messages;
        }

        // Calculate number of complete user sessions we can fit
        // Each session: 1 JOIN + at least 1 TEXT + 1 LEAVE = minimum 3 messages
        int minMessagesPerSession = 3;
        int maxSessions = totalMessages / minMessagesPerSession;

        // Distribute messages across sessions (some users will send more texts than others)
        int sessionsToCreate = Math.max(1, Math.min(maxSessions, totalMessages / 5)); // avg 5 msgs per user
        int messagesPerSession = totalMessages / sessionsToCreate;

        for (int sessionNum = 0; sessionNum < sessionsToCreate && messages.size() < totalMessages; sessionNum++) {
            String userId = "user" + sessionNum;
            String username = "user" + sessionNum;
            int roomId = (sessionNum % ROOM_COUNT) + 1; // Distribute users across rooms

            // Calculate remaining space before adding this session
            int remainingSpace = totalMessages - messages.size();
            if (remainingSpace <= 0) break;

            // 1. JOIN message
            if (messages.size() < totalMessages) {
                messages.add(createMessage(roomId, userId, username, "JOIN", "").toString());
            }

            // 2. TEXT messages (variable count per user)
            int textMsgCount = messagesPerSession - 2; // Reserve space for JOIN and LEAVE
            if (textMsgCount < 1) textMsgCount = 1;

            // Add some randomness to message count per user (between 50% and 150% of average)
            int variation = (int) (textMsgCount * 0.5);
            textMsgCount = textMsgCount - variation / 2 + rnd.nextInt(Math.max(1, variation));

            // Ensure we don't exceed totalMessages
            int maxTexts = totalMessages - messages.size() - 1; // Reserve 1 for LEAVE
            textMsgCount = Math.min(textMsgCount, Math.max(0, maxTexts));

            for (int i = 0; i < textMsgCount && messages.size() < totalMessages - 1; i++) {
                messages.add(createMessage(roomId, userId, username, "TEXT", getRandomMessage()).toString());
            }

            // 3. LEAVE message (only if we have space)
            if (messages.size() < totalMessages) {
                messages.add(createMessage(roomId, userId, username, "LEAVE", "").toString());
            }
        }

        // STRICT: Only add messages if we're below totalMessages, never exceed
        if (messages.size() < totalMessages && sessionsToCreate > 0) {
            while (messages.size() < totalMessages) {
                int userNum = rnd.nextInt(sessionsToCreate);
                String userId = "user" + userNum;
                String username = "user" + userNum;
                int roomId = (userNum % ROOM_COUNT) + 1;
                messages.add(createMessage(roomId, userId, username, "TEXT", getRandomMessage()).toString());
            }
        }

        // CRITICAL: Trim to exact count if we somehow exceeded (should not happen now)
        if (messages.size() > totalMessages) {
            messages = new ArrayList<>(messages.subList(0, totalMessages));
        }

        return messages;
    }

    private JsonObject createMessage(int roomId, String userId, String username, String messageType, String text) {
        JsonObject msg = new JsonObject();

        msg.addProperty("messageId", UUID.randomUUID().toString());
        msg.addProperty("roomId", String.valueOf(roomId));
        msg.addProperty("userId", userId);
        msg.addProperty("username", username);
        msg.addProperty("message", text);
        msg.addProperty("messageType", messageType);
        msg.addProperty("timestamp", System.currentTimeMillis());
        msg.addProperty("testId", testId);  // Add testId to every message

        return msg;
    }

    private String getRandomMessage() {
        return POOL[rnd.nextInt(POOL.length)];
    }
}
