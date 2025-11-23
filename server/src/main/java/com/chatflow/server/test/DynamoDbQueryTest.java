package com.chatflow.server.test;

import com.chatflow.server.storage.DynamoDbQueryService;
import com.chatflow.server.storage.DynamoDbQueryService.ChatMessage;
import com.chatflow.server.storage.DynamoDbQueryService.UserRoomInfo;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

/**
 * Standalone test program to verify DynamoDB queries.
 *
 * To run on EC2:
 * 1. Build: mvn clean package
 * 2. Upload JAR to EC2
 * 3. Run: java -cp chatflow-server.jar com.chatflow.server.test.DynamoDbQueryTest
 *
 * Prerequisites:
 * - DynamoDB tables created
 * - IAM role attached to EC2 instance
 * - Some test data in tables (send messages using client)
 */
public class DynamoDbQueryTest {

    public static void main(String[] args) {
        System.out.println("=".repeat(60));
        System.out.println("DynamoDB Query Tests");
        System.out.println("=".repeat(60));
        System.out.println();

        DynamoDbQueryService queryService = new DynamoDbQueryService();

        // Test 1: Get messages for room 1 (last 24 hours)
        testRoomMessages(queryService);

        // Test 2: Get user message history
        testUserHistory(queryService);

        // Test 3: Count active users
        testActiveUsers(queryService);

        // Test 4: Get user rooms
        testUserRooms(queryService);

        System.out.println();
        System.out.println("=".repeat(60));
        System.out.println("All Tests Complete!");
        System.out.println("=".repeat(60));
    }

    private static void testRoomMessages(DynamoDbQueryService queryService) {
        System.out.println("[Test 1] Get Messages for Room");
        System.out.println("-".repeat(60));

        try {
            // Use a test ID - you should replace this with an actual testId from your test run
            String testId = "test-2025-01-15T00:00:00Z";  // Replace with actual testId

            // Test multiple rooms
            for (int roomId = 1; roomId <= 5; roomId++) {
                List<ChatMessage> messages = queryService.getMessagesForRoom(
                    String.valueOf(roomId),
                    testId,
                    10
                );

                System.out.printf("  Room %d: %d messages found\n", roomId, messages.size());

                if (!messages.isEmpty()) {
                    ChatMessage sample = messages.get(0);
                    System.out.printf("    Sample: [%s] %s: %s\n",
                        sample.messageType(),
                        sample.username(),
                        sample.message().substring(0, Math.min(30, sample.message().length()))
                    );
                }
            }

            System.out.println("  ✓ Test passed");

        } catch (Exception e) {
            System.out.println("  ✗ Test failed: " + e.getMessage());
            e.printStackTrace();
        }

        System.out.println();
    }

    private static void testUserHistory(DynamoDbQueryService queryService) {
        System.out.println("[Test 2] Get User Message History");
        System.out.println("-".repeat(60));

        try {
            // Use a test ID - you should replace this with an actual testId from your test run
            String testId = "test-2025-01-15T00:00:00Z";  // Replace with actual testId

            // Get some messages first to find a userId
            List<ChatMessage> roomMessages = queryService.getMessagesForRoom("1", testId, 1);

            if (roomMessages.isEmpty()) {
                System.out.println("  ⚠ No messages found. Send some test messages first!");
                System.out.println();
                return;
            }

            String testUserId = roomMessages.get(0).userId();

            // Get all messages from user for this testId
            List<ChatMessage> allUserMessages = queryService.getUserMessageHistory(
                testUserId, testId, 100
            );
            System.out.printf("  User %s total messages: %d\n", testUserId, allUserMessages.size());

            // Show sample messages
            if (!allUserMessages.isEmpty()) {
                System.out.println("  Sample messages:");
                for (int i = 0; i < Math.min(3, allUserMessages.size()); i++) {
                    ChatMessage msg = allUserMessages.get(i);
                    System.out.printf("    [Room %s] %s\n",
                        msg.roomId(),
                        msg.message().substring(0, Math.min(40, msg.message().length()))
                    );
                }
            }

            System.out.println("  ✓ Test passed");

        } catch (Exception e) {
            System.out.println("  ✗ Test failed: " + e.getMessage());
            e.printStackTrace();
        }

        System.out.println();
    }

    private static void testActiveUsers(DynamoDbQueryService queryService) {
        System.out.println("[Test 3] Count Active Users");
        System.out.println("-".repeat(60));

        try {
            // Use a test ID - you should replace this with an actual testId from your test run
            String testId = "test-2025-01-15T00:00:00Z";  // Replace with actual testId

            int activeUsers = queryService.countActiveUsers(testId);
            System.out.printf("  Active users (testId=%s): %d\n", testId, activeUsers);

            if (activeUsers == 0) {
                System.out.println("  ⚠ No active users found. Send some test messages!");
            } else {
                System.out.println("  ✓ Test passed");
            }

        } catch (Exception e) {
            System.out.println("  ✗ Test failed: " + e.getMessage());
            e.printStackTrace();
        }

        System.out.println();
    }

    private static void testUserRooms(DynamoDbQueryService queryService) {
        System.out.println("[Test 4] Get User Rooms");
        System.out.println("-".repeat(60));

        try {
            // Use a test ID - you should replace this with an actual testId from your test run
            String testId = "test-2025-01-15T00:00:00Z";  // Replace with actual testId

            // Get some messages first to find a userId
            List<ChatMessage> roomMessages = queryService.getMessagesForRoom("1", testId, 1);

            if (roomMessages.isEmpty()) {
                System.out.println("  ⚠ No messages found. Send some test messages first!");
                System.out.println();
                return;
            }

            String testUserId = roomMessages.get(0).userId();

            List<UserRoomInfo> rooms = queryService.getUserRooms(testUserId, testId);

            System.out.printf("  User %s participated in %d rooms:\n", testUserId, rooms.size());

            for (UserRoomInfo room : rooms) {
                System.out.printf("    Room %-3s: %4d messages, last active: %s\n",
                    room.roomId(),
                    room.messageCount(),
                    Instant.ofEpochMilli(room.lastActivityTimestamp()).toString()
                );
            }

            if (rooms.isEmpty()) {
                System.out.println("  ⚠ No room participation found");
            } else {
                System.out.println("  ✓ Test passed");
            }

        } catch (Exception e) {
            System.out.println("  ✗ Test failed: " + e.getMessage());
            e.printStackTrace();
        }

        System.out.println();
    }
}
