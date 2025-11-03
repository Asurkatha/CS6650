package com.chatflow.server.ws;

import com.chatflow.server.model.ChatMessage;
import com.chatflow.server.metrics.MetricsTracker;
import com.chatflow.server.mq.MessagePublisher;
import com.chatflow.server.mq.QueueMessage;
import com.chatflow.server.ws.MessageValidator;
import com.google.gson.*;
import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
ChatEndpoint - Handles WebSocket connections and message processing
All messages are validated and published to RabbitMQ
Implements error handling and connection management
 */
public class ChatEndpoint extends WebSocketServer {

    private static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();

    private final MessagePublisher publisher;
    private final RoomManager roomManager;
    private final String serverId;
    private final Map<WebSocket, String> connectionRooms = new ConcurrentHashMap<>();

    public ChatEndpoint(int port, MessagePublisher publisher, RoomManager roomManager, String serverId) {
        super(new InetSocketAddress(port));
        this.publisher = publisher;
        this.roomManager = roomManager;
        this.serverId = serverId;
    }

    @Override
    public void onOpen(WebSocket conn, ClientHandshake handshake) {
        // Connection opened, waiting for first message with roomId
    }

    @Override
    public void onClose(WebSocket conn, int code, String reason, boolean remote) {
        String roomId = connectionRooms.remove(conn);
        if (roomId != null) {
            roomManager.removeAllSessionsForClient(conn);
        }
    }

    @Override
    public void onMessage(WebSocket conn, String message) {
        try {
            JsonObject msg = JsonParser.parseString(message).getAsJsonObject();
           
            String valEr = MessageValidator.validate(msg);
            if (!"clear".equals(valEr)) {
                sendError(conn, valEr);
                return;
            }

            String roomId = msg.has("roomId") ? msg.get("roomId").getAsString() : "1";
            String userId = msg.has("userId") ? msg.get("userId").getAsString() : "unknown";
            String username = msg.has("username") ? msg.get("username").getAsString() : "Guest";
            String messageText = msg.has("message") ? msg.get("message").getAsString() : "";
            long timestamp = msg.has("timestamp") ? msg.get("timestamp").getAsLong() : System.currentTimeMillis();
            String messageType = msg.has("messageType") ? msg.get("messageType").getAsString() : "TEXT";

            String clientIp = conn.getRemoteSocketAddress().getAddress().getHostAddress();
            // Register connection on first message
            if (!connectionRooms.containsKey(conn)) {
                connectionRooms.put(conn, roomId);
                roomManager.addSession(roomId, conn);
            }

            MetricsTracker.recordReceived(roomId);

            // Create queue message
            QueueMessage queueMsg = new QueueMessage(
                    msg.has("messageId") ? msg.get("messageId").getAsString() : UUID.randomUUID().toString(),
                    roomId,
                    userId,
                    username,
                    messageText,
                    String.valueOf(timestamp),
                    messageType,
                    serverId,
                    clientIp
            );

            // Publish to RabbitMQ
            try {
                publisher.publish(queueMsg);
            } catch (Exception publishError) {
                String errorMsg = publishError.getMessage() != null
                        ? publishError.getMessage()
                        : publishError.getClass().getSimpleName();
                System.out.println("Failed to publish to room " + roomId + ": " + errorMsg);
            }

        } catch (JsonSyntaxException e) {
            sendError(conn, "Invalid JSON");
        } catch (Exception e) {
            System.out.println("onMessage error: " + e.getMessage());
        }
    }


    @Override
    public void onError(WebSocket conn, Exception ex) {
        System.err.println("WebSocket error: " + ex.getMessage());
    }

    @Override
    public void onStart() {
        System.out.println("ChatEndpoint started on port " + getPort());
        setConnectionLostTimeout(30);
    }

    private String getClientIp(WebSocket conn) {
        try {
            InetSocketAddress addr = conn.getRemoteSocketAddress();
            if (addr != null) {
                return addr.getAddress().getHostAddress();
            }
        } catch (Exception e) {
            System.err.println("Failed to get client IP: " + e.getMessage());
        }
        return "unknown";
    }

    private void sendError(WebSocket conn, String message) {
        try {
            if (conn == null || !conn.isOpen()) {
                return;
            }
            JsonObject error = new JsonObject();
            error.addProperty("error", message);
            conn.send(GSON.toJson(error));
        } catch (Exception e) {
        }
    }

    public RoomManager getRoomManager() {
        return roomManager;
    }
}
