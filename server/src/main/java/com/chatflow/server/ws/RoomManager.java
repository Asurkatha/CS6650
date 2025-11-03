package com.chatflow.server.ws;

import com.chatflow.server.mq.QueueMessage;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.java_websocket.WebSocket;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicLong;

/**
 * RoomManager - Manages WebSocket connections and room assignments
 * Handles adding, removing, and broadcasting messages to rooms
 * Implements lock-free iteration for concurrent access
 * Implements metrics tracking for broadcast performance
 * Implements connection cleanup for disconnected clients
 */
public class RoomManager {

    private static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();

    private final Map<String, Set<WebSocket>> roomSessions = new ConcurrentHashMap<>();

    // Metrics
    private final AtomicLong messagesBroadcast = new AtomicLong(0);
    private final AtomicLong broadcastFailures = new AtomicLong(0);

 
    public void addSession(String roomId, WebSocket socket) {
        roomSessions.computeIfAbsent(roomId, k -> new CopyOnWriteArraySet<>()).add(socket);
    }

  
    public void removeSession(String roomId, WebSocket socket) {
        Set<WebSocket> sessions = roomSessions.get(roomId);
        if (sessions != null) {
            sessions.remove(socket);
        }
    }

    public void removeAllSessionsForClient(WebSocket conn) {
        for (Set<WebSocket> sessions : roomSessions.values()) {
            sessions.remove(conn);
        }
    }

    /**
     * Get the number of connections in a specific room
     */
    public int getRoomSize(String roomId) {
        Set<WebSocket> sessions = roomSessions.get(roomId);
        return sessions != null ? sessions.size() : 0;
    }

   // Broadcast a message to all clients in a specific room
    public int broadcastToRoom(QueueMessage msg) {
        Set<WebSocket> connections = roomSessions.get(msg.roomId());

        // Handle empty rooms
        if (connections == null || connections.isEmpty()) {
            messagesBroadcast.incrementAndGet();
            return 0;
        }

        String json = GSON.toJson(msg);
        int successCount = 0;

        for (WebSocket socket : connections) {
            if (socket != null && socket.isOpen()) {
                try {
                    socket.send(json);
                    successCount++;
                } catch (Exception e) {
                    connections.remove(socket);
                }
            } else {
                // Dead socket, remove it
                connections.remove(socket);
            }
        }

        messagesBroadcast.incrementAndGet();

        if (successCount == 0 && !connections.isEmpty()) {
            broadcastFailures.incrementAndGet();
        }

        return successCount;
    }

    /**
     * Get total connections across all rooms
     */
    public int getTotalConnections() {
        return roomSessions.values().stream().mapToInt(Set::size).sum();
    }

    /**
     * Get total rooms with active connections
     */
    public int getRoomCount() {
        return (int) roomSessions.values().stream()
                .filter(set -> !set.isEmpty())
                .count();
    }

    public long getMessagesBroadcast() {
        return messagesBroadcast.get();
    }

    public long getBroadcastFailures() {
        return broadcastFailures.get();
    }
}
