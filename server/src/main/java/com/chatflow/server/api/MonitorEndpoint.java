package com.chatflow.server.api;

import com.chatflow.server.mq.QueueMessage;
import com.chatflow.server.ws.RoomManager;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Monitor Endpoint (Dashboard) to see chat activity in each room (added for my own ease of debugging and worked with AI to have an interface now)
 * - Provides a dashboard for monitoring chat activity
 * - Tracks messages per room
 * - Provides API endpoints for room and message data
 */
//CAN ENABLE/DISABLE LOGGING USING CLI arguments "DENABLE_DASHBOARD_LOGGING=true" or "DENABLE_DASHBOARD_LOGGING=false" false by default
public class MonitorEndpoint {
    private static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();
    private static final Map<String, List<MessageLog>> roomMessages = new ConcurrentHashMap<>();
    private static final int MAX_MESSAGES_PER_ROOM = 500000;
    private static final DateTimeFormatter TIME_FORMAT = DateTimeFormatter.ofPattern("HH:mm:ss");

    private static volatile boolean LOGGING_ENABLED = false;

    private final HttpServer server;
    private final RoomManager roomManager;

    public static void enableLogging(boolean enable) {
        LOGGING_ENABLED = enable;
    }

    public MonitorEndpoint(int port, RoomManager roomManager) throws IOException {
        this.roomManager = roomManager;
        this.server = HttpServer.create(new InetSocketAddress(port), 0);

        server.createContext("/dashboard", new DashboardHandler());
        server.createContext("/api/rooms", new RoomsDataHandler());
        server.createContext("/api/messages", new MessagesDataHandler());

        server.setExecutor(null);
    }

    public void start() {
        server.start();
        System.out.println(" Monitor dashboard started on port " + server.getAddress().getPort());
        System.out.println(" Dashboard: http://localhost:" + server.getAddress().getPort() + "/dashboard");
    }

    public void stop() {
        server.stop(0);
    }

    public static void logMessage(QueueMessage msg, String action) {

        if (!LOGGING_ENABLED) {
            return;
        }

        String roomId = msg.roomId();
        roomMessages.computeIfAbsent(roomId, k -> new CopyOnWriteArrayList<>());

        List<MessageLog> logs = roomMessages.get(roomId);

        logs.add(new MessageLog(
                Instant.now().toString(),
                action,
                msg.messageType(),
                msg.username(),
                msg.clientIp(),
                msg.message()
        ));

        if (logs.size() > MAX_MESSAGES_PER_ROOM) {
            logs.remove(0);
        }
    }

    private class DashboardHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String html = getDashboardHTML();
            byte[] bytes = html.getBytes(StandardCharsets.UTF_8);

            exchange.getResponseHeaders().set("Content-Type", "text/html; charset=UTF-8");
            exchange.sendResponseHeaders(200, bytes.length);

            try (OutputStream os = exchange.getResponseBody()) {
                os.write(bytes);
            }
        }
    }

    private class RoomsDataHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            Map<String, Object> data = new HashMap<>();

            for (int i = 1; i <= 20; i++) {
                String roomId = String.valueOf(i);
                int connections = roomManager.getRoomSize(roomId);
                int messageCount = roomMessages.getOrDefault(roomId, Collections.emptyList()).size();

                Map<String, Object> roomData = new HashMap<>();
                roomData.put("connections", connections);
                roomData.put("messages", messageCount);
                data.put(roomId, roomData);
            }

            String json = GSON.toJson(data);
            byte[] bytes = json.getBytes(StandardCharsets.UTF_8);

            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
            exchange.sendResponseHeaders(200, bytes.length);

            try (OutputStream os = exchange.getResponseBody()) {
                os.write(bytes);
            }
        }
    }

    private class MessagesDataHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String query = exchange.getRequestURI().getQuery();
            String roomId = null;

            if (query != null && query.startsWith("room=")) {
                roomId = query.substring(5);
            }

            Map<String, Object> response = new HashMap<>();
            if (roomId != null) {
                List<MessageLog> logs = roomMessages.getOrDefault(roomId, Collections.emptyList());
                response.put("roomId", roomId);
                response.put("messages", logs);
            }

            String json = GSON.toJson(response);
            byte[] bytes = json.getBytes(StandardCharsets.UTF_8);

            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
            exchange.sendResponseHeaders(200, bytes.length);

            try (OutputStream os = exchange.getResponseBody()) {
                os.write(bytes);
            }
        }
    }

    private String getDashboardHTML() {
        StringBuilder html = new StringBuilder();
        html.append("<!DOCTYPE html>\n");
        html.append("<html>\n");
        html.append("<head>\n");
        html.append("    <title>Chat Monitor Dashboard</title>\n");
        html.append("    <meta charset=\"UTF-8\">\n");
        html.append("    <style>\n");
        html.append("        * { margin: 0; padding: 0; box-sizing: border-box; }\n");
        html.append("        body {\n");
        html.append("            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;\n");
        html.append("            background: #1a1a2e;\n");
        html.append("            color: #eee;\n");
        html.append("            padding: 20px;\n");
        html.append("        }\n");
        html.append("        .header {\n");
        html.append("            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);\n");
        html.append("            padding: 30px;\n");
        html.append("            border-radius: 10px;\n");
        html.append("            margin-bottom: 20px;\n");
        html.append("            box-shadow: 0 4px 6px rgba(0,0,0,0.3);\n");
        html.append("        }\n");
        html.append("        .header h1 { margin-bottom: 10px; }\n");
        html.append("        .stats {\n");
        html.append("            display: grid;\n");
        html.append("            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));\n");
        html.append("            gap: 15px;\n");
        html.append("            margin-bottom: 20px;\n");
        html.append("        }\n");
        html.append("        .stat-card {\n");
        html.append("            background: #16213e;\n");
        html.append("            padding: 20px;\n");
        html.append("            border-radius: 8px;\n");
        html.append("            border-left: 4px solid #667eea;\n");
        html.append("        }\n");
        html.append("        .stat-card h3 { color: #667eea; font-size: 14px; margin-bottom: 5px; }\n");
        html.append("        .stat-card .value { font-size: 32px; font-weight: bold; }\n");
        html.append("        .main-grid {\n");
        html.append("            display: grid;\n");
        html.append("            grid-template-columns: 2fr 3fr;\n");
        html.append("            gap: 20px;\n");
        html.append("        }\n");
        html.append("        .rooms-section h2 { color: #667eea; margin-bottom: 15px; }\n");
        html.append("        .rooms-grid {\n");
        html.append("            display: grid;\n");
        html.append("            grid-template-columns: repeat(auto-fill, minmax(140px, 1fr));\n");
        html.append("            gap: 10px;\n");
        html.append("            max-height: 600px;\n");
        html.append("            overflow-y: auto;\n");
        html.append("        }\n");
        html.append("        .room-card {\n");
        html.append("            background: #16213e;\n");
        html.append("            border-radius: 8px;\n");
        html.append("            padding: 12px;\n");
        html.append("            border: 2px solid #0f3460;\n");
        html.append("            cursor: pointer !important;\n");
        html.append("            transition: all 0.2s;\n");
        html.append("            pointer-events: auto !important;\n");
        html.append("        }\n");
        html.append("        .room-card:hover {\n");
        html.append("            border-color: #667eea;\n");
        html.append("            transform: translateY(-2px);\n");
        html.append("        }\n");
        html.append("        .room-card.selected {\n");
        html.append("            border-color: #667eea;\n");
        html.append("            background: #1a2847;\n");
        html.append("        }\n");
        html.append("        .room-card h3 {\n");
        html.append("            color: #667eea;\n");
        html.append("            font-size: 14px;\n");
        html.append("            margin-bottom: 8px;\n");
        html.append("        }\n");
        html.append("        .badge {\n");
        html.append("            background: #667eea;\n");
        html.append("            color: white;\n");
        html.append("            padding: 2px 6px;\n");
        html.append("            border-radius: 10px;\n");
        html.append("            font-size: 10px;\n");
        html.append("            margin-left: 5px;\n");
        html.append("        }\n");
        html.append("        .badge.online { background: #10b981; }\n");
        html.append("        .badge.offline { background: #6b7280; }\n");
        html.append("        .room-stats {\n");
        html.append("            font-size: 11px;\n");
        html.append("            color: #aaa;\n");
        html.append("            display: flex;\n");
        html.append("            flex-direction: column;\n");
        html.append("            gap: 3px;\n");
        html.append("        }\n");
        html.append("        .messages-panel {\n");
        html.append("            background: #16213e;\n");
        html.append("            border-radius: 8px;\n");
        html.append("            padding: 20px;\n");
        html.append("            max-height: 600px;\n");
        html.append("            display: flex;\n");
        html.append("            flex-direction: column;\n");
        html.append("        }\n");
        html.append("        .messages-panel h2 {\n");
        html.append("            color: #667eea;\n");
        html.append("            margin-bottom: 15px;\n");
        html.append("        }\n");
        html.append("        .messages-container {\n");
        html.append("            flex: 1;\n");
        html.append("            overflow-y: auto;\n");
        html.append("        }\n");
        html.append("        .message {\n");
        html.append("            background: #0f3460;\n");
        html.append("            padding: 10px;\n");
        html.append("            border-radius: 6px;\n");
        html.append("            margin-bottom: 8px;\n");
        html.append("            border-left: 3px solid #667eea;\n");
        html.append("        }\n");
        html.append("        .message.JOIN { border-left-color: #10b981; }\n");
        html.append("        .message.LEAVE { border-left-color: #ef4444; }\n");
        html.append("        .message.TEXT { border-left-color: #667eea; }\n");
        html.append("        .message-header {\n");
        html.append("            display: flex;\n");
        html.append("            justify-content: space-between;\n");
        html.append("            margin-bottom: 6px;\n");
        html.append("            font-size: 12px;\n");
        html.append("        }\n");
        html.append("        .message-user {\n");
        html.append("            font-weight: bold;\n");
        html.append("            color: #667eea;\n");
        html.append("        }\n");
        html.append("        .message-time {\n");
        html.append("            color: #888;\n");
        html.append("            font-size: 11px;\n");
        html.append("        }\n");
        html.append("        .message-content {\n");
        html.append("            color: #ddd;\n");
        html.append("            font-size: 13px;\n");
        html.append("            word-wrap: break-word;\n");
        html.append("        }\n");
        html.append("        .message-meta {\n");
        html.append("            margin-top: 6px;\n");
        html.append("            font-size: 10px;\n");
        html.append("            color: #666;\n");
        html.append("        }\n");
        html.append("        .no-messages {\n");
        html.append("            text-align: center;\n");
        html.append("            padding: 40px;\n");
        html.append("            color: #666;\n");
        html.append("        }\n");
        html.append("        ::-webkit-scrollbar { width: 8px; }\n");
        html.append("        ::-webkit-scrollbar-track { background: #0f3460; }\n");
        html.append("        ::-webkit-scrollbar-thumb { background: #667eea; border-radius: 4px; }\n");
        html.append("    </style>\n");
        html.append("</head>\n");
        html.append("<body>\n");
        html.append("    <div class=\"header\">\n");
        html.append("        <h1>🎯 Real-Time Chat Monitor</h1>\n");
        html.append("        <p>Auto-refreshing every 2 seconds</p>\n");
        html.append("    </div>\n");
        html.append("    <div class=\"stats\">\n");
        html.append("        <div class=\"stat-card\">\n");
        html.append("            <h3>ACTIVE ROOMS</h3>\n");
        html.append("            <div class=\"value\" id=\"activeRooms\">0</div>\n");
        html.append("        </div>\n");
        html.append("        <div class=\"stat-card\">\n");
        html.append("            <h3>TOTAL CONNECTIONS</h3>\n");
        html.append("            <div class=\"value\" id=\"totalConnections\">0</div>\n");
        html.append("        </div>\n");
        html.append("        <div class=\"stat-card\">\n");
        html.append("            <h3>TOTAL MESSAGES</h3>\n");
        html.append("            <div class=\"value\" id=\"totalMessages\">0</div>\n");
        html.append("        </div>\n");
        html.append("    </div>\n");
        html.append("    <div class=\"main-grid\">\n");
        html.append("        <div class=\"rooms-section\">\n");
        html.append("            <h2>📊 All Rooms</h2>\n");
        html.append("            <div class=\"rooms-grid\" id=\"roomsGrid\"></div>\n");
        html.append("        </div>\n");
        html.append("        <div class=\"messages-panel\">\n");
        html.append("            <h2 id=\"messagesPanelTitle\">Select a room to view messages</h2>\n");
        html.append("            <div class=\"messages-container\" id=\"messagesContainer\">\n");
        html.append("                <div class=\"no-messages\">Click on a room to see its messages</div>\n");
        html.append("            </div>\n");
        html.append("        </div>\n");
        html.append("    </div>\n");
        html.append("    <script>\n");
        html.append("        let selectedRoom = null;\n");
        html.append("        let isUpdating = false;\n");
        html.append("        async function fetchRooms() {\n");
        html.append("            if (isUpdating) return;\n");
        html.append("            isUpdating = true;\n");
        html.append("            try {\n");
        html.append("                const response = await fetch('/api/rooms');\n");
        html.append("                const data = await response.json();\n");
        html.append("                let activeRooms = 0, totalConnections = 0, totalMessages = 0;\n");
        html.append("                const roomsGrid = document.getElementById('roomsGrid');\n");
        html.append("                roomsGrid.innerHTML = '';\n");
        html.append("                for (let i = 1; i <= 20; i++) {\n");
        html.append("                    const roomId = String(i);\n");
        html.append("                    const roomData = data[roomId] || { connections: 0, messages: 0 };\n");
        html.append("                    if (roomData.connections > 0) activeRooms++;\n");
        html.append("                    totalConnections += roomData.connections;\n");
        html.append("                    totalMessages += roomData.messages;\n");
        html.append("                    const card = document.createElement('div');\n");
        html.append("                    card.className = 'room-card' + (selectedRoom === roomId ? ' selected' : '');\n");
        html.append("                    card.style.pointerEvents = 'auto';\n"); // ✅ Force clickable
        html.append("                    card.style.cursor = 'pointer';\n");     // ✅ Force cursor
        html.append("                    card.onclick = function() { selectRoom(roomId); };\n"); // ✅ Direct function
        html.append("                    card.innerHTML = `<h3>Room ${roomId}<span class='badge ${roomData.connections > 0 ? 'online' : 'offline'}'>${roomData.connections > 0 ? 'ON' : 'OFF'}</span></h3><div class='room-stats'><div>👥 ${roomData.connections}</div><div>💬 ${roomData.messages}</div></div>`;\n");
        html.append("                    roomsGrid.appendChild(card);\n");
        html.append("                }\n");
        html.append("                document.getElementById('activeRooms').textContent = activeRooms;\n");
        html.append("                document.getElementById('totalConnections').textContent = totalConnections;\n");
        html.append("                document.getElementById('totalMessages').textContent = totalMessages;\n");
        html.append("            } catch (error) { console.error('Error:', error); }\n");
        html.append("            finally { isUpdating = false; }\n");
        html.append("        }\n");
        html.append("        async function selectRoom(roomId) {\n");
        html.append("            selectedRoom = roomId;\n");
        html.append("            document.getElementById('messagesPanelTitle').textContent = `📩 Room ${roomId} Messages (BROADCAST only)`;\n");
        html.append("            fetchRooms();\n");
        html.append("            try {\n");
        html.append("                const response = await fetch(`/api/messages?room=${roomId}`);\n");
        html.append("                const data = await response.json();\n");
        html.append("                const messages = data.messages || [];\n");
        html.append("                const container = document.getElementById('messagesContainer');\n");
        html.append("                const wasAtBottom = container.scrollHeight - container.scrollTop <= container.clientHeight + 50;\n");
        html.append("                if (messages.length === 0) {\n");
        html.append("                    container.innerHTML = '<div class=\"no-messages\">No broadcast messages yet in this room</div>';\n");
        html.append("                    return;\n");
        html.append("                }\n");
        html.append("                container.innerHTML = messages.map(msg => {\n");
        html.append("                    const icon = msg.messageType === 'JOIN' ? '✅' : msg.messageType === 'LEAVE' ? '👋' : '💬';\n");
        html.append("                    const content = msg.message && msg.message.trim() \n");
        html.append("                        ? msg.message \n");
        html.append("                        : (msg.messageType === 'JOIN' ? '(joined room)' : \n");
        html.append("                           msg.messageType === 'LEAVE' ? '(left room)' : '(no content)');\n");
        html.append("                    return `<div class='message ${msg.messageType}'>\n");
        html.append("                               <div class='message-header'>\n");
        html.append("                                   <span class='message-user'>${icon} ${msg.username}</span>\n");
        html.append("                                   <span class='message-time'>${new Date(msg.timestamp).toLocaleTimeString()}</span>\n");
        html.append("                               </div>\n");
        html.append("                               <div class='message-content'>${content}</div>\n");
        html.append("                               <div class='message-meta'>${msg.messageType}</div>\n");
        html.append("                           </div>`;\n");
        html.append("                }).reverse().join('');\n");
        html.append("                if (wasAtBottom) container.scrollTop = container.scrollHeight;\n");
        html.append("            } catch (error) { console.error('Error:', error); }\n");
        html.append("        }\n");
        html.append("        setInterval(fetchRooms, 100);\n");
        html.append("        setInterval(() => { if (selectedRoom) selectRoom(selectedRoom); }, 100);\n");
        html.append("        fetchRooms();\n");
        html.append("    </script>\n");
        html.append("</body>\n");
        html.append("</html>\n");

        return html.toString();
    }

    private static class MessageLog {
        String timestamp;
        String action;
        String messageType;
        String username;
        String clientIp;
        String message;

        MessageLog(String timestamp, String action, String messageType,
                   String username, String clientIp, String message) {
            this.timestamp = timestamp;
            this.action = action;
            this.messageType = messageType;
            this.username = username;
            this.clientIp = clientIp;
            this.message = message;
        }
    }
}
