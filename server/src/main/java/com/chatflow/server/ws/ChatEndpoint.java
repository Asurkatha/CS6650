package com.chatflow.server.ws;

import com.chatflow.server.model.ChatMessage;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import org.java_websocket.WebSocket;
import org.java_websocket.exceptions.WebsocketNotConnectedException;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;

import java.net.InetSocketAddress;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


/**
 * WebSocket server endpoint for handling chat messages in different rooms.
 * Supports multiple chat rooms identified by roomId in the URL path.
 * Validates incoming messages and responds with status and server timestamp.
 */
public class ChatEndpoint extends WebSocketServer
{
    private static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();

    // roomId -> connections set (value is dummy Boolean to get a concurrent set)
    private final Map<String, ConcurrentHashMap<WebSocket, Boolean>> rooms = new ConcurrentHashMap<>();

    public ChatEndpoint(int port)
    {
        super(new InetSocketAddress("0.0.0.0",port));
    }

    private static String roomIdFrom(String resourceDescriptor)
    {
        // Expecting "/chat/{roomId}"
        if (resourceDescriptor == null) return "default";
        String[] parts = resourceDescriptor.split("/");
        return (parts.length >= 3 && "chat".equals(parts[1]) && !parts[2].isBlank()) ? parts[2] : "default";
    }

    @Override
    public void onOpen(WebSocket conn, ClientHandshake handshake)
    {
        String roomId = roomIdFrom(handshake.getResourceDescriptor());
        rooms.computeIfAbsent(roomId, k -> new ConcurrentHashMap<>()).put(conn, Boolean.TRUE);
        System.out.printf("OPEN %s room=%s%n", conn.getRemoteSocketAddress(), roomId);
    }

    @Override
    public void onClose(WebSocket conn, int code, String reason, boolean remote)
    {
        rooms.values().forEach(set -> set.remove(conn));
        System.out.printf("CLOSE %s code=%d reason=%s%n", conn.getRemoteSocketAddress(), code, reason);
    }
    //helper to safely send a message if the connection is still open
    private void safeSend(WebSocket conn, String payload)
    {
        if (conn != null && conn.isOpen())
        {
            try
            {
                conn.send(payload);
            }
            catch (WebsocketNotConnectedException ignored)
            {
                // peer closed between isOpen() check and send(); ignore
            }
        }
    }
    @Override
    public void onMessage(WebSocket conn, String text)
    {
        try
        {
            ChatMessage msg = GSON.fromJson(text, ChatMessage.class);
            String err = MessageValidator.validate(msg);
            if (err != null)
            {
                safeSend(conn, error(err));
                return;
            }
            // minimal echo (status + server timestamp)
            JsonObject resp = new JsonObject();
            resp.addProperty("status", "OK");
            resp.addProperty("serverTimestamp", Instant.now().toString());
            safeSend(conn, GSON.toJson(resp));
        }
        catch (JsonParseException e)
        {
            safeSend(conn, error("Invalid JSON: " + e.getMessage()));
        }
        catch (Exception e)
        {
            // connection might already be closed;
            System.err.println("onMessage exception: " + e);
        }
    }
    @Override
    public void onError(WebSocket conn, Exception ex)
    {
        System.err.println("WS ERROR " + ex.getMessage());
    }

    @Override
    public void onStart()
    {
        System.out.println("ChatEndpoint started");
    }

    private String error(String msg)
    {
        JsonObject obj = new JsonObject();
        obj.addProperty("status", "ERROR");
        obj.addProperty("message", msg);
        obj.addProperty("serverTimestamp", Instant.now().toString());
        return GSON.toJson(obj);
    }
}
