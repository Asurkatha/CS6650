package com.chatflow.server;

import com.chatflow.server.api.HealthServer;
import com.chatflow.server.ws.ChatEndpoint;


/**
 * Main application class to start the WebSocket chat server and health check server.
 * receives configuration from environment variables and starts the servers.
 */
public class App {
    public static void main(String[] args) throws Exception {
        int wsPort = getIntEnv("WS_PORT", 8080);
        int httpPort = getIntEnv("HTTP_PORT", 8081);

        ChatEndpoint ws = new ChatEndpoint(wsPort);
        ws.start();
        System.out.println("WebSocket: ws://0.0.0.0:" + wsPort + "/chat/{roomId}");
        HealthServer health = new HealthServer(httpPort);
        health.start();
        System.out.println("Health:    http://0.0.0.0:" + httpPort + "/health");
    }
    // Helper method to get integer environment variables with a default value
    private static int getIntEnv(String key, int def) {
        try { return Integer.parseInt(System.getenv().getOrDefault(key, String.valueOf(def))); }
        catch (Exception e) { return def; }
    }
}
