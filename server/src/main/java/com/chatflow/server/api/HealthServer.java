package com.chatflow.server.api;

import com.sun.net.httpserver.HttpServer;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

/**
 * Simple HTTP server that provides a health check endpoint.
 * Responds with a JSON indicating the service status is "UP".
 */
public class HealthServer {
    private final HttpServer http;

    public HealthServer(int port) throws Exception {
        http = HttpServer.create(new InetSocketAddress("0.0.0.0",port), 0);
        http.createContext("/health", exchange -> {
            byte[] body = "{\"status\":\"UP\"}".getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream os = exchange.getResponseBody()) { os.write(body); }
        });
    }
    public void start() { http.start(); }
}
