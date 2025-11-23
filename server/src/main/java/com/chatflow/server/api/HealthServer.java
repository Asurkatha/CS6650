package com.chatflow.server.api;

import com.rabbitmq.client.*;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

/**
 * Health Server
 * - Checks RabbitMQ connectivity
 * - Checks Server connectivity
 * - Returns 200 if healthy, 503 if unhealthy
 */
public class HealthServer {
    private final HttpServer http;
    private final RabbitMQHealthCheck rabbitMQCheck;

    public HealthServer(int port) throws Exception {
        http = HttpServer.create(new InetSocketAddress("0.0.0.0", port), 0);

        // Create RabbitMQ health checker
        this.rabbitMQCheck = new RabbitMQHealthCheck();

        http.createContext("/health", exchange -> {
            try {
                if (rabbitMQCheck.isHealthy()) {
                    //  Healthy: RabbitMQ connected
                    byte[] body = "{\"status\":\"UP\",\"rabbitmq\":\"connected\"}"
                            .getBytes(StandardCharsets.UTF_8);
                    exchange.getResponseHeaders().add("Content-Type", "application/json");
                    exchange.sendResponseHeaders(200, body.length);
                    try (OutputStream os = exchange.getResponseBody()) {
                        os.write(body);
                    }
                } else {
                    // Unhealthy: RabbitMQ not connected
                    byte[] body = "{\"status\":\"DOWN\",\"rabbitmq\":\"disconnected\"}"
                            .getBytes(StandardCharsets.UTF_8);
                    exchange.getResponseHeaders().add("Content-Type", "application/json");
                    exchange.sendResponseHeaders(503, body.length);
                    try (OutputStream os = exchange.getResponseBody()) {
                        os.write(body);
                    }
                }
            } catch (Exception e) {
                byte[] body = ("{\"status\":\"ERROR\",\"message\":\"" +
                        e.getMessage() + "\"}")
                        .getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.sendResponseHeaders(503, body.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(body);
                }
            }
        });
    }

    public void addContext(String path, HttpHandler handler) {
        http.createContext(path, handler);
    }

    public void start() {
        http.start();
    }

    public void stop() {
        http.stop(0);
        System.out.println("Health server stopped");
    }

    /**
     * Inner class for RabbitMQ health checking
     * Caches results for 5 seconds to avoid hammering RabbitMQ
     */
    private static class RabbitMQHealthCheck {
        private static final long CACHE_DURATION = 5000; // 5 seconds
        private long lastCheck = 0;
        private boolean lastStatus = false;

        boolean isHealthy() {
            long now = System.currentTimeMillis();

            // Use cached result if recent
            if (now - lastCheck < CACHE_DURATION) {
                return lastStatus;
            }

            try {
                String rabbitHost = System.getProperty("RABBIT_HOST", "172.31.43.127");
                int rabbitPort = Integer.parseInt(System.getProperty("RABBIT_PORT", "5672"));
                String rabbitUser = System.getProperty("RABBIT_USER", "asurkatha");//RabbitMQ credentials just for this project
                String rabbitPass = System.getProperty("RABBIT_PASS", "SecurePassword123");//RabbitMQ credentials just for this project

                ConnectionFactory factory = new ConnectionFactory();
                factory.setHost(rabbitHost);
                factory.setPort(rabbitPort);
                factory.setUsername(rabbitUser);
                factory.setPassword(rabbitPass);
                factory.setConnectionTimeout(2000); // 2 second timeout

                Connection conn = factory.newConnection();
                conn.close();

                lastStatus = true;
                lastCheck = now;
                return true;

            } catch (Exception e) {
                System.err.println("[Health] RabbitMQ check failed: " + e.getMessage());
                lastStatus = false;
                lastCheck = now;
                return false;
            }
        }
    }
}