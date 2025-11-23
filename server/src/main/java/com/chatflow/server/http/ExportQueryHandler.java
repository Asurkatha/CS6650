package com.chatflow.server.http;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * HTTP endpoint to trigger automatic query export.
 * Endpoint: GET /export-queries?testId=<testId>
 *
 * Note: Query export functionality is handled by QueryHandler and QueryResultsHandler endpoints.
 * This endpoint acknowledges the request for backward compatibility.
 */
public class ExportQueryHandler implements HttpHandler {

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        if (!"GET".equals(exchange.getRequestMethod())) {
            sendResponse(exchange, 405, "{\"error\": \"Method not allowed\"}");
            return;
        }

        try {
            String query = exchange.getRequestURI().getQuery();
            String testId = null;

            if (query != null) {
                Map<String, String> params = parseQueryParams(query);
                testId = params.get("testId");
            }

            if (testId == null || testId.isEmpty()) {
                sendResponse(exchange, 400, "{\"error\": \"Missing required parameter: testId\"}");
                return;
            }

            System.out.println("[Export] Received export request for testId: " + testId);
            System.out.println("[Export] Queries are available via /queries and /query-results endpoints");

            // Return success immediately (queries are available via standard endpoints)
            String response = String.format(
                "{\"status\": \"success\", \"message\": \"Query export started for testId: %s\", \"testId\": \"%s\"}",
                testId, testId
            );
            sendResponse(exchange, 200, response);

        } catch (Exception e) {
            System.err.println("[Export] Handler error: " + e.getMessage());
            e.printStackTrace();
            String errorResponse = String.format(
                "{\"status\": \"error\", \"message\": \"%s\"}",
                e.getMessage().replace("\"", "'")
            );
            sendResponse(exchange, 500, errorResponse);
        }
    }

    private Map<String, String> parseQueryParams(String query) {
        Map<String, String> params = new HashMap<>();
        if (query == null || query.isEmpty()) {
            return params;
        }

        for (String param : query.split("&")) {
            String[] parts = param.split("=", 2);
            if (parts.length == 2) {
                params.put(parts[0], parts[1]);
            }
        }
        return params;
    }

    private void sendResponse(HttpExchange exchange, int statusCode, String response) throws IOException {
        byte[] bytes = response.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(statusCode, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }
}
