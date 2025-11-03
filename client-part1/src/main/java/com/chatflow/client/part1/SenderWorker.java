package com.chatflow.client.part1;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;

import java.net.URI;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.CountDownLatch;

/**
 * SenderWorker class
 * - Sends messages from queue to server
 * - Sends EXACTLY what comes from queue (JOIN/TEXT/LEAVE from generator)
 * - On failure, retries the SAME message 
 */

class SenderWorker implements Runnable {

    enum Mode { FIXED_ROOM, RANDOM_ROOM_PER_MESSAGE }

    private static final int DEFAULT_MAX_IN_FLIGHT =
            Integer.getInteger("CLIENT_MAX_IN_FLIGHT", 100000);
    private static volatile Semaphore IN_FLIGHT = new Semaphore(DEFAULT_MAX_IN_FLIGHT, true);

    private final String wsBase;
    private final BlockingQueue<String> queue;
    private final Mode mode;
    private final int fixedRoom;
    private final AtomicLong sendsOk;
    private final AtomicLong sendFails;
    private final AtomicInteger totalConnections;
    private final AtomicInteger reconnections;
    private final AtomicLong acksOk;
    private final CountDownLatch allAcks;
    private final ConcurrentMap<String, String> pendingAcks;
    private final AtomicLong connectionFailures;
    private final AtomicLong retryAttempts;
    private final Random rnd = new Random();
    private final Map<Integer, SimpleClient> clients = new HashMap<>();
    private final boolean countAsNewSend;

    private static final ConcurrentLinkedQueue<SimpleClient> LIVE_CLIENTS = new ConcurrentLinkedQueue<>();

    SenderWorker(String wsBase, BlockingQueue<String> queue,
                 Mode mode, int fixedRoom, AtomicLong sendsOk, AtomicLong sendFails,
                  AtomicInteger totalConnections, AtomicInteger reconnections,
                  AtomicLong acksOk, CountDownLatch allAcks,
                  ConcurrentMap<String, String> pendingAcks,
                  AtomicLong connectionFailures,
                  AtomicLong retryAttempts,
                  boolean countAsNewSend) {
        this.wsBase = wsBase;
        this.queue = queue;
        this.mode = mode;
        this.fixedRoom = fixedRoom;
        this.sendsOk = sendsOk;
        this.sendFails = sendFails;
        this.totalConnections = totalConnections;
        this.reconnections = reconnections;
        this.acksOk = acksOk;
        this.allAcks = allAcks;
        this.pendingAcks = pendingAcks;
        this.connectionFailures = connectionFailures;
        this.retryAttempts = retryAttempts;
        this.countAsNewSend = countAsNewSend;
    }

    @Override
    public void run() {
        try {
            while (true) {
                // Pull exactly one message from queue
                String json = queue.poll(2, TimeUnit.SECONDS);
                if (json == null) {
                    if (queue.isEmpty()) {
                        break;
                    }
                    continue;
                }

                // Parse message
                JsonObject msg = JsonParser.parseString(json).getAsJsonObject();
                String messageType = msg.get("messageType").getAsString();
                String messageId = msg.get("messageId").getAsString();

                // Determine room and ensure message carries the assignment
                int room = (mode == Mode.FIXED_ROOM) ? fixedRoom : 1 + rnd.nextInt(20);
                msg.addProperty("roomId", String.valueOf(room));

                // Acquire or create a connection for the room
                SimpleClient client = clients.get(room);

                if (client == null || !client.isOpen()) {
                    if (client != null) {
                        try {
                            if (client.isOpen()) {
                                client.closeBlocking();
                            }
                        } catch (Exception ignore) {}
                        LIVE_CLIENTS.remove(client);
                    }

                    client = connect(room);
                    clients.put(room, client);
                }

                String payload = msg.toString();
                IN_FLIGHT.acquire();
                boolean ok = false;
                try {
                    if (!pendingAcks.containsKey(messageId)) {
                        IN_FLIGHT.release();
                        continue;
                    }
                    ok = sendWithRetry(client, payload, 10);
                } catch (InterruptedException e) {
                    IN_FLIGHT.release();
                    throw e;
                }

                if (ok) {
                    if (countAsNewSend) {
                        sendsOk.incrementAndGet();
                    }
                } else {
                    sendFails.incrementAndGet();
                    IN_FLIGHT.release();
                    if (!queue.offer(payload)) {
                        queue.put(payload);
                    }
                    continue;
                }
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            closeAllClients();
        }
    }
 
    private void closeAllClients() {
        for (SimpleClient c : clients.values()) {
            closeClient(c);
        }
        clients.clear();
    }

    
    private SimpleClient connect(int room) throws InterruptedException {
        String url = wsBase + "/?roomId=" + room;

        while (true) {
            SimpleClient c = new SimpleClient(
                    URI.create(url),
                    acksOk,
                    allAcks,
                    pendingAcks
            );
            try {
                c.setTcpNoDelay(true);
                c.setConnectionLostTimeout(0);
            } catch (Throwable ignore) {}

            try {
                c.connectBlocking();
                totalConnections.incrementAndGet();
                LIVE_CLIENTS.add(c);
                return c;
            } catch (InterruptedException e) {
                throw e;
            } catch (Exception ex) {
                connectionFailures.incrementAndGet();
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw ie;
                }
            }
        }
    }

    /**
     * Send message with retry logic
     * On failure, retries the EXACT SAME message (not a new one)
     */
    private boolean sendWithRetry(SimpleClient c, String json, int maxRetries)
            throws InterruptedException {
        int attempt = 0;

        while (attempt <= maxRetries) {
            try {
                if (!c.isOpen()) {
                    c.reconnectBlocking();
                    reconnections.incrementAndGet();
                }
                c.send(json);
                return true;

            } catch (NullPointerException e) {
                if (attempt == maxRetries) {
                    return false;
                }

                long sleepMs = Math.min(200L, attempt * 25L);
                Thread.sleep(sleepMs);
                retryAttempts.incrementAndGet();
                attempt++;

            } catch (Exception e) {
                if (attempt == maxRetries) {
                    return false;
                }

                long sleepMs = Math.min(1000L, (long) Math.pow(2, attempt) * 50L);
                Thread.sleep(sleepMs);
                retryAttempts.incrementAndGet();
                attempt++;
            }
        }

        return false;
    }

    /**
     * Simple WebSocket client
     */
    static class SimpleClient extends WebSocketClient {

        private final AtomicLong acksOk;
        private final CountDownLatch allAcks;
        private final ConcurrentMap<String, String> pendingAcks;

        SimpleClient(URI serverUri,
                     AtomicLong acksOk,
                     CountDownLatch allAcks,
                     ConcurrentMap<String, String> pendingAcks) {
            super(serverUri);
            this.acksOk = acksOk;
            this.allAcks = allAcks;
            this.pendingAcks = pendingAcks;
        }

        @Override public void onOpen(ServerHandshake handsh) {}

        @Override public void onMessage(String message) {
            if (message == null) {
                return;
            }
            try {
                JsonObject obj = JsonParser.parseString(message).getAsJsonObject();
                if (obj.has("messageId")) {
                    String messageId = obj.get("messageId").getAsString();
                    if (pendingAcks != null && pendingAcks.remove(messageId) != null) {
                        if (acksOk != null) acksOk.incrementAndGet();
                        if (allAcks != null) allAcks.countDown();
                        releasePermit();
                    }
                }
            } catch (Exception ignore) {
            }
        }

        @Override public void onMessage(ByteBuffer bytes) {}

        @Override public void onClose(int code, String reason, boolean remote) {
            LIVE_CLIENTS.remove(this);
        }

        @Override public void onError(Exception ex) {}
    }

    private static void releasePermit() {
        IN_FLIGHT.release();
    }

    static void releasePermits(int count) {
        for (int i = 0; i < count; i++) {
            IN_FLIGHT.release();
        }
    }

    static synchronized void configureMaxInFlight(int permits) 
    {
        int effectivePermits = Math.max(1, permits);
        IN_FLIGHT = new Semaphore(effectivePermits, true);
    }

    private static void closeClient(SimpleClient client) {
        if (client == null) {
            return;
        }
        try {
            if (client.isOpen()) {
                client.closeBlocking();
            } else {
                client.close();
            }
        } catch (Exception ignore) { }
    }

    static void shutdownAllClients() {
        SimpleClient client;
        while ((client = LIVE_CLIENTS.poll()) != null) {
            closeClient(client);
        }
    }
}
