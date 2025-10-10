package com.chatflow.client.part1;

import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;

import java.net.URI;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

class SenderWorker implements Runnable {

    enum Mode { FIXED_ROOM, RANDOM_ROOM_PER_MESSAGE }

    private final String wsBase;
    private final BlockingQueue<String> queue;
    private final int messagesToSend;
    private final Mode mode;
    private final int fixedRoom;

    private final AtomicLong sendsOk;
    private final AtomicLong sendFails;
    private final AtomicInteger totalConnections;
    private final AtomicInteger reconnections;
    private final AtomicLong acksOk;
    private final java.util.concurrent.CountDownLatch allAcks; // may be null in warmup if you choose

    private final Random rnd = new Random();
    private final Map<Integer, SimpleClient> clients = new HashMap<>();

    SenderWorker(String wsBase,
                 BlockingQueue<String> queue,
                 int messagesToSend,
                 Mode mode,
                 int fixedRoom,
                 AtomicLong sendsOk,
                 AtomicLong sendFails,
                 AtomicInteger totalConnections,
                 AtomicInteger reconnections,
                 AtomicLong acksOk,                                  // NEW
                 java.util.concurrent.CountDownLatch allAcks) {      // NEW
        this.wsBase = wsBase;
        this.queue = queue;
        this.messagesToSend = messagesToSend;
        this.mode = mode;
        this.fixedRoom = fixedRoom;
        this.sendsOk = sendsOk;
        this.sendFails = sendFails;
        this.totalConnections = totalConnections;
        this.reconnections = reconnections;
        this.acksOk = acksOk;
        this.allAcks = allAcks;
    }

    @Override
    public void run() {
        int sent = 0;
        try {
            while (sent < messagesToSend) {
                String json = queue.poll(1, TimeUnit.SECONDS);
                if (json == null) continue;

                int room = (mode == Mode.FIXED_ROOM) ? fixedRoom : 1 + rnd.nextInt(20);
                // check existing connection or create new one
                SimpleClient client = clients.get(room);
                if (client == null || !client.isOpen()) {
                    if (client != null) try { client.closeBlocking(); } catch (Exception ignore) {}
                    client = connect(room);
                    clients.put(room, client);
                }

                boolean ok = sendWithRetry(client, json, 5); //exponential backoff retries
                if (ok) sendsOk.incrementAndGet(); else sendFails.incrementAndGet();

                sent++;
            }
        } catch (InterruptedException ignored) {
            // allow fast shutdown
        } finally {
            // Wait for global ACK latch to reach zero (or timeout) before closing sockets,
            // so we don't kill in-flight ACKs.
            if (allAcks != null) {
                try {
                    // generous timeout for ACKs to arrive
                    allAcks.await(120, java.util.concurrent.TimeUnit.SECONDS);
                } catch (InterruptedException ignore) {}
            }
            for (SimpleClient c : clients.values()) {
                try { c.closeBlocking(); } catch (Exception ignore) {}
            }
        }

    }

    // connect to a specific room, return connected client
    private SimpleClient connect(int room) throws InterruptedException {
        String url = wsBase + "/chat/" + room;
        SimpleClient c = new SimpleClient(URI.create(url), acksOk, allAcks);
        try {
            c.setTcpNoDelay(true);
            c.setConnectionLostTimeout(0); // disable client ping timer during load
        } catch (Throwable ignore) {}
        c.connectBlocking();
        totalConnections.incrementAndGet();
        return c;
    }
    // helper to send with retries and exponential backoff
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
            } catch (Exception e) {
                if (attempt == maxRetries) return false;
                long sleepMs = Math.min(2000L, (long) Math.pow(2, attempt) * 50L);
                Thread.sleep(sleepMs);
                attempt++;
            }
        }
        return false;
    }

    // Simple WebSocket client that counts ACKs (echoes) from server
    static class SimpleClient extends WebSocketClient {
        private final AtomicLong acksOk;
        private final java.util.concurrent.CountDownLatch allAcks;

        SimpleClient(URI serverUri,
                     AtomicLong acksOk,
                     java.util.concurrent.CountDownLatch allAcks) {
            super(serverUri);
            this.acksOk = acksOk;
            this.allAcks = allAcks;
        }

        @Override public void onOpen(ServerHandshake handsh) { /* no-op */ }

        @Override public void onMessage(String message) {
            // Every echo/response from the server counts as an ACK
            if (acksOk != null) acksOk.incrementAndGet();
            if (allAcks != null) allAcks.countDown();
        }

        @Override public void onMessage(ByteBuffer bytes) { /* ignore binary */ }

        @Override public void onClose(int code, String reason, boolean remote) { /* no-op */ }

        @Override public void onError(Exception ex) { /* optionally log */ }
    }
}
