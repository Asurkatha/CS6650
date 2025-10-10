package com.chatflow.client.part2;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;

import java.net.URI;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
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

    // send counters (incremented on successful send())
    private final AtomicLong sendsOk;
    private final AtomicLong sendFails;

    // ack counters (incremented in onMessage)
    private final AtomicLong acksOk;

    // pendings cleared on close/reconnect
    private final AtomicLong lostPendings;

    private final AtomicInteger totalConnections;
    private final AtomicInteger reconnections;

    private final java.util.concurrent.CountDownLatch allAcks; // NEW: global latch

    private final MetricsCollector metrics; // nullable for warmup

    private final Random rnd = new Random();
    private final Map<Integer, SimpleClient> clients = new HashMap<>();

    SenderWorker(String wsBase,
                 BlockingQueue<String> queue,
                 int messagesToSend,
                 Mode mode,
                 int fixedRoom,
                 AtomicLong sendsOk,
                 AtomicLong sendFails,
                 AtomicLong acksOk,
                 AtomicLong lostPendings,
                 AtomicInteger totalConnections,
                 AtomicInteger reconnections,
                 java.util.concurrent.CountDownLatch allAcks,
                 MetricsCollector metrics) {
        this.wsBase = wsBase;
        this.queue = queue;
        this.messagesToSend = messagesToSend;
        this.mode = mode;
        this.fixedRoom = fixedRoom;
        this.sendsOk = sendsOk;
        this.sendFails = sendFails;
        this.acksOk = acksOk;
        this.lostPendings = lostPendings;
        this.totalConnections = totalConnections;
        this.reconnections = reconnections;
        this.allAcks = allAcks;
        this.metrics = metrics;
    }

    @Override
    public void run() {
        int sent = 0;
        try {
            while (sent < messagesToSend) {
                String json = queue.poll(1, TimeUnit.SECONDS);
                if (json == null) continue;

                int room = (mode == Mode.FIXED_ROOM) ? fixedRoom : 1 + rnd.nextInt(20);
                SimpleClient client = clients.get(room);
                if (client == null || !client.isOpen()) {
                    if (client != null) try { client.closeBlocking(); } catch (Exception ignore) {}
                    client = connect(room);
                    clients.put(room, client);
                }

                String type = extractType(json);
                boolean ok = sendWithRetry(client, json, type, room, 5);
                if (ok) sendsOk.incrementAndGet(); else sendFails.incrementAndGet();

                sent++;
            }
        } catch (InterruptedException ignored) {
        } finally {
            // Wait for each connection to drain its pending ACKs before closing (up to 30s)
            for (SimpleClient c : clients.values()) {
                long waited = 0;
                while (c.pendingCount() > 0 && waited < 30000) {
                    try { Thread.sleep(10); } catch (InterruptedException ignore) {}
                    waited += 10;
                }
                try { c.closeBlocking(); } catch (Exception ignore) {}
            }
        }
    }

    private String extractType(String json) {
        try {
            return JsonParser.parseString(json).getAsJsonObject().get("messageType").getAsString();
        } catch (Exception e) { return ""; }
    }

    private SimpleClient connect(int room) throws InterruptedException {
        String url = wsBase + "/chat/" + room;
        SimpleClient c = new SimpleClient(
                URI.create(url), room, metrics, acksOk, lostPendings, reconnections, allAcks);
        try { c.setTcpNoDelay(true); } catch (Throwable ignore) {}
        c.connectBlocking();
        totalConnections.incrementAndGet();
        return c;
    }

    // register Pending BEFORE send; rollback if send fails (keeps FIFO aligned)
    private boolean sendWithRetry(SimpleClient c, String json, String type, int room, int maxRetries)
            throws InterruptedException {

        int attempt = 0;
        while (attempt <= maxRetries) {
            Pending p = new Pending(System.nanoTime(), Instant.now().toString(), type, room);
            c.pushPending(p);
            try {
                if (!c.isOpen()) {
                    c.reconnectBlocking();
                    reconnections.incrementAndGet();
                }
                c.send(json);                    // on success - onMessage() will poll the Pending
                return true;
            } catch (Exception e) {
                c.rollbackPending(p);            // remove stale Pending on failed send
                if (attempt == maxRetries) return false;
                long sleepMs = Math.min(2000L, (long) Math.pow(2, attempt) * 50L);
                Thread.sleep(sleepMs);
                attempt++;
            }
        }
        return false;
    }

    //data carried to onMessage for latency
    static class Pending {
        final long sendNanos;   // monotonic time (for latency)
        final String sendIso;   // wall clock for CSV
        final String type;
        final int room;
        Pending(long sendNanos, String sendIso, String type, int room) {
            this.sendNanos = sendNanos; this.sendIso = sendIso; this.type = type; this.room = room;
        }
    }

    // WebSocket client with per-connection FIFO of pendings
    static class SimpleClient extends WebSocketClient {
        private final int roomId;
        private final ConcurrentLinkedQueue<Pending> pendings = new ConcurrentLinkedQueue<>();
        private final MetricsCollector metrics; // nullable in warmup
        private final AtomicLong acksOk;
        private final AtomicLong lostPendings;
        private final AtomicInteger reconnections;
        private final java.util.concurrent.CountDownLatch allAcks;

        SimpleClient(URI serverUri,
                     int roomId,
                     MetricsCollector metrics,
                     AtomicLong acksOk,
                     AtomicLong lostPendings,
                     AtomicInteger reconnections,
                     java.util.concurrent.CountDownLatch allAcks) {
            super(serverUri);
            this.roomId = roomId;
            this.metrics = metrics;
            this.acksOk = acksOk;
            this.lostPendings = lostPendings;
            this.reconnections = reconnections;
            this.allAcks = allAcks;
        }

        void pushPending(Pending p) { pendings.add(p); }
        void rollbackPending(Pending p) { pendings.remove(p); }
        int pendingCount() { return pendings.size(); }

        @Override public void onOpen(ServerHandshake h) { }
        @Override public void onMessage(ByteBuffer bytes) { }

        @Override
        public void onMessage(String message) { // one ack per message, in order
            Pending p = pendings.poll();
            if (p == null) return;

            acksOk.incrementAndGet();
            if (allAcks != null) allAcks.countDown();  // global ack latch

            if (metrics != null) {
                double latencyMs = (System.nanoTime() - p.sendNanos) / 1_000_000.0; // ns to ms
                String status = "OK"; // default
                try {
                    if (message.startsWith("{")) {
                        JsonObject o = JsonParser.parseString(message).getAsJsonObject();
                        if (o.has("status")) status = o.get("status").getAsString();
                    } else { status = message.trim(); }
                } catch (Exception ignore) {  }
                metrics.logCsv(p.sendIso, p.type, (long) Math.round(latencyMs), status, roomId);

            }
        }

        @Override
        public void onClose(int code, String reason, boolean remote) {
            // count and drop any pending sends on this dead socket
            int dropped = 0; Pending x;
            while ((x = pendings.poll()) != null) dropped++;
            if (dropped > 0) lostPendings.addAndGet(dropped);
        }

        @Override
        public void onError(Exception e) {

        }

        @Override
        public void reconnect() {
            super.reconnect();
            reconnections.incrementAndGet();
            //clear (onClose also counts)
            pendings.clear();
        }
    }
}
