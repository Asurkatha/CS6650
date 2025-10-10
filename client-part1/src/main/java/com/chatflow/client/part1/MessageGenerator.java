package com.chatflow.client.part1;

import com.google.gson.JsonObject;
import java.time.Instant;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
/**
 * Generates random chat messages and puts them into a blocking queue.
 * Each message includes userId, username, message content, timestamp, and messageType.
 * Message types are distributed as 90% TEXT, 5% JOIN, and 5% LEAVE.
 */
class MessageGenerator implements Runnable {
    private final BlockingQueue<String> out;
    private final int total;
    private final Random rnd = new Random();
    private static final String[] POOL = makeMessagePool();

    MessageGenerator(BlockingQueue<String> out, int total) {
        this.out = out;
        this.total = total;
    }

    //creats random messages and puts them in the out queue
    @Override
    public void run() {
        try {
            for (int i = 0; i < total; i++) {
                int userId = 1 + rnd.nextInt(100000);
                String username = "user" + userId;
                String message = POOL[rnd.nextInt(POOL.length)];
                String messageType = pickType();

                JsonObject msg = new JsonObject();
                msg.addProperty("userId", userId);
                msg.addProperty("username", username);
                msg.addProperty("message", message);
                msg.addProperty("timestamp", Instant.now().toString());
                msg.addProperty("messageType", messageType);

                out.put(msg.toString()); // JUST JSON
            }
        } catch (InterruptedException ignored) {}
    }

    private static String pickType() {
        int x = (int) (Math.random() * 100);
        if (x < 90) return "TEXT";
        if (x < 95) return "JOIN";
        return "LEAVE";
    }

    private static String[] makeMessagePool() {
        String base = "Hello|How are you?|Nice to meet you|Testing|Ping|Pong|What's up?|Good day|All set|Ready|"
                + "Acknowledged|Confirming|Thanks|Cheers|Let’s go|Checking in|Quick note|FYI|Update|Done|"
                + "Hello room|Streaming|Latency test|Throughput|Benchmark|Warmup|Client msg|Server echo|"
                + "Random text|Lorem ipsum|Edge case|Retrying|Backoff|Reconnect|Stable|OK|Fine|Yo|"
                + "Hey|Hi|Hola|Bonjour|Namaste|Konnichiwa|Annyeong|Guten Tag|Ciao|Hej";
        return base.split("\\|");
    }
}
