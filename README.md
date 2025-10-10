# CS6650

# ChatSever — CS6650 Assignment 1


---


## Project Layout

```

CS6650/
├── server/                 
│   └── target/chatflow-server.jar
├── client-part1/           
│   └── target/chatflow-client-part1.jar
├── client-part2/          
│   └── target/chatflow-client-part2.jar
└── results/                

````

---

## Prerequisites

- **JDK 17+** (`java -version`)
- **Maven 3.8+** (`mvn -version`)
- (Optional) **wscat** (`npm i -g wscat`) for quick WS tests

All runnable JARs are shaded (dependencies included).

---

## Build

From repo root:
 Build all modules
* `mvn -q clean package`

# Or build only one module (and its deps)
* `mvn -q -pl server -am clean package`
* `mvn -q -pl client-part1 -am clean package`
* `mvn -q -pl client-part2 -am clean package`


Artifacts:

* `server/target/chatflow-server.jar`
* `client-part1/target/chatflow-client-part1.jar`
* `client-part2/target/chatflow-client-part2.jar`

---

## Run the Server

### Local

```bash
java -jar server/target/chatflow-server.jar
```

### AWS EC2

1. Launch a **t2.micro** in **us-west-2** (free tier).
2. Security Group inbound:

    * **TCP 8080** (your IP or 0.0.0.0/0 for testing)
    * **TCP 22** (SSH)
3. Copy & run:

   ```bash
   scp -i your.pem server/target/chatflow-server.jar ec2-user@EC2_PUBLIC_IP:~
   ssh -i your.pem ec2-user@EC2_PUBLIC_IP
   nohup java -jar chatflow-server.jar #nohup to keep it running after logout
   ```

### Endpoints

* WebSocket: `ws://<host>:8080/chat/{roomId}`
* Health: `http://<host>:8081/health` → `{"status":"UP","serverTime":"..."}`

---

## Quick WebSocket Smoke Test

```bash
wscat -c ws://localhost:8080/chat/1
# Send a valid JSON
>{"userId":"123","username":"user123","message":"hi","timestamp":"2025-01-01T00:00:00Z","messageType":"TEXT"}
# Expect: {"status":"OK","serverTimestamp":"..."}
```

---

## Run the Clients

Both clients run a **Warmup** (32×1000) and a **Main** phase.
All timings are **ACK-based end-to-end** (first send → last ACK).

### Part 1 (throughput summary)

```bash
java -jar client-part1/target/chatflow-client-part1.jar <wsBase> [warmThreads] [warmMsgsPerThread] [mainThreads] [mainTotalMessages]
```

Defaults: `warmThreads=32`, `warmMsgsPerThread=1000`, `mainThreads=50`, `mainTotalMessages=500000`

Examples:

```bash
java -jar client-part1/target/chatflow-client-part1.jar ws://localhost:8080 32 1000 50 500000
java -jar client-part1/target/chatflow-client-part1.jar ws://EC2_PUBLIC_IP:8080 32 1000 50 500000
```

### Part 2 (metrics + chart)

```bash
java -jar client-part2/target/chatflow-client-part2.jar <wsBase> [warmThreads] [warmMsgsPerThread] [mainThreads] [mainTotalMessages]
```

Same defaults as Part 1.

---

## Outputs (Part 2)

* **Console summary** (per phase):

    * ACKs Ok, Sends Ok/Failed
    * Runtime (send → last ACK)
    * Throughput (ACKs / e2e)
    * Connections, Reconnections
    * Latency stats: mean, median, p95, p99, min, max
    * Message type distribution
    * Throughput per room
* **`metrics.csv`** — per-ACK rows (clean, single-writer):

  ```
  timestamp,messageType,latencyMs,statusCode,roomId
  ```


---

## Handy Commands

```bash
# Server (local)
java -jar server/target/chatflow-server.jar

# Client part 1 (local)
java -jar client-part1/target/chatflow-client-part1.jar ws://localhost:8080 32 1000 50 500000

# Client part 2 (EC2)
java -jar client-part2/target/chatflow-client-part2.jar ws://EC2_PUBLIC_IP:8080 32 1000 50 500000
```
---
