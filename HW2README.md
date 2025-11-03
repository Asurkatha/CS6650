# ChatFlow HW2 README

## Overview

ChatFlow HW2 demonstrates a horizontally scalable chat backend powered by RabbitMQ, an AWS Application Load Balancer (ALB), and a Java load generator. The deliverables include WebSocket servers, a high‑throughput client, deployment artifacts, and instrumentation that satisfies the HW2 requirements.

## Repository Layout

```
HW1/CS6650/
├── client-part1/        # HW2 load generator (primary client)
├── client-part2/        # Earlier HW client (kept for reference)
├── server/              # WebSocket + RabbitMQ backend
├── results/             # Run reports, architecture/class diagrams
└── HW2README.md         # This document
```

Key diagrams and run reports live in `results/`:
- `HW2ArchDiag.png` – deployment / component view
- `HW2ClassDiag.png` – core class interaction
- `run-*.json` – per run client metrics and optional RabbitMQ stats

## Architecture Summary

| Component | Responsibilities |
|-----------|------------------|
| `client-part1` | Spawns configurable sender threads, enforces max in-flight messages, maintains per-room WebSocket connections, tracks ACKs, retries, and throughput. |
| `server` | Accepts WebSocket connections, validates messages, publishes them to RabbitMQ, consumes per-room queues, broadcasts to active sockets, and emits periodic `[METRICS]` logs. |
| RabbitMQ | Durable messaging backbone with publisher confirms, per-room queues, and management plugin for monitoring. |
| AWS ALB + EC2 | Load balances WebSocket traffic across multiple server instances. Health checks hit a lightweight HTTP endpoint. |
| Observability | `MetricsTracker` + `QueueStatsTracker` aggregate server metrics; client writes run reports; RabbitMQ management UI offers queue and rate telemetry. |

Canonical server metric line (every 5s):
```
[METRICS] Recv: 126896 (0/s) | Pub: 126896 (0/s) | Consume: 125057 (0/s) |
Broadcast: 125057 (0/s) | Confirmed: 126896 | Lag: 1839 | Dupes: 9 (+0/s) |
Failures: 0 (+0/s) | Retries: 0 (+0/s) | QueueDepth: latest=0 avg=0.0 peak=0 |
Connections: 0 | Rooms: 0
```

## Build Instructions

Prerequisites: Java 17+, Maven 3.8+, RabbitMQ 3.x, AWS access for EC2/ALB, SSH keys (`SC.pem` for EC2 instances, `RMQ` key for RabbitMQ host).

```bash
# Build server jar
cd server
mvn clean package

# Build client jar
cd ../client-part1
mvn clean package
```

Outputs:
- `server/target/chatflow-server.jar`
- `client-part1/target/chatflow-client-part1.jar`

> Maven warns about duplicate `gson` declarations because the client shades dependencies; the build succeeds.

## RabbitMQ Setup

1. SSH into the broker host:
   ```bash
   ssh -i RMQ ubuntu@elasticip
   ```
2. Enable and start server/management plugin:
   ```bash
   sudo systemctl enable rabbitmq-server
   sudo systemctl start rabbitmq-server
   sudo rabbitmq-plugins enable rabbitmq_management
   ```
3. (Optional) Create admin user:
   ```bash
   sudo rabbitmqctl add_user asurkatha SecurePassword123
   sudo rabbitmqctl set_user_tags asurkatha administrator
   sudo rabbitmqctl set_permissions -p / asurkatha ".*" ".*" ".*"
   ```
4. Access management console at `http://<elasticip>:15672`.

## Server Deployment

### Local
```bash
cd server/target
java \
 "-DRABBIT_HOST=54.214.193.178" \
 "-DRABBIT_PORT=5672" \
 "-DRABBIT_USER=asurkatha" \
 "-DRABBIT_PASS=SecurePassword123" \
 "-DWS_PORT=8080" \
 "-DHTTP_PORT=8081" \
 -jar chatflow-server.jar
```

### Upload to EC2
```bash
scp -i SC.pem server/target/chatflow-server.jar ec2-user@44.254.156.147:/home/ec2-user/
```

### Run on EC2 (single instance)
```bash
java \
 -DRABBIT_HOST=54.214.193.178 \
 -DRABBIT_PORT=5672 \
 -DRABBIT_USER=asurkatha \
 -DRABBIT_PASS=SecurePassword123 \
 -jar chatflow-server.jar
```

### Multi-instance / ALB
- Launch several EC2 instances with the command above (wrap in user data or systemd service).
- Register them with the ALB (`ChatServers-752792592.us-west-2.elb.amazonaws.com`).
- ALB health checks target the HTTP server (`/health` on port `8081` by default).

## Client Execution

### Against ALB
```bash
cd client-part1/target
java -jar chatflow-client-part1.jar \
  ws://ChatServers-752792592.us-west-2.elb.amazonaws.com:80 \
  32 \
  500000
```

### Against Local Server
```bash
java -jar chatflow-client-part1.jar ws://localhost:8080 32 500000
```

Command format:
```
java -jar chatflow-client-part1.jar <ws-url> [threads=32] [totalMessages=500000] [maxInFlight=50000]
```

Each run prints progress (sends, ACKs, retries) and writes a JSON report to `client-part1/results/`.

## Observability and Reporting

- **Server metrics**: `[METRICS]` log every 5 seconds (throughput, confirmations, retries, queue depth).
- **Client metrics**: Console summary + JSON run report (`/results/run-*.json`) capturing throughput, runtime, retries, connection failures; optionally collects RabbitMQ HTTP statistics when `-DRABBIT_HTTP_BASE/USER/PASS` system properties are supplied.
- **RabbitMQ UI**: Monitor queue depth, publish/consume rates, node health.
- **AWS CloudWatch**: Track EC2 CPU, network, ALB request counts; include screenshots in HW2 submission.
- **Diagrams**: `HW2ArchDiag.png` and `HW2ClassDiag.png` in `results/`.

## Typical Test Workflow

1. Provision RabbitMQ and enable management plugin.
2. Start 1+ EC2 instances running `chatflow-server.jar` (or run locally).
3. Register instances behind ALB; confirm `/health` passes.
4. Run load tests with `client-part1`.
5. Monitor `[METRICS]` logs, RabbitMQ dashboard, CloudWatch for resource usage.
6. Collect run reports and screenshots for documentation.

## Troubleshooting

| Issue | Diagnosis / Fix |
|-------|------------------|
| `ConnFail` increments immediately | Server not running or ALB targeting wrong port – verify EC2 process and security groups. |
| Retries climb, ACKs stay 0 | Server closes sockets (RabbitMQ unreachable, crash, validation error). Check server logs and RabbitMQ credentials. |
| `TIMEOUT: … ACKs still pending` | Consumers lagging. Check `Lag` and `QueueDepth`, scale consumer threads or instances. |
| SLF4J replay warning | Benign; occurs when WebSocket library logs before backend init. To silence, invoke `LoggerFactory.getILoggerFactory();` in a static block. |
| Maven duplicate `gson` warning | Caused by shading; harmless for now, but can be resolved by unifying dependency declarations. |

## Deliverables Checklist

- [x] Server + client sources
- [x] Executable JARs (`chatflow-server.jar`, `chatflow-client-part1.jar`)
- [x] RabbitMQ + EC2/ALB deployment instructions
- [x] Architecture & class diagrams (`results/`)
- [x] Client run reports (`client-part1/results/`)
- [x] Comprehensive README (this file)

## Notes & Best Practices

- Secure RabbitMQ credentials using environment variables or AWS Secrets Manager in production.
- When testing 2/4-instance ALB setups, assign unique `-DSERVER_ID` values for easier log correlation.
- Monitor CloudWatch + RabbitMQ to capture throughput improvements as instances scale from single server to 2 and 4-node configurations.
- Stop RabbitMQ management plugin and scale-in EC2/ALB resources after testing to control costs.

## Commands Reference

### RabbitMQ
```bash
ssh -i RMQ ubuntu@elasticip
sudo systemctl enable rabbitmq-server
sudo systemctl start rabbitmq-server
sudo rabbitmq-plugins enable rabbitmq_management
```

### Server (local)
```bash
java "-DRABBIT_HOST=54.214.193.178" "-DRABBIT_PORT=5672" \
     "-DRABBIT_USER=asurkatha" "-DRABBIT_PASS=SecurePassword123" \
     "-DWS_PORT=8080" "-DHTTP_PORT=8081" \
     -jar target/chatflow-server.jar
```

### Server (EC2)
```bash
java -DRABBIT_HOST=54.214.193.178 -DRABBIT_PORT=5672 \
     -DRABBIT_USER=asurkatha -DRABBIT_PASS=SecurePassword123 \
     -jar chatflow-server.jar
```

### Client (ALB)
```bash
java -jar target/chatflow-client-part1.jar \
     ws://ChatServers-752792592.us-west-2.elb.amazonaws.com:80 \
     32 500000
```

### Client (local server)
```bash
java -jar target/chatflow-client-part1.jar ws://localhost:8080 32 500000
```

### Upload server JAR to EC2
```bash
scp -i SC.pem server/target/chatflow-server.jar \
    ec2-user@44.254.156.147:/home/ec2-user/
```

## Contact

For additional metrics, logs, or configuration snippets, rerun the tests noting any changes to threads, message volume, or JVM flags, then share the relevant logs or run reports. The current README should enable reproducible HW2 deployments end-to-end.
