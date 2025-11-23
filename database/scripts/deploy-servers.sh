#!/bin/bash

# Deploy updated server JAR to all 4 EC2 instances
# Usage: ./deploy-servers.sh <key-file.pem> <server1-ip> <server2-ip> <server3-ip> <server4-ip>

set -e

if [ "$#" -ne 5 ]; then
    echo "Usage: $0 <key-file.pem> <server1-ip> <server2-ip> <server3-ip> <server4-ip>"
    exit 1
fi

KEY_FILE=$1
SERVER1=$2
SERVER2=$3
SERVER3=$4
SERVER4=$5

JAR_PATH="../server/target/chatflow-server.jar"

if [ ! -f "$JAR_PATH" ]; then
    echo "Error: JAR file not found at $JAR_PATH"
    echo "Run 'mvn clean package -DskipTests' first"
    exit 1
fi

echo "=========================================="
echo "Deploying to 4 servers"
echo "=========================================="
echo ""

SERVERS=("$SERVER1" "$SERVER2" "$SERVER3" "$SERVER4")

for i in "${!SERVERS[@]}"; do
    SERVER="${SERVERS[$i]}"
    NUM=$((i+1))

    echo "[$NUM/4] Deploying to $SERVER..."

    # Upload JAR
    echo "  Uploading JAR..."
    scp -i "$KEY_FILE" "$JAR_PATH" "ec2-user@$SERVER:~/chatflow-server.jar"

    # Stop old process and start new one
    echo "  Restarting server..."
    ssh -i "$KEY_FILE" "ec2-user@$SERVER" << 'EOF'
        pkill -f chatflow-server.jar || true
        sleep 2
        nohup java -jar ~/chatflow-server.jar > ~/server.log 2>&1 &
        sleep 2
        ps aux | grep chatflow-server | grep -v grep
EOF

    echo "  ✓ Server $NUM deployed"
    echo ""
done

echo "=========================================="
echo "✓ All 4 servers deployed!"
echo "=========================================="
echo ""
echo "Verify logs:"
for i in "${!SERVERS[@]}"; do
    SERVER="${SERVERS[$i]}"
    NUM=$((i+1))
    echo "  Server $NUM: ssh -i $KEY_FILE ec2-user@$SERVER 'tail -f ~/server.log'"
done
echo ""
