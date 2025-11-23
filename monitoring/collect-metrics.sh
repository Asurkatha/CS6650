#!/bin/bash

# Performance Metrics Collection Script
# Usage: ./collect-metrics.sh <test-start-time> <test-end-time> [region]
#
# Example:
#   ./collect-metrics.sh "2025-11-22T19:00:00Z" "2025-11-22T20:00:00Z" us-west-2

if [ $# -lt 2 ]; then
    echo "Usage: $0 <test-start-time> <test-end-time> [region]"
    echo "Example: $0 '2025-11-22T19:00:00Z' '2025-11-22T20:00:00Z' us-west-2"
    exit 1
fi

TEST_START="$1"
TEST_END="$2"
REGION="${3:-us-west-2}"

OUTPUT_DIR="metrics-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$OUTPUT_DIR"

echo "=========================================="
echo "Performance Metrics Collection"
echo "=========================================="
echo "Test Period: $TEST_START to $TEST_END"
echo "Region: $REGION"
echo "Output Directory: $OUTPUT_DIR"
echo ""

# Function to save metric to file
save_metric() {
    local metric_name="$1"
    local output_file="$2"
    shift 2
    echo "Collecting: $metric_name..."
    "$@" > "$OUTPUT_DIR/$output_file" 2>&1
    echo "  ✓ Saved to $output_file"
}

# ===========================================
# DynamoDB Metrics
# ===========================================
echo ""
echo "=== DynamoDB Metrics ==="

save_metric "DynamoDB Write Capacity (Messages)" "ddb-write-capacity-messages.json" \
aws cloudwatch get-metric-statistics \
  --namespace AWS/DynamoDB \
  --metric-name ConsumedWriteCapacityUnits \
  --dimensions Name=TableName,Value=Messages \
  --start-time "$TEST_START" \
  --end-time "$TEST_END" \
  --period 60 \
  --statistics Average,Maximum,Sum \
  --region "$REGION"

save_metric "DynamoDB Write Capacity (UserRoomStats)" "ddb-write-capacity-userrooms.json" \
aws cloudwatch get-metric-statistics \
  --namespace AWS/DynamoDB \
  --metric-name ConsumedWriteCapacityUnits \
  --dimensions Name=TableName,Value=UserRoomStats \
  --start-time "$TEST_START" \
  --end-time "$TEST_END" \
  --period 60 \
  --statistics Average,Maximum,Sum \
  --region "$REGION"

save_metric "DynamoDB Write Latency (Messages)" "ddb-write-latency-messages.json" \
aws cloudwatch get-metric-statistics \
  --namespace AWS/DynamoDB \
  --metric-name SuccessfulRequestLatency \
  --dimensions Name=TableName,Value=Messages Name=Operation,Value=PutItem \
  --start-time "$TEST_START" \
  --end-time "$TEST_END" \
  --period 60 \
  --statistics Average,Maximum,Minimum \
  --region "$REGION"

save_metric "DynamoDB Throttle Events (Messages)" "ddb-throttles-messages.json" \
aws cloudwatch get-metric-statistics \
  --namespace AWS/DynamoDB \
  --metric-name UserErrors \
  --dimensions Name=TableName,Value=Messages \
  --start-time "$TEST_START" \
  --end-time "$TEST_END" \
  --period 60 \
  --statistics Sum \
  --region "$REGION"

save_metric "DynamoDB System Errors (Messages)" "ddb-system-errors-messages.json" \
aws cloudwatch get-metric-statistics \
  --namespace AWS/DynamoDB \
  --metric-name SystemErrors \
  --dimensions Name=TableName,Value=Messages \
  --start-time "$TEST_START" \
  --end-time "$TEST_END" \
  --period 60 \
  --statistics Sum \
  --region "$REGION"

# ===========================================
# EC2 Server Metrics
# ===========================================
echo ""
echo "=== EC2 Server Metrics ==="

# Get list of all running instances with tag ChatFlow-Server
INSTANCE_IDS=$(aws ec2 describe-instances \
  --filters "Name=instance-state-name,Values=running" \
  --query 'Reservations[].Instances[].InstanceId' \
  --output text \
  --region "$REGION")

if [ -z "$INSTANCE_IDS" ]; then
    echo "WARNING: No running EC2 instances found."
    echo "Please provide instance IDs manually or check your filters."
else
    echo "Found instances: $INSTANCE_IDS"

    for instance_id in $INSTANCE_IDS; do
        echo ""
        echo "Collecting metrics for instance: $instance_id"

        save_metric "CPU Utilization ($instance_id)" "ec2-cpu-$instance_id.json" \
        aws cloudwatch get-metric-statistics \
          --namespace AWS/EC2 \
          --metric-name CPUUtilization \
          --dimensions Name=InstanceId,Value="$instance_id" \
          --start-time "$TEST_START" \
          --end-time "$TEST_END" \
          --period 60 \
          --statistics Average,Maximum,Minimum \
          --region "$REGION"

        save_metric "Network In ($instance_id)" "ec2-network-in-$instance_id.json" \
        aws cloudwatch get-metric-statistics \
          --namespace AWS/EC2 \
          --metric-name NetworkIn \
          --dimensions Name=InstanceId,Value="$instance_id" \
          --start-time "$TEST_START" \
          --end-time "$TEST_END" \
          --period 60 \
          --statistics Sum,Average \
          --region "$REGION"

        save_metric "Network Out ($instance_id)" "ec2-network-out-$instance_id.json" \
        aws cloudwatch get-metric-statistics \
          --namespace AWS/EC2 \
          --metric-name NetworkOut \
          --dimensions Name=InstanceId,Value="$instance_id" \
          --start-time "$TEST_START" \
          --end-time "$TEST_END" \
          --period 60 \
          --statistics Sum,Average \
          --region "$REGION"
    done
fi

# ===========================================
# Load Balancer Configuration
# ===========================================
echo ""
echo "=== Load Balancer Configuration ==="

save_metric "Target Groups" "lb-target-groups.json" \
aws elbv2 describe-target-groups --region "$REGION"

# Get first target group ARN (adjust if you have multiple)
TARGET_GROUP_ARN=$(aws elbv2 describe-target-groups \
  --region "$REGION" \
  --query 'TargetGroups[0].TargetGroupArn' \
  --output text)

if [ "$TARGET_GROUP_ARN" != "None" ] && [ -n "$TARGET_GROUP_ARN" ]; then
    save_metric "Target Health" "lb-target-health.json" \
    aws elbv2 describe-target-health \
      --target-group-arn "$TARGET_GROUP_ARN" \
      --region "$REGION"

    save_metric "Target Group Attributes" "lb-target-group-attributes.json" \
    aws elbv2 describe-target-group-attributes \
      --target-group-arn "$TARGET_GROUP_ARN" \
      --region "$REGION"
fi

# ===========================================
# DynamoDB Table Info
# ===========================================
echo ""
echo "=== DynamoDB Table Configuration ==="

for table in Messages UserRoomStats HourlyMessageStats; do
    save_metric "Table Config ($table)" "ddb-config-$table.json" \
    aws dynamodb describe-table \
      --table-name "$table" \
      --region "$REGION"
done

# ===========================================
# Generate Summary Report
# ===========================================
echo ""
echo "=== Generating Summary Report ==="

cat > "$OUTPUT_DIR/SUMMARY.md" << 'EOF'
# Performance Metrics Summary

## Test Details
- Start Time: TEST_START_PLACEHOLDER
- End Time: TEST_END_PLACEHOLDER
- Region: REGION_PLACEHOLDER

## Files Generated

### DynamoDB Metrics
- `ddb-write-capacity-messages.json` - Write capacity consumption
- `ddb-write-latency-messages.json` - Write latency statistics
- `ddb-throttles-messages.json` - Throttle events (should be 0)
- `ddb-system-errors-messages.json` - System errors (should be 0)

### EC2 Server Metrics
- `ec2-cpu-<instance>.json` - CPU utilization per server
- `ec2-network-in-<instance>.json` - Network inbound traffic
- `ec2-network-out-<instance>.json` - Network outbound traffic

### Load Balancer
- `lb-target-groups.json` - Target group configuration
- `lb-target-health.json` - Health status of all targets
- `lb-target-group-attributes.json` - Stickiness and algorithm settings

### DynamoDB Configuration
- `ddb-config-Messages.json` - Messages table configuration
- `ddb-config-UserRoomStats.json` - UserRoomStats table configuration
- `ddb-config-HourlyMessageStats.json` - HourlyMessageStats table configuration

## How to Analyze

### Calculate Average Write Throughput
```bash
# From ddb-write-capacity-messages.json
cat ddb-write-capacity-messages.json | jq '.Datapoints[].Average' | awk '{sum+=$1} END {print "Average WCU:", sum/NR}'
```

### Check for Throttles
```bash
# Should be 0
cat ddb-throttles-messages.json | jq '.Datapoints[].Sum' | awk '{sum+=$1} END {print "Total Throttles:", sum}'
```

### Get Peak CPU Usage
```bash
# For each server
for f in ec2-cpu-*.json; do
  echo "$f: $(cat $f | jq '.Datapoints[].Maximum' | sort -n | tail -1)%"
done
```

### Verify Load Distribution
```bash
# Check target health - all should be "healthy"
cat lb-target-health.json | jq '.TargetHealthDescriptions[].TargetHealth.State'
```

## Next Steps
1. Import CloudWatch data into Excel/Python for visualization
2. Generate graphs for queue depth, latency, throughput
3. Compare across baseline (500K), stress (1M), endurance (30min) tests
EOF

# Replace placeholders
sed -i "s/TEST_START_PLACEHOLDER/$TEST_START/g" "$OUTPUT_DIR/SUMMARY.md"
sed -i "s/TEST_END_PLACEHOLDER/$TEST_END/g" "$OUTPUT_DIR/SUMMARY.md"
sed -i "s/REGION_PLACEHOLDER/$REGION/g" "$OUTPUT_DIR/SUMMARY.md"

echo "  ✓ Saved to SUMMARY.md"

# ===========================================
# Create Quick Analysis Script
# ===========================================
cat > "$OUTPUT_DIR/analyze.sh" << 'EOF'
#!/bin/bash
# Quick analysis of collected metrics

echo "==========================================">
echo "Performance Metrics Quick Analysis"
echo "=========================================="

echo ""
echo "=== DynamoDB Write Performance ==="
echo "Average Write Capacity Units:"
cat ddb-write-capacity-messages.json | jq -r '.Datapoints[].Average' | \
  awk '{sum+=$1; count++} END {printf "  %.2f WCU/s (avg over %d samples)\n", sum/count, count}'

echo ""
echo "Peak Write Capacity Units:"
cat ddb-write-capacity-messages.json | jq -r '.Datapoints[].Maximum' | sort -n | tail -1 | \
  awk '{printf "  %.2f WCU/s (peak)\n", $1}'

echo ""
echo "Total Write Capacity Consumed:"
cat ddb-write-capacity-messages.json | jq -r '.Datapoints[].Sum' | \
  awk '{sum+=$1} END {printf "  %.0f WCU total\n", sum}'

echo ""
echo "=== DynamoDB Write Latency ==="
echo "Average Latency:"
cat ddb-write-latency-messages.json | jq -r '.Datapoints[].Average' | \
  awk '{sum+=$1; count++} END {printf "  %.2f ms (avg)\n", sum/count}'

echo "Peak Latency:"
cat ddb-write-latency-messages.json | jq -r '.Datapoints[].Maximum' | sort -n | tail -1 | \
  awk '{printf "  %.2f ms (max)\n", $1}'

echo ""
echo "=== Error Analysis ==="
THROTTLES=$(cat ddb-throttles-messages.json | jq -r '.Datapoints[].Sum' | awk '{sum+=$1} END {print sum}')
SYSTEM_ERRORS=$(cat ddb-system-errors-messages.json | jq -r '.Datapoints[].Sum' | awk '{sum+=$1} END {print sum}')

echo "Throttle Events: ${THROTTLES:-0}"
echo "System Errors: ${SYSTEM_ERRORS:-0}"

if [ "${THROTTLES:-0}" = "0" ] && [ "${SYSTEM_ERRORS:-0}" = "0" ]; then
    echo "  ✓ No errors detected!"
else
    echo "  ⚠ Errors detected - investigate throttling or capacity"
fi

echo ""
echo "=== EC2 Server Load ==="
for f in ec2-cpu-*.json; do
    instance=$(basename $f .json | cut -d'-' -f3-)
    avg=$(cat $f | jq -r '.Datapoints[].Average' | awk '{sum+=$1; count++} END {printf "%.1f", sum/count}')
    peak=$(cat $f | jq -r '.Datapoints[].Maximum' | sort -n | tail -1)
    echo "Instance $instance: avg=${avg}%, peak=${peak}%"
done

echo ""
echo "=== Load Balancer Health ==="
if [ -f lb-target-health.json ]; then
    cat lb-target-health.json | jq -r '.TargetHealthDescriptions[] | "Target: \(.Target.Id) - State: \(.TargetHealth.State)"'
fi

echo ""
echo "=========================================="
echo "Analysis complete!"
EOF

chmod +x "$OUTPUT_DIR/analyze.sh"

echo ""
echo "=========================================="
echo "✓ Metrics collection complete!"
echo "=========================================="
echo ""
echo "Output directory: $OUTPUT_DIR"
echo ""
echo "Next steps:"
echo "1. Review SUMMARY.md for file descriptions"
echo "2. Run ./analyze.sh for quick analysis"
echo "3. Import JSON files into Excel/Python for detailed graphs"
echo ""
echo "Example:"
echo "  cd $OUTPUT_DIR"
echo "  cat SUMMARY.md"
echo "  ./analyze.sh"
echo ""
