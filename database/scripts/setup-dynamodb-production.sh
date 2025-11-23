#!/bin/bash


set -e

REGION=${AWS_REGION:-us-west-2}


wait_for_table() {
    TABLE_NAME=$1
    echo "Waiting for $TABLE_NAME to be ACTIVE..."
    aws dynamodb wait table-exists --table-name $TABLE_NAME --region $REGION
    echo "✓ $TABLE_NAME is ACTIVE"
}


aws dynamodb create-table \
    --table-name Messages \
    --attribute-definitions \
        AttributeName=testId_roomId,AttributeType=S \
        AttributeName=msgTimestamp,AttributeType=S \
        AttributeName=testId_userId,AttributeType=S \
    --key-schema \
        AttributeName=testId_roomId,KeyType=HASH \
        AttributeName=msgTimestamp,KeyType=RANGE \
    --billing-mode PAY_PER_REQUEST \
    --global-secondary-indexes \
        "[{
            \"IndexName\": \"GSI_TestUser\",
            \"KeySchema\": [
                {\"AttributeName\":\"testId_userId\",\"KeyType\":\"HASH\"},
                {\"AttributeName\":\"msgTimestamp\",\"KeyType\":\"RANGE\"}
            ],
            \"Projection\": {\"ProjectionType\":\"ALL\"}
        }]" \
    --region $REGION 2>/dev/null || echo "Messages table exists"

wait_for_table Messages

# Enable 60-minute TTL
aws dynamodb update-time-to-live \
    --table-name Messages \
    --time-to-live-specification "Enabled=true, AttributeName=ttl" \
    --region $REGION 2>/dev/null || echo "TTL enabled"



aws dynamodb create-table \
    --table-name UserRoomStats \
    --attribute-definitions \
        AttributeName=testId_userId,AttributeType=S \
        AttributeName=roomId,AttributeType=S \
        AttributeName=testId,AttributeType=S \
        AttributeName=messageCount,AttributeType=N \
    --key-schema \
        AttributeName=testId_userId,KeyType=HASH \
        AttributeName=roomId,KeyType=RANGE \
    --billing-mode PAY_PER_REQUEST \
    --global-secondary-indexes \
        "[
            {
                \"IndexName\": \"GSI_TestTopUsers\",
                \"KeySchema\": [
                    {\"AttributeName\":\"testId\",\"KeyType\":\"HASH\"},
                    {\"AttributeName\":\"messageCount\",\"KeyType\":\"RANGE\"}
                ],
                \"Projection\": {\"ProjectionType\":\"ALL\"}
            },
            {
                \"IndexName\": \"GSI_RoomTopUsers\",
                \"KeySchema\": [
                    {\"AttributeName\":\"roomId\",\"KeyType\":\"HASH\"},
                    {\"AttributeName\":\"messageCount\",\"KeyType\":\"RANGE\"}
                ],
                \"Projection\": {\"ProjectionType\":\"KEYS_ONLY\"}
            }
        ]" \
    --region $REGION 2>/dev/null || echo "UserRoomStats table exists"

wait_for_table UserRoomStats

aws dynamodb update-time-to-live \
    --table-name UserRoomStats \
    --time-to-live-specification "Enabled=true, AttributeName=ttl" \
    --region $REGION 2>/dev/null || echo "TTL enabled"

aws dynamodb create-table \
    --table-name HourlyMessageStats \
    --attribute-definitions \
        AttributeName=testId,AttributeType=S \
        AttributeName=hourBucket,AttributeType=S \
    --key-schema \
        AttributeName=testId,KeyType=HASH \
        AttributeName=hourBucket,KeyType=RANGE \
    --billing-mode PAY_PER_REQUEST \
    --region $REGION 2>/dev/null || echo "HourlyMessageStats table exists"

wait_for_table HourlyMessageStats

aws dynamodb update-time-to-live \
    --table-name HourlyMessageStats \
    --time-to-live-specification "Enabled=true, AttributeName=ttl" \
    --region $REGION 2>/dev/null || echo "TTL enabled"

