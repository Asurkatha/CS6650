#!/bin/bash

set -e

REGION=${AWS_REGION:-us-west-2}


echo "Checking if tables exist"
TABLES=$(aws dynamodb list-tables --region $REGION --query 'TableNames' --output text)

check_table() {
    TABLE=$1
    if echo "$TABLES" | grep -q "$TABLE"; then
        echo "✓ $TABLE exists"
        return 0
    else
        echo "✗ $TABLE does NOT exist"
        return 1
    fi
}

check_table "Messages"
check_table "UserRooms"
check_table "UserActivityBuckets"

echo "Checking table status"

check_status() {
    TABLE=$1
    STATUS=$(aws dynamodb describe-table --table-name $TABLE --region $REGION --query 'Table.TableStatus' --output text)
    echo "  $TABLE: $STATUS"

    if [ "$STATUS" != "ACTIVE" ]; then
        echo "    ⚠ Warning: Table is not ACTIVE"
    fi
}

check_status "Messages"
check_status "UserRooms"
check_status "UserActivityBuckets"

echo "Checking GSI on Messages table"
GSI=$(aws dynamodb describe-table --table-name Messages --region $REGION \
    --query 'Table.GlobalSecondaryIndexes[0].IndexName' --output text 2>/dev/null)

if [ "$GSI" == "GSI_UserMessages" ]; then
    echo "GSI_UserMessages exists"
else
    echo "GSI_UserMessages NOT found"
fi

echo "Checking IAM permissions from EC2"

if curl -s -f -m 2 http://169.254.169.254/latest/meta-data/iam/security-credentials/ > /dev/null 2>&1; then
    echo "Running on EC2 instance"
    ROLE=$(curl -s http://169.254.169.254/latest/meta-data/iam/security-credentials/)
    echo "IAM Role: $ROLE"

    echo "Testing DynamoDB access"
    if aws dynamodb describe-table --table-name Messages --region $REGION > /dev/null 2>&1; then
        echo "Can access DynamoDB tables"
    else
        echo "Cannot access DynamoDB tables - check IAM permissions"
    fi
else
    echo "IAM role check skipped"
fi


echo "Table Details:"
echo "Messages:"
aws dynamodb describe-table --table-name Messages --region $REGION \
    --query 'Table.[TableName,TableStatus,ItemCount,TableSizeBytes,BillingModeSummary.BillingMode]' \
    --output table

echo "UserRooms:"
aws dynamodb describe-table --table-name UserRooms --region $REGION \
    --query 'Table.[TableName,TableStatus,ItemCount,TableSizeBytes,BillingModeSummary.BillingMode]' \
    --output table

echo "UserActivityBuckets:"
aws dynamodb describe-table --table-name UserActivityBuckets --region $REGION \
    --query 'Table.[TableName,TableStatus,ItemCount,TableSizeBytes,BillingModeSummary.BillingMode]' \
    --output table

