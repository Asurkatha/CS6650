#!/bin/bash

# Delete Old DynamoDB Tables
# Run this before setting up the new production schema

set -e

REGION=${AWS_REGION:-us-west-2}

echo "=========================================="
echo "Deleting Old DynamoDB Tables"
echo "=========================================="
echo "Region: $REGION"
echo ""
echo "⚠️  WARNING: This will delete all existing data!"
echo "Press Ctrl+C to cancel, or Enter to continue..."
read

# List of tables to delete
TABLES=(
    "Messages"
    "UserRoomStats"
    "HourlyMessageStats"
    "UserActivityBuckets"
    "DailyAggregates"
)

for TABLE in "${TABLES[@]}"; do
    echo ""
    echo "Deleting table: $TABLE"

    # Check if table exists
    if aws dynamodb describe-table --table-name $TABLE --region $REGION &>/dev/null; then
        aws dynamodb delete-table --table-name $TABLE --region $REGION
        echo "✓ Deletion initiated for $TABLE"

        # Wait for deletion
        echo "  Waiting for $TABLE to be deleted..."
        aws dynamodb wait table-not-exists --table-name $TABLE --region $REGION
        echo "✓ $TABLE deleted"
    else
        echo "⊘ Table $TABLE does not exist, skipping"
    fi
done

echo ""
echo "=========================================="
echo "✓ All old tables deleted!"
echo "=========================================="
echo ""
echo "Next steps:"
echo "  1. Run: ./setup-dynamodb-production.sh"
echo "  2. Deploy updated server code"
echo "  3. Run test"
echo ""
