#!/usr/bin/env bash
set -euo pipefail

ENDPOINT_URL="${AWS_ENDPOINT_URL:-http://localhost:4566}"
REGION="${AWS_DEFAULT_REGION:-us-east-1}"

echo "================================================================"
echo "  Bootstrapping LocalStack AWS Resources (CLI / Bash)           "
echo "  Target: ${ENDPOINT_URL} (${REGION})                           "
echo "================================================================"

export AWS_ACCESS_KEY_ID="test"
export AWS_SECRET_ACCESS_KEY="test"
export AWS_DEFAULT_REGION="${REGION}"

# Kinesis
echo "[*] Creating Kinesis stream: odysseus-sensor-stream..."
aws --endpoint-url="${ENDPOINT_URL}" kinesis create-stream \
    --stream-name odysseus-sensor-stream \
    --shard-count 2 || true

# S3
echo "[*] Creating S3 Buckets..."
aws --endpoint-url="${ENDPOINT_URL}" s3 mb s3://odysseus-telemetry-curated || true
aws --endpoint-url="${ENDPOINT_URL}" s3 mb s3://odysseus-telemetry-checkpoints || true
aws --endpoint-url="${ENDPOINT_URL}" s3 mb s3://odysseus-telemetry-dlq || true

# SQS DLQ & FIFO
echo "[*] Creating SQS Queues..."
DLQ_URL=$(aws --endpoint-url="${ENDPOINT_URL}" sqs create-queue \
    --queue-name odysseus-incident-alerts-dlq.fifo \
    --attributes FifoQueue=true,ContentBasedDeduplication=false \
    --query 'QueueUrl' --output text)

DLQ_ARN=$(aws --endpoint-url="${ENDPOINT_URL}" sqs get-queue-attributes \
    --queue-url "${DLQ_URL}" --attribute-names QueueArn \
    --query 'Attributes.QueueArn' --output text)

aws --endpoint-url="${ENDPOINT_URL}" sqs create-queue \
    --queue-name odysseus-incident-alerts.fifo \
    --attributes "FifoQueue=true,ContentBasedDeduplication=false,VisibilityTimeout=60,RedrivePolicy={\"deadLetterTargetArn\":\"${DLQ_ARN}\",\"maxReceiveCount\":\"3\"}" || true

# DynamoDB
echo "[*] Creating DynamoDB table: odysseus_alert_idempotency..."
aws --endpoint-url="${ENDPOINT_URL}" dynamodb create-table \
    --table-name odysseus_alert_idempotency \
    --attribute-definitions AttributeName=idempotency_key,AttributeType=S \
    --key-schema AttributeName=idempotency_key,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST || true

aws --endpoint-url="${ENDPOINT_URL}" dynamodb update-time-to-live \
    --table-name odysseus_alert_idempotency \
    --time-to-live-specification "Enabled=true,AttributeName=ttl" || true

# SNS
echo "[*] Creating SNS Topic..."
aws --endpoint-url="${ENDPOINT_URL}" sns create-topic --name odysseus-incident-topic || true

echo "[SUCCESS] LocalStack bootstrap complete."

