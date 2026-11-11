#!/usr/bin/env python3
"""
AWS Lambda Alerter with Atomic DynamoDB Deduplication.
Part of Proyecto Odysseus - Streaming MLOps Pipeline.

Consumes SQS FIFO alert records emitted by Apache Flink CEP.
Protects downstream operators and on-call engineers against alert storms
and duplicate notifications during Flink checkpoint recovery.
"""

import json
import logging
import os
import time
from typing import Dict, Any, List

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL", None)
REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
DYNAMODB_TABLE = os.getenv("DYNAMODB_IDEMPOTENCY_TABLE", "odysseus_alert_idempotency")
SNS_TOPIC_ARN = os.getenv("SNS_TOPIC_ARN", "arn:aws:sns:us-east-1:000000000000:odysseus-incident-topic")
TTL_WINDOW_SECONDS = int(os.getenv("DEDUPLICATION_TTL_SECONDS", "900"))  # 15 minutes

boto_config = Config(region_name=REGION)
dynamodb = boto3.client("dynamodb", endpoint_url=ENDPOINT_URL, config=boto_config)
sns = boto3.client("sns", endpoint_url=ENDPOINT_URL, config=boto_config)


def record_idempotency_token(alert_id: str, sensor_id: str, pattern: str) -> bool:
    """
    Attempts to atomically claim the SHA-256 idempotency key in DynamoDB.
    Returns True if key was successfully acquired (first time seen).
    Returns False if key already exists (duplicate/replay).
    """
    now_epoch = int(time.time())
    ttl_epoch = now_epoch + TTL_WINDOW_SECONDS

    try:
        dynamodb.put_item(
            TableName=DYNAMODB_TABLE,
            Item={
                "idempotency_key": {"S": alert_id},
                "sensor_id": {"S": sensor_id},
                "pattern_name": {"S": pattern},
                "created_at": {"N": str(now_epoch)},
                "ttl": {"N": str(ttl_epoch)}
            },
            ConditionExpression="attribute_not_exists(idempotency_key)"
        )
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            logger.warning(f"🔁 [DEDUPLICATION BLOCKED] Duplicate alert detected (Key: {alert_id}). Suppressing alert.")
            return False
        logger.error(f"DynamoDB unexpected error: {e}")
        raise e


def publish_sns_notification(alert: Dict[str, Any]):
    """Dispatches a formatted incident alert to Amazon SNS."""
    sensor_id = alert.get("sensor_id", "UNKNOWN")
    pattern = alert.get("pattern_name", "ANOMALY")
    severity = alert.get("severity", "CRITICAL")
    details = alert.get("details", "No details provided")
    detected_ts = alert.get("detected_timestamp", 0)

    subject = f"🚨 [{severity}] Odysseus IoT Incident: {sensor_id} ({pattern})"
    message_body = (
        f"=== PROYECTO ODYSSEUS - REAL-TIME INCIDENT NOTIFICATION ===\n"
        f"Severity:           {severity}\n"
        f"Target Asset:       {sensor_id}\n"
        f"Pattern Matched:    {pattern}\n"
        f"Event Timestamp:    {detected_ts}\n"
        f"Diagnostic Summary: {details}\n"
        f"Idempotency Token:  {alert.get('alert_id')}\n"
        f"Engine:             Apache Flink 1.18 CEP\n"
        f"============================================================"
    )

    resp = sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=subject[:100],  # AWS SNS subject limit
        Message=message_body
    )
    logger.info(f"📣 [SNS PUBLISHED] MessageId: {resp.get('MessageId')} for Sensor: {sensor_id}")


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """SQS FIFO Event Source Mapping Handler with batch item failure reporting."""
    records: List[Dict[str, Any]] = event.get("Records", [])
    logger.info(f"Processing SQS batch of {len(records)} records...")

    batch_item_failures = []

    for record in records:
        message_id = record.get("messageId")
        try:
            body_str = record.get("body", "{}")
            alert = json.loads(body_str)

            alert_id = alert.get("alert_id")
            sensor_id = alert.get("sensor_id", "UNKNOWN")
            pattern = alert.get("pattern_name", "UNKNOWN_PATTERN")

            if not alert_id:
                logger.error(f"Malformed alert payload missing alert_id: {body_str}")
                continue

            # Check idempotency barrier
            is_unique = record_idempotency_token(alert_id, sensor_id, pattern)
            if is_unique:
                publish_sns_notification(alert)
            else:
                logger.info(f"Suppressed duplicate notification for alert_id {alert_id}")

        except Exception as e:
            logger.error(f"Error processing record {message_id}: {e}", exc_info=True)
            batch_item_failures.append({"itemIdentifier": message_id})

    return {
        "statusCode": 200,
        "batchItemFailures": batch_item_failures
    }

