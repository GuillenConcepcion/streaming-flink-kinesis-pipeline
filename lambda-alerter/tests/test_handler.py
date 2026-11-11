"""
Unit & Integration Tests for AWS Lambda Alerter.
Verifies the atomic DynamoDB SHA-256 deduplication barrier and SNS dispatching.
Supports both Moto (in-memory AWS mock) and unittest.mock fallback for zero-dependency execution.
"""

import json
import os
import sys
from unittest.mock import MagicMock
import pytest

# Add parent directory to path to import handler
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Set mock AWS environment variables
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
os.environ["DYNAMODB_IDEMPOTENCY_TABLE"] = "odysseus_alert_idempotency"
os.environ["SNS_TOPIC_ARN"] = "arn:aws:sns:us-east-1:123456789012:odysseus-incident-topic"
os.environ["DEDUPLICATION_TTL_SECONDS"] = "900"

import handler  # noqa: E402


class MockDynamoDBTable:
    """In-memory mock store simulating DynamoDB conditional write semantics."""
    def __init__(self):
        self.items = {}

    def put_item(self, TableName, Item, ConditionExpression=None):
        key = Item["idempotency_key"]["S"]
        if ConditionExpression and "attribute_not_exists" in ConditionExpression:
            if key in self.items:
                from botocore.exceptions import ClientError
                raise ClientError(
                    {"Error": {"Code": "ConditionalCheckFailedException", "Message": "The conditional request failed"}},
                    "PutItem"
                )
        self.items[key] = Item
        return {"ResponseMetadata": {"HTTPStatusCode": 200}}

    def get_item(self, TableName, Key):
        key = Key["idempotency_key"]["S"]
        if key in self.items:
            return {"Item": self.items[key]}
        return {}


@pytest.fixture(autouse=True)
def setup_mock_aws_clients():
    """Sets up lightweight in-memory mocks for DynamoDB and SNS."""
    mock_db = MockDynamoDBTable()
    mock_sns = MagicMock()
    mock_sns.publish.return_value = {"MessageId": "mock-sns-msg-12345"}

    orig_dynamodb = handler.dynamodb
    orig_sns = handler.sns

    handler.dynamodb = mock_db
    handler.sns = mock_sns

    yield mock_db, mock_sns

    handler.dynamodb = orig_dynamodb
    handler.sns = orig_sns


def test_atomic_idempotency_barrier(setup_mock_aws_clients):
    """Verifies that an idempotency key can only be claimed once in DynamoDB."""
    test_key = "a1b2c3d4e5f678901234567890abcdef1234567890abcdef1234567890abcdef"
    sensor_id = "TURBINE-001"
    pattern = "THERMAL_KINETIC_CATASTROPHE"

    # First attempt: Key is fresh, must succeed
    first_attempt = handler.record_idempotency_token(test_key, sensor_id, pattern)
    assert first_attempt is True, "First attempt to claim idempotency token must return True"

    # Second attempt: Duplicate replay, must be blocked by conditional check
    second_attempt = handler.record_idempotency_token(test_key, sensor_id, pattern)
    assert second_attempt is False, "Duplicate attempt with identical token must return False (suppressed)"


def test_lambda_handler_end_to_end_deduplication(setup_mock_aws_clients):
    """
    Verifies that when Flink replays a checkpoint producing duplicate SQS records,
    only EXACTLY ONE notification is published to Amazon SNS.
    """
    mock_db, mock_sns = setup_mock_aws_clients

    sample_alert = {
        "alert_id": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        "sensor_id": "TURBINE-042",
        "pattern_name": "THERMAL_KINETIC_CATASTROPHE",
        "detected_timestamp": 1794396030000,
        "trigger_temperature": 91.2,
        "trigger_vibration": 5.4,
        "severity": "CRITICAL",
        "details": "Temperature spike exceeded 85C followed by vibration > 4.5 mm/s within 12s"
    }

    # Simulate SQS FIFO event containing the initial alert AND an immediate checkpoint replay
    sqs_event = {
        "Records": [
            {
                "messageId": "msg-001",
                "body": json.dumps(sample_alert)
            },
            {
                "messageId": "msg-002-replay-duplicate",
                "body": json.dumps(sample_alert)  # Identical payload & alert_id
            }
        ]
    }

    # Execute Lambda handler
    response = handler.lambda_handler(sqs_event, None)

    assert response["statusCode"] == 200
    assert len(response["batchItemFailures"]) == 0, "No batch item failures should occur on valid/suppressed records"

    # Verify that SNS was called EXACTLY ONCE despite 2 incoming records in batch
    assert mock_sns.publish.call_count == 1, (
        f"Expected exactly 1 SNS publication, but received {mock_sns.publish.call_count}"
    )

    # Verify that the item was stored in DynamoDB
    db_item = mock_db.get_item(
        TableName=handler.DYNAMODB_TABLE,
        Key={"idempotency_key": {"S": sample_alert["alert_id"]}}
    )
    assert "Item" in db_item
    assert db_item["Item"]["sensor_id"]["S"] == "TURBINE-042"


def test_lambda_handler_handles_malformed_payload(setup_mock_aws_clients):
    """Verifies that malformed payloads lacking an alert_id are safely skipped."""
    mock_db, mock_sns = setup_mock_aws_clients

    sqs_event = {
        "Records": [
            {
                "messageId": "msg-corrupted",
                "body": json.dumps({"raw_corrupt": "data_without_alert_id"})
            }
        ]
    }

    response = handler.lambda_handler(sqs_event, None)
    assert response["statusCode"] == 200
    assert len(response["batchItemFailures"]) == 0
    assert mock_sns.publish.call_count == 0
