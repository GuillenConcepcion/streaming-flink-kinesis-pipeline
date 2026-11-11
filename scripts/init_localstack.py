#!/usr/bin/env python3
"""
Bootstrap AWS resources in LocalStack for the Flink & Kinesis streaming pipeline.
Idempotent script using boto3.
"""
import os
import sys
import time
import boto3
from botocore.config import Config

ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566")
REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

boto_config = Config(
    region_name=REGION,
    retries={"max_attempts": 5, "mode": "standard"}
)

kinesis = boto3.client("kinesis", endpoint_url=ENDPOINT_URL, config=boto_config,
                       aws_access_key_id="test", aws_secret_access_key="test")
s3 = boto3.client("s3", endpoint_url=ENDPOINT_URL, config=boto_config,
                  aws_access_key_id="test", aws_secret_access_key="test")
sqs = boto3.client("sqs", endpoint_url=ENDPOINT_URL, config=boto_config,
                   aws_access_key_id="test", aws_secret_access_key="test")
dynamodb = boto3.client("dynamodb", endpoint_url=ENDPOINT_URL, config=boto_config,
                        aws_access_key_id="test", aws_secret_access_key="test")
sns = boto3.client("sns", endpoint_url=ENDPOINT_URL, config=boto_config,
                   aws_access_key_id="test", aws_secret_access_key="test")


def init_kinesis():
    stream_name = "odysseus-sensor-stream"
    print(f"[*] Initializing Kinesis Stream: {stream_name}")
    try:
        streams = kinesis.list_streams().get("StreamNames", [])
        if stream_name not in streams:
            kinesis.create_stream(StreamName=stream_name, ShardCount=2)
            print(f"    [+] Created stream '{stream_name}' with 2 shards.")
        else:
            print(f"    [=] Stream '{stream_name}' already exists.")
    except Exception as e:
        print(f"    [!] Error initializing Kinesis: {e}")


def init_s3():
    buckets = [
        "odysseus-telemetry-curated",
        "odysseus-telemetry-checkpoints",
        "odysseus-telemetry-dlq"
    ]
    print("[*] Initializing S3 Buckets...")
    existing = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
    for bucket in buckets:
        if bucket not in existing:
            s3.create_bucket(Bucket=bucket)
            print(f"    [+] Created bucket '{bucket}'")
        else:
            print(f"    [=] Bucket '{bucket}' already exists.")


def init_sqs():
    print("[*] Initializing SQS FIFO Queues...")
    dlq_name = "odysseus-incident-alerts-dlq.fifo"
    queue_name = "odysseus-incident-alerts.fifo"
    
    # 1. Create DLQ
    dlq_resp = sqs.create_queue(
        QueueName=dlq_name,
        Attributes={
            "FifoQueue": "true",
            "ContentBasedDeduplication": "false"
        }
    )
    dlq_url = dlq_resp["QueueUrl"]
    dlq_arn = sqs.get_queue_attributes(QueueUrl=dlq_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    print(f"    [+] SQS DLQ ready: {dlq_name} ({dlq_arn})")

    # 2. Create Main Queue with RedrivePolicy
    redrive_policy = f'{{"deadLetterTargetArn":"{dlq_arn}","maxReceiveCount":"3"}}'
    sqs_resp = sqs.create_queue(
        QueueName=queue_name,
        Attributes={
            "FifoQueue": "true",
            "ContentBasedDeduplication": "false",
            "VisibilityTimeout": "60",
            "RedrivePolicy": redrive_policy
        }
    )
    print(f"    [+] SQS FIFO Queue ready: {queue_name} -> {sqs_resp['QueueUrl']}")


def init_dynamodb():
    table_name = "odysseus_alert_idempotency"
    print(f"[*] Initializing DynamoDB Table: {table_name}")
    try:
        tables = dynamodb.list_tables().get("TableNames", [])
        if table_name not in tables:
            dynamodb.create_table(
                TableName=table_name,
                KeySchema=[{"AttributeName": "idempotency_key", "KeyType": "HASH"}],
                AttributeDefinitions=[{"AttributeName": "idempotency_key", "AttributeType": "S"}],
                BillingMode="PAY_PER_REQUEST"
            )
            print(f"    [+] Created table '{table_name}'")
            
            # Enable TTL
            time.sleep(1)
            dynamodb.update_time_to_live(
                TableName=table_name,
                TimeToLiveSpecification={
                    "Enabled": True,
                    "AttributeName": "ttl"
                }
            )
            print(f"    [+] Enabled TTL on attribute 'ttl'")
        else:
            print(f"    [=] Table '{table_name}' already exists.")
    except Exception as e:
        print(f"    [!] Error initializing DynamoDB: {e}")


def init_sns():
    topic_name = "odysseus-incident-topic"
    print(f"[*] Initializing SNS Topic: {topic_name}")
    try:
        resp = sns.create_topic(Name=topic_name)
        print(f"    [+] Topic ready: {resp.get('TopicArn')}")
    except Exception as e:
        print(f"    [!] Error initializing SNS: {e}")


if __name__ == "__main__":
    print("================================================================")
    print("  Bootstrapping LocalStack AWS Infrastructure for Odysseus       ")
    print(f"  Target Endpoint: {ENDPOINT_URL} ({REGION})                   ")
    print("================================================================")
    init_kinesis()
    init_s3()
    init_sqs()
    init_dynamodb()
    init_sns()
    print("[SUCCESS] All LocalStack resources initialized successfully.")

