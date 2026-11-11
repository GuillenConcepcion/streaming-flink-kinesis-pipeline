# -----------------------------------------------------------------------------
# AWS Kinesis Data Stream: High-Throughput Sensor Ingestion Tier
# -----------------------------------------------------------------------------

resource "aws_kinesis_stream" "sensor_stream" {
  name             = "${var.project_name}-sensor-stream"
  shard_count      = var.kinesis_shard_count
  retention_period = 24 # 24 hours retention for recovery buffer

  shard_level_metrics = [
    "IncomingBytes",
    "IncomingRecords",
    "OutgoingBytes",
    "OutgoingRecords",
    "WriteProvisionedThroughputExceeded",
    "ReadProvisionedThroughputExceeded",
    "IteratorAgeMilliseconds"
  ]

  stream_mode_details {
    stream_mode = "PROVISIONED"
  }
}

