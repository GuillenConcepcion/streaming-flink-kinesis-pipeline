output "kinesis_stream_arn" {
  description = "ARN of the sensor ingestion Kinesis Data Stream"
  value       = aws_kinesis_stream.sensor_stream.arn
}

output "kinesis_stream_name" {
  description = "Name of the sensor ingestion Kinesis Data Stream"
  value       = aws_kinesis_stream.sensor_stream.name
}

output "curated_s3_bucket" {
  description = "Amazon S3 bucket for curated telemetry datasets"
  value       = aws_s3_bucket.curated_bucket.id
}

output "checkpoints_s3_bucket" {
  description = "Amazon S3 bucket for Flink RocksDB checkpoints and savepoints"
  value       = aws_s3_bucket.checkpoints_bucket.id
}

output "sqs_alert_queue_url" {
  description = "URL of the Amazon SQS FIFO alert queue"
  value       = aws_sqs_queue.alert_queue.url
}

output "dynamodb_idempotency_table" {
  description = "Name of DynamoDB table storing SHA-256 deduplication tokens"
  value       = aws_dynamodb_table.alert_idempotency.name
}

output "sns_incident_topic_arn" {
  description = "ARN of Amazon SNS topic dispatching engineer notifications"
  value       = aws_sns_topic.incident_topic.arn
}

output "glue_database_name" {
  description = "Glue Catalog Database Name"
  value       = aws_glue_catalog_database.odysseus_db.name
}

output "athena_workgroup_name" {
  description = "Athena Workgroup for serverless interactive SQL"
  value       = aws_athena_workgroup.analytics_workgroup.name
}

output "cloudwatch_dashboard_name" {
  description = "Operational CloudWatch Dashboard Name"
  value       = aws_cloudwatch_dashboard.streaming_overview.dashboard_name
}

output "kinesis_lag_alarm_arn" {
  description = "CloudWatch Alarm ARN for Kinesis consumer lag"
  value       = aws_cloudwatch_metric_alarm.kinesis_iterator_age.arn
}


