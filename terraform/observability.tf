# -----------------------------------------------------------------------------
# Observability Tier: CloudWatch Metric Alarms & Pipeline Dashboard
# -----------------------------------------------------------------------------

# 1. Kinesis Iterator Age Alarm (Stream Consumer Lag)
resource "aws_cloudwatch_metric_alarm" "kinesis_iterator_age" {
  alarm_name          = "${var.project_name}-kinesis-high-iterator-age"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "GetRecords.IteratorAgeMilliseconds"
  namespace           = "AWS/Kinesis"
  period              = 60
  statistic           = "Maximum"
  threshold           = 30000 # 30 seconds consumer lag
  alarm_description   = "Triggered when Flink stream consumer falls behind Kinesis real-time edge by > 30s."
  alarm_actions       = [aws_sns_topic.incident_topic.arn]
  ok_actions          = [aws_sns_topic.incident_topic.arn]

  dimensions = {
    StreamName = aws_kinesis_stream.sensor_stream.name
  }
}

# 2. Kinesis Write Throughput Exceeded Alarm (Shard Saturation)
resource "aws_cloudwatch_metric_alarm" "kinesis_write_exceeded" {
  alarm_name          = "${var.project_name}-kinesis-write-exceeded"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "WriteProvisionedThroughputExceeded"
  namespace           = "AWS/Kinesis"
  period              = 60
  statistic           = "Sum"
  threshold           = 5
  alarm_description   = "Triggered when sensor ingest rate exceeds provisioned Kinesis shard capacity."
  alarm_actions       = [aws_sns_topic.incident_topic.arn]

  dimensions = {
    StreamName = aws_kinesis_stream.sensor_stream.name
  }
}

# 3. SQS Alert Dead-Letter Queue (DLQ) Visibility Alarm (Poison Pills)
resource "aws_cloudwatch_metric_alarm" "sqs_dlq_non_empty" {
  alarm_name          = "${var.project_name}-sqs-dlq-messages-detected"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ApproximateNumberOfMessagesVisible"
  namespace           = "AWS/SQS"
  period              = 60
  statistic           = "Maximum"
  threshold           = 0 # Trigger on any message in DLQ
  alarm_description   = "Critical: Alerts landed in SQS Dead-Letter Queue due to 3 consecutive Lambda processing failures."
  alarm_actions       = [aws_sns_topic.incident_topic.arn]

  dimensions = {
    QueueName = aws_sqs_queue.alert_dlq.name
  }
}

# 4. Pipeline Operational Overview Dashboard
resource "aws_cloudwatch_dashboard" "streaming_overview" {
  dashboard_name = "${var.project_name}-operational-overview"

  dashboard_body = jsonencode({
    widgets = [
      {
        type   = "metric"
        x      = 0
        y      = 0
        width  = 12
        height = 6
        properties = {
          metrics = [
            ["AWS/Kinesis", "IncomingRecords", "StreamName", aws_kinesis_stream.sensor_stream.name, { stat = "Sum", period = 60 }],
            [".", "IncomingBytes", ".", ".", { stat = "Sum", period = 60 }]
          ]
          view    = "timeSeries"
          stacked = false
          region  = var.aws_region
          title   = "Kinesis Ingestion Rate (Records & Bytes)"
        }
      },
      {
        type   = "metric"
        x      = 12
        y      = 0
        width  = 12
        height = 6
        properties = {
          metrics = [
            ["AWS/Kinesis", "GetRecords.IteratorAgeMilliseconds", "StreamName", aws_kinesis_stream.sensor_stream.name, { stat = "Maximum", period = 60 }]
          ]
          view    = "timeSeries"
          region  = var.aws_region
          title   = "Flink Consumer Lag (IteratorAgeMilliseconds)"
          yAxis   = { left = { min = 0 } }
        }
      },
      {
        type   = "metric"
        x      = 0
        y      = 6
        width  = 12
        height = 6
        properties = {
          metrics = [
            ["AWS/SQS", "ApproximateNumberOfMessagesVisible", "QueueName", aws_sqs_queue.alert_queue.name, { stat = "Maximum", period = 60 }],
            [".", "ApproximateNumberOfMessagesVisible", "QueueName", aws_sqs_queue.alert_dlq.name, { stat = "Maximum", period = 60, color = "#d62728" }]
          ]
          view    = "timeSeries"
          region  = var.aws_region
          title   = "SQS FIFO Alert Buffer Depth & DLQ Spillage"
        }
      },
      {
        type   = "metric"
        x      = 12
        y      = 6
        width  = 12
        height = 6
        properties = {
          metrics = [
            ["AWS/Lambda", "Invocations", "FunctionName", aws_lambda_function.alerter.function_name, { stat = "Sum", period = 60 }],
            [".", "Errors", ".", ".", { stat = "Sum", period = 60, color = "#d62728" }]
          ]
          view    = "timeSeries"
          region  = var.aws_region
          title   = "Lambda Alerter Invocations & Errors"
        }
      }
    ]
  })
}

