# -----------------------------------------------------------------------------
# Alerting Tier: SQS FIFO Buffer, DynamoDB Deduplication, Lambda, & SNS
# -----------------------------------------------------------------------------

# 1. SQS Dead-Letter Queue (FIFO)
resource "aws_sqs_queue" "alert_dlq" {
  name                        = "${var.project_name}-incident-dlq.fifo"
  fifo_queue                  = true
  content_based_deduplication = false
}

# 2. Main SQS Incident Alert Queue (FIFO)
resource "aws_sqs_queue" "alert_queue" {
  name                        = "${var.project_name}-incident-alerts.fifo"
  fifo_queue                  = true
  content_based_deduplication = false # Relies on explicit deterministic SHA-256 tokens
  visibility_timeout_seconds  = 60
  message_retention_seconds   = 86400 # 1 day buffer

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.alert_dlq.arn
    maxReceiveCount     = 3
  })
}

# 3. DynamoDB Atomic Idempotency Table (15-Minute TTL)
resource "aws_dynamodb_table" "alert_idempotency" {
  name         = "odysseus_alert_idempotency"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "idempotency_key"

  attribute {
    name = "idempotency_key"
    type = "S"
  }

  ttl {
    attribute_name = "ttl"
    enabled        = true
  }

  point_in_time_recovery {
    enabled = true
  }
}

# 4. Amazon SNS Topic for Incident Broadcasting
resource "aws_sns_topic" "incident_topic" {
  name = "${var.project_name}-incident-topic"
}

resource "aws_sns_topic_subscription" "email_sub" {
  topic_arn = aws_sns_topic.incident_topic.arn
  protocol  = "email"
  endpoint  = var.alert_notification_email
}

# 5. IAM Role for Lambda Alerter Worker
resource "aws_iam_role" "lambda_alerter_role" {
  name = "${var.project_name}-lambda-alerter-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
    }]
  })
}

resource "aws_iam_policy" "lambda_alerter_policy" {
  name = "${var.project_name}-lambda-alerter-policy"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = [
          "sqs:ReceiveMessage",
          "sqs:DeleteMessage",
          "sqs:GetQueueAttributes"
        ]
        Resource = aws_sqs_queue.alert_queue.arn
      },
      {
        Effect   = "Allow"
        Action   = [
          "dynamodb:PutItem",
          "dynamodb:GetItem"
        ]
        Resource = aws_dynamodb_table.alert_idempotency.arn
      },
      {
        Effect   = "Allow"
        Action   = "sns:Publish"
        Resource = aws_sns_topic.incident_topic.arn
      },
      {
        Effect   = "Allow"
        Action   = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:*:*:*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_attach" {
  role       = aws_iam_role.lambda_alerter_role.name
  policy_arn = aws_iam_policy.lambda_alerter_policy.arn
}

# 6. Lambda Deployment Package
data "archive_file" "lambda_zip" {
  type        = "zip"
  source_file = "${path.module}/../lambda-alerter/handler.py"
  output_path = "${path.module}/lambda_alerter.zip"
}

# 7. Lambda Alerter Function
resource "aws_lambda_function" "alerter" {
  filename         = data.archive_file.lambda_zip.output_path
  function_name    = "${var.project_name}-alerter"
  role             = aws_iam_role.lambda_alerter_role.arn
  handler          = "handler.lambda_handler"
  runtime          = "python3.11"
  timeout          = 30
  memory_size      = 256
  source_code_hash = data.archive_file.lambda_zip.output_base64sha256

  environment {
    variables = {
      DYNAMODB_IDEMPOTENCY_TABLE = aws_dynamodb_table.alert_idempotency.name
      SNS_TOPIC_ARN              = aws_sns_topic.incident_topic.arn
      DEDUPLICATION_TTL_SECONDS  = "900"
    }
  }
}

# 8. SQS to Lambda Event Source Mapping
resource "aws_lambda_event_source_mapping" "sqs_event_trigger" {
  event_source_arn                   = aws_sqs_queue.alert_queue.arn
  function_name                      = aws_lambda_function.alerter.arn
  batch_size                         = 25
  maximum_batching_window_in_seconds = 5
  function_response_types            = ["ReportBatchItemFailures"]
}

