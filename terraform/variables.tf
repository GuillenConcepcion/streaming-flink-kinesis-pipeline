variable "aws_region" {
  description = "Target AWS Region"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Deployment environment name"
  type        = string
  default     = "dev"
}

variable "project_name" {
  description = "Project designation tag"
  type        = string
  default     = "odysseus-streaming"
}

variable "kinesis_shard_count" {
  description = "Number of provisioned shards for Kinesis Data Stream"
  type        = number
  default     = 2
}

variable "alert_notification_email" {
  description = "Operator email address for SNS incident subscriptions"
  type        = string
  default     = "guillenconcepcion@gmail.com"
}

