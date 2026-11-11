# -----------------------------------------------------------------------------
# Storage & Catalog Tier: S3, Glue Data Catalog (Partition Projection), & Athena
# -----------------------------------------------------------------------------

resource "random_id" "bucket_suffix" {
  byte_length = 4
}

# 1. Curated Telemetry Bucket
resource "aws_s3_bucket" "curated_bucket" {
  bucket        = "${var.project_name}-curated-${random_id.bucket_suffix.hex}"
  force_destroy = true
}

# 2. Checkpoints & Savepoints Bucket
resource "aws_s3_bucket" "checkpoints_bucket" {
  bucket        = "${var.project_name}-checkpoints-${random_id.bucket_suffix.hex}"
  force_destroy = true
}

# 3. Dead-Letter Queue (DLQ) Bucket for Corrupted / Malformed Telemetry
resource "aws_s3_bucket" "dlq_bucket" {
  bucket        = "${var.project_name}-dlq-${random_id.bucket_suffix.hex}"
  force_destroy = true
}

# 4. AWS Glue Catalog Database
resource "aws_glue_catalog_database" "odysseus_db" {
  name        = "odysseus_streaming_db"
  description = "Odysseus Real-Time Industrial Telemetry Catalog"
}

# 5. AWS Glue Catalog Table with Partition Projection (Zero-Crawler Design)
resource "aws_glue_catalog_table" "curated_telemetry" {
  name          = "curated_sensor_metrics"
  database_name = aws_glue_catalog_database.odysseus_db.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    "classification"                  = "json"
    "projection.enabled"              = "true"
    "projection.year.type"            = "integer"
    "projection.year.range"           = "2024,2030"
    "projection.month.type"           = "integer"
    "projection.month.range"          = "1,12"
    "projection.month.digits"         = "2"
    "projection.day.type"             = "integer"
    "projection.day.range"            = "1,31"
    "projection.day.digits"           = "2"
    "projection.hour.type"            = "integer"
    "projection.hour.range"           = "0,23"
    "projection.hour.digits"          = "2"
    "storage.location.template"       = "s3://${aws_s3_bucket.curated_bucket.id}/telemetry/year=$${year}/month=$${month}/day=$${day}/hour=$${hour}/"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.curated_bucket.id}/telemetry/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"

    ser_de_info {
      name                  = "json-serde"
      serialization_library = "org.openx.data.jsonserde.JsonSerDe"
    }

    columns {
      name = "sensor_id"
      type = "string"
    }
    columns {
      name = "window_start"
      type = "bigint"
    }
    columns {
      name = "window_end"
      type = "bigint"
    }
    columns {
      name = "avg_temperature"
      type = "double"
    }
    columns {
      name = "max_temperature"
      type = "double"
    }
    columns {
      name = "avg_vibration"
      type = "double"
    }
    columns {
      name = "max_vibration"
      type = "double"
    }
    columns {
      name = "avg_pressure"
      type = "double"
    }
    columns {
      name = "avg_power"
      type = "double"
    }
    columns {
      name = "min_battery"
      type = "double"
    }
    columns {
      name = "sample_count"
      type = "bigint"
    }
    columns {
      name = "anomaly_flag"
      type = "boolean"
    }
  }

  partition_keys {
    name = "year"
    type = "string"
  }
  partition_keys {
    name = "month"
    type = "string"
  }
  partition_keys {
    name = "day"
    type = "string"
  }
  partition_keys {
    name = "hour"
    type = "string"
  }
}

# 6. Athena Workgroup for Ad-Hoc Analytics
resource "aws_athena_workgroup" "analytics_workgroup" {
  name        = "${var.project_name}-workgroup"
  description = "Odysseus Serverless Streaming Analytics Engine"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${aws_s3_bucket.curated_bucket.id}/athena-query-results/"
    }
  }
}

