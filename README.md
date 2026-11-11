# 🌊 Odysseus Real-Time Streaming Data Pipeline: Apache Flink & AWS Kinesis

[![CI / Enterprise Quality Gates](https://img.shields.io/badge/CI%2FCD-Quality%20Gates%20Passing-059669?style=flat-square&logo=githubactions&logoColor=white)](.github/workflows/ci.yml)
[![Architecture: EGM Validated](https://img.shields.io/badge/Architecture-EGM%20Validated-0284c7?style=flat-square)](docs/designs/flink_kinesis_streaming.md)
[![Engine: Apache Flink 1.18 (Java 17)](https://img.shields.io/badge/Flink-1.18%20(Java%2017)-e11d48?style=flat-square)](https://flink.apache.org/)
[![Storage: Amazon S3 & Glue Partition Projection](https://img.shields.io/badge/Catalog-Glue%20Projection-059669?style=flat-square)](https://aws.amazon.com/glue/)
[![Alerting: SQS FIFO + DynamoDB TTL](https://img.shields.io/badge/Alerting-SQS%20FIFO%20Idempotent-d97706?style=flat-square)](https://aws.amazon.com/sqs/)
[![Observability: CloudWatch & Prometheus](https://img.shields.io/badge/Observability-CloudWatch%20%2B%20Prometheus-818cf8?style=flat-square)](terraform/observability.tf)
[![Code Style: Ruff](https://img.shields.io/badge/Code%20Style-Ruff-black?style=flat-square)](https://github.com/astral-sh/ruff)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-64748b?style=flat-square)](LICENSE)

**Proyecto:** Odysseus AI Platform & Predictive MLOps Framework  
**Lead Architect:** [Guillen Concepcion](https://www.linkedin.com/in/guillen-concepcion-25266b127) *(Senior Data Scientist & MLOps Engineer)*  
**Specification & Design:** [docs/designs/flink_kinesis_streaming.md](docs/designs/flink_kinesis_streaming.md) | [Interactive Architecture Visualizer (HTML)](docs/designs/technical_architecture_system_design.html)

---

## 🧭 Executive Overview

This repository houses a high-throughput, distributed streaming data pipeline designed for mission-critical Industrial IoT monitoring. The system ingests telemetry streams from distributed industrial machines, executes stateful sliding window aggregations and Complex Event Processing (CEP) with **Apache Flink 1.18 (Java 17)**, guarantees transactional exactly-once commits of columnar Parquet data to **Amazon S3**, enables ad-hoc serverless analytics via **AWS Glue Data Catalog** and **Amazon Athena**, and routes incident alerts through a decoupled, idempotent **Amazon SQS FIFO $\rightarrow$ AWS Lambda $\rightarrow$ Amazon SNS** notification chain.

Designed and verified under the **Elephant-Goldfish Model (EGM)**, this architecture solves the two classical pathologies of real-time stream systems:
1. **Streaming Backpressure Elimination:** Eliminates synchronous external HTTP/webhook calls by offloading alerts to an asynchronous SQS FIFO buffer.
2. **Alert Storm Suppression:** Eliminates duplicate incident notifications during Flink checkpoint replay via deterministic SHA-256 tokens and atomic DynamoDB TTL verification.

---

## 📐 System Architecture

```text
+---------------------------------------------------------------------------------------------------+
|                                    PRODUCER & INGESTION TIER                                      |
|                                                                                                   |
|   +--------------------------+                         +--------------------------------------+   |
|   |  Python Telemetry        |   Batched KPL PutRecords|   AWS Kinesis Data Stream            |   |
|   |  Generator               | ----------------------->|   - 2-10 Shards (Provisioned)        |   |
|   |  (Jitter, Anomaly Inject)|                         |   - PartitionKey: sensor_id          |   |
|   +--------------------------+                         +--------------------------------------+   |
+---------------------------------------------------------------------------------------------------+
                                                                   |
                                                                   | withIdleness(30s)
                                                                   v
+---------------------------------------------------------------------------------------------------+
|                                  APACHE FLINK STREAM ENGINE (JAVA 17)                             |
|                                                                                                   |
|   +-------------------------------------------------------------------------------------------+   |
|   | Kinesis Streams Source (BoundedOutOfOrderness Watermarks: 5s lag)                         |   |
|   +-------------------------------------------------------------------------------------------+   |
|            |                                                                   |                  |
|            | Valid Stream (Keyed by sensor_id)                                 | Malformed / Late |
|            v                                                                   v                  |
|   +----------------------------------------------------+             +------------------------+   |
|   | Sliding Window Aggregators                         |             | Side Output / DLQ      |   |
|   | - 1-Minute Window, sliding every 10 seconds        |             | s3://.../dlq/malformed/|   |
|   | - RocksDB StateBackend + Incremental Checkpoints   |             +------------------------+   |
|   | - Stable Operator UIDs: .uid("sensor-window-agg")  |                                          |
|   +----------------------------------------------------+                                          |
|            |                                                                                      |
|            +------------------------------------+                                                 |
|            |                                    |                                                 |
|            v                                    v                                                 |
|   +---------------------------------+  +------------------------------------------------------+   |
|   | Rolling FileSink (Parquet / S3) |  | Complex Event Processing (CEP) Engine                |   |
|   | - Rolling: 128 MB / 30 min      |  | - Pattern: ThermalSpike -> followedBy(VibrationAlarm)|   |
|   | - Exactly-Once 2-Phase Commit   |  |   within(30 seconds)                                 |   |
|   +---------------------------------+  | - Deterministic SHA-256 Idempotency Token            |   |
|            |                           +------------------------------------------------------+   |
|            | Parquet Partitions                                 | SQS FIFO Sink (Asynchronous)    |
|            v                                                    v                                 |
+-----------------------------------+    +----------------------------------------------------------+
             |                                    |
             v                                    v
+-----------------------------------+    +----------------------------------------------------------+
|       STORAGE & CATALOG TIER      |    |                 ALERTING & NOTIFICATION TIER             |
|                                   |    |                                                          |
|   +---------------------------+   |    |   +--------------------------------------------------+   |
|   | Amazon S3 Bucket          |   |    |   | Amazon SQS FIFO Queue                            |   |
|   | (s3://.../curated/)       |   |    |   | - MessageGroupId: sensor_id                      |   |
|   +---------------------------+   |    |   | - DeduplicationId: SHA-256 IdempotencyKey        |   |
|                 |                 |    |   +--------------------------------------------------+   |
|                 v                 |    |                            |                             |
|   +---------------------------+   |    |                            v Event Source Mapping        |
|   | AWS Glue Data Catalog     |   |    |   +--------------------------------------------------+   |
|   | - Partition Projection    |   |    |   | AWS Lambda Alerter                               |   |
|   +---------------------------+   |    |   | - DynamoDB Atomic TTL Verification (15 min)      |   |
|                 |                 |    |   +--------------------------------------------------+   |
|                 v                 |    |                            |                             |
|   +---------------------------+   |    |                            v                             |
|   | Amazon Athena / Zeppelin  |   |    |   +--------------------------------------------------+   |
|   | Serverless SQL Analytics  |   |    |   | Amazon SNS Topic (Email Notifications)           |   |
|   +---------------------------+   |    |   +--------------------------------------------------+   |
|                                   |    |                                                          |
+-----------------------------------+    +----------------------------------------------------------+
```

---

## 🔬 Mathematical Formulations

### 1. Complex Event Processing (CEP) Non-Deterministic Finite Automaton
The engine monitors each `sensor_id` for dangerous kinetic/thermal coupling signatures:

$$\begin{aligned}
\text{Pattern} = & \text{begin}("thermal\_spike") \\
& \text{.where}(e_1 \implies e_1.\text{temperature} > 85.0^\circ\text{C}) \\
& \text{.followedBy}("kinetic\_vibration") \\
& \text{.where}(e_2 \implies e_2.\text{vibration\_rms} > 4.5\text{ mm/s}) \\
& \text{.within}(\text{Time.seconds}(30))
\end{aligned}$$

### 2. Idempotency Token Generation
To prevent alert storms upon checkpoint replays, the CEP select function calculates:

$$\text{IdempotencyKey} = \text{SHA256}(\text{sensor\_id} + \text{pattern\_name} + t_{\text{start}} + t_{\text{end}})$$

This token serves simultaneously as the Amazon SQS `MessageDeduplicationId` and the DynamoDB primary partition key.

---

## 🗂️ Project Directory Structure

```text
experiments/streaming_flink_kinesis_pipeline/
├── docs/
│   └── designs/
│       ├── flink_kinesis_streaming.md              # Canonical EGM Technical Specification
│       └── technical_architecture_system_design.html # Interactive Architecture Dashboard
├── docker-compose.yml                              # LocalStack + Flink JobManager + TaskManager
├── Makefile                                        # Operational Workflow & Build Automation
├── scripts/
│   ├── init_localstack.py                          # Cross-platform AWS bootstrap script
│   └── init_localstack.sh                          # Bash AWS bootstrap script
├── generator/                                      # Telemetry Ingestion Layer
│   ├── generator.py                                # High-throughput synthetic IoT generator
│   └── requirements.txt                            # boto3, numpy
├── flink-processor/                                # Stream Processing Engine (Java 17)
│   ├── pom.xml                                     # Maven POM with Flink 1.18 & Shade fat JAR
│   ├── src/main/java/com/odysseus/streaming/
│   │   ├── StreamingJob.java                       # Main Pipeline Wiring & Checkpointing
│   │   ├── model/
│   │   │   ├── SensorEvent.java                    # Telemetry POJO
│   │   │   ├── AggregatedSensorMetric.java         # Analytical Window Aggregation POJO
│   │   │   └── AlertEvent.java                     # Incident Alert POJO with SHA-256 Hash
│   │   ├── operators/
│   │   │   └── WindowAggregators.java              # Incremental Mean/Max/RMS Accumulator
│   │   ├── cep/
│   │   │   └── AnomalyPatterns.java                # Flink CEP Catastrophe Pattern
│   │   └── sink/
│   │       └── SqsAlertSink.java                   # Asynchronous SQS FIFO Alert Sink
│   └── src/test/java/com/odysseus/streaming/
│       └── cep/AnomalyPatternsTest.java            # Unit tests for CEP State Machine
├── lambda-alerter/                                 # Incident Alerting Tier
│   ├── handler.py                                  # Lambda handler with DynamoDB atomic TTL
│   └── requirements.txt
└── terraform/                                      # Production Cloud Infrastructure (IaC)
    ├── main.tf                                     # AWS Provider configuration
    ├── variables.tf                                # Configurable deployment parameters
    ├── kinesis.tf                                  # Ingestion Kinesis Stream
    ├── storage_catalog.tf                          # S3 + Glue Partition Projection + Athena
    ├── alerting.tf                                 # SQS FIFO + DynamoDB + SNS + Lambda
    └── outputs.tf                                  # Resource ARNs and endpoints
```

---

## ⚡ Quickstart & Local Execution Guide

### Prerequisites
* **Docker & Docker Compose** (or Podman)
* **Java 17+** & **Maven 3.8+**
* **Python 3.10+** (with `boto3` and `numpy`)

### Step 1: Bootstrap LocalStack and Apache Flink Cluster
```bash
make up
```
* Flink Dashboard UI: [http://localhost:8081](http://localhost:8081)
* LocalStack Edge Gateway: [http://localhost:4566](http://localhost:4566)

### Step 2: Initialize Mock AWS Infrastructure
```bash
make init-aws
```
Creates Kinesis stream (`odysseus-sensor-stream`), S3 buckets (`curated`, `checkpoints`, `dlq`), SQS FIFO queues, DynamoDB table (`odysseus_alert_idempotency`), and SNS topic in LocalStack.

### Step 3: Run Automated Unit Tests (Flink CEP & Lambda Idempotency)
```bash
# Execute Flink CEP State Machine Unit Tests (Java 17)
make test

# Execute AWS Lambda Alerter & DynamoDB Idempotency Tests (Moto Mocking)
make test-lambda

# Validate Static Code Quality (Ruff) & Terraform IaC Syntax
make lint
make terraform-validate
```

### Step 4: Build Shaded Executable JAR
```bash
# Build using local Maven (if installed)
make build

# OR build inside an ephemeral Docker container (Zero prerequisites)
make build-docker
```

### Step 5: Submit Pipeline to Flink Cluster
```bash
make submit
```
Submits the shaded fat JAR to the running Flink JobManager via REST API and verifies running topology.

### Step 6: Start Telemetry Generator (with Anomaly Injection)
```bash
make generate-traffic
```
Publishes continuous sensor readings at 1,000 QPS with network delivery jitter, periodically injecting thermal-vibration composite anomalies to trigger the alerting pipeline.

---

## 📈 Interactive Analytics Demo Notebook

Explore the downstream Parquet dataset committed by Flink using the self-contained analytics notebook:
* [`notebooks/01_streaming_analytics_demo.ipynb`](notebooks/01_streaming_analytics_demo.ipynb)
* Connects via **DuckDB** to query Parquet partitions using ANSI SQL, validating sub-second partition pruning without Glue Crawlers, and plotting the thermal vs. vibration phase space isolated by Flink CEP.

---

## 📊 Serverless SQL Queries via Amazon Athena

Thanks to **Glue Partition Projection**, Athena queries bypass traditional table metadata crawling and prune S3 partitions instantly:

```sql
-- Query 1: Detect highest vibration spikes over the last 24 hours
SELECT 
    sensor_id,
    from_unixtime(window_start / 1000) AS start_time,
    max_temperature,
    max_vibration,
    avg_power,
    anomaly_flag
FROM odysseus_streaming_db.curated_sensor_metrics
WHERE year = '2026' 
  AND month = '11' 
  AND day = '11'
  AND anomaly_flag = true
ORDER BY max_vibration DESC
LIMIT 50;
```

```sql
-- Query 2: Fleet-wide rolling correlation between Power Draw and Operating Temperature
SELECT 
    sensor_id,
    approx_percentile(avg_temperature, 0.50) AS median_temp,
    approx_percentile(avg_temperature, 0.95) AS p95_temp,
    approx_percentile(max_vibration, 0.95)   AS p95_vibration,
    sum(sample_count)                       AS total_samples_analyzed
FROM odysseus_streaming_db.curated_sensor_metrics
WHERE year = '2026' AND month = '11'
GROUP BY sensor_id
ORDER BY p95_temp DESC;
```

---

## 👨‍💻 Author & Contact

- **Lead Architect:** [Guillen Concepcion](https://www.linkedin.com/in/guillen-concepcion-25266b127) *(Senior Data Scientist & MLOps Engineer)*
- **GitHub:** [https://github.com/GuillenConcepcion](https://github.com/GuillenConcepcion)
- **LinkedIn:** [https://www.linkedin.com/in/guillen-concepcion-25266b127](https://www.linkedin.com/in/guillen-concepcion-25266b127)
- **Email:** `guillenconcepcion@gmail.com`

