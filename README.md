<p align="center">
  <img src="docs/images/hero_streaming_pipeline.png" alt="Odysseus Real-Time Streaming Data Pipeline: Apache Flink & AWS Kinesis" width="100%" style="border-radius: 12px; box-shadow: 0 10px 30px rgba(0,0,0,0.5);" />
</p>

# 🌊 Odysseus Real-Time Streaming Data Pipeline: Apache Flink & AWS Kinesis

<p align="center">
  <a href="https://guillenconcepcion.github.io/streaming-flink-kinesis-pipeline/"><img src="https://img.shields.io/badge/Live%20Architecture-Interactive%20Web%20Dashboard-0284c7?style=for-the-badge&logo=googlechrome&logoColor=white" alt="Live Interactive Architecture Dashboard" /></a>
  <a href="docs/designs/flink_kinesis_streaming.md"><img src="https://img.shields.io/badge/EGM%20Spec-Canonical%20Design%20Doc-059669?style=for-the-badge&logo=markdown&logoColor=white" alt="Canonical Technical Spec" /></a>
</p>

<p align="center">
  <a href=".github/workflows/ci.yml"><img src="https://img.shields.io/badge/CI%2FCD-Quality%20Gates%20Passing-059669?style=flat-square&logo=githubactions&logoColor=white" alt="CI/CD Quality Gates" /></a>
  <a href="https://flink.apache.org/"><img src="https://img.shields.io/badge/Engine-Flink%201.18%20(Java%2017)-e11d48?style=flat-square&logo=apacheflink&logoColor=white" alt="Apache Flink 1.18" /></a>
  <a href="https://aws.amazon.com/kinesis/"><img src="https://img.shields.io/badge/Ingestion-AWS%20Kinesis-ff9900?style=flat-square&logo=amazonaws&logoColor=white" alt="AWS Kinesis" /></a>
  <a href="https://aws.amazon.com/glue/"><img src="https://img.shields.io/badge/Catalog-Glue%20Projection-059669?style=flat-square" alt="Glue Partition Projection" /></a>
  <a href="https://aws.amazon.com/sqs/"><img src="https://img.shields.io/badge/Alerting-SQS%20FIFO%20Idempotent-d97706?style=flat-square" alt="SQS FIFO" /></a>
  <a href="terraform/observability.tf"><img src="https://img.shields.io/badge/Observability-CloudWatch%20%2B%20Prometheus-818cf8?style=flat-square" alt="CloudWatch" /></a>
  <a href="https://github.com/astral-sh/ruff"><img src="https://img.shields.io/badge/Code%20Style-Ruff-black?style=flat-square" alt="Ruff" /></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-MIT-blue?style=flat-square" alt="MIT License" /></a>
</p>

**Proyecto:** Odysseus AI Platform & Predictive MLOps Framework  
**Lead Architect:** [Guillen Concepcion](https://www.linkedin.com/in/guillen-concepcion-25266b127) *(Senior Data Scientist & MLOps Engineer)*  
**Specification & Design:** [docs/designs/flink_kinesis_streaming.md](docs/designs/flink_kinesis_streaming.md) | [Interactive Architecture Visualizer (HTML)](https://guillenconcepcion.github.io/streaming-flink-kinesis-pipeline/)

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
├── .github/
│   └── workflows/
│       ├── ci.yml                                  # Multi-stage CI/CD Quality Gates (Java, Python, Terraform)
│       └── static.yml                              # Automated GitHub Pages Deployment Workflow
├── docs/
│   ├── index.html                                  # Live Interactive Architecture Dashboard (GitHub Pages)
│   ├── LINKEDIN_POST.md                            # Executive Thought Leadership Publication Post
│   ├── images/
│   │   ├── hero_streaming_pipeline.png             # 16:9 Branded Hero Banner with Typography
│   │   └── guillen_logo.png                        # Author & Lead Architect Profile Avatar
│   └── designs/
│       ├── flink_kinesis_streaming.md              # Canonical EGM Technical Specification
│       └── technical_architecture_system_design.html # Single-Page Interactive Visualizer
├── docker-compose.yml                              # LocalStack (Kinesis, S3, SQS, DynamoDB) + Flink Cluster
├── Makefile                                        # Operational Workflow & Build Automation
├── LICENSE                                         # MIT Open Source License
├── notebooks/
│   └── 01_streaming_analytics_demo.ipynb           # DuckDB & Athena Serverless Analytics Demo
├── scripts/
│   ├── init_localstack.py                          # Cross-platform AWS bootstrap script (Boto3)
│   ├── init_localstack.sh                          # Bash AWS bootstrap script
│   └── overlay_banner_title.py                     # Python Pillow typography compositor for banner
├── generator/                                      # Telemetry Ingestion Layer
│   ├── generator.py                                # High-throughput synthetic IoT generator (1,000+ QPS)
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
│   ├── requirements.txt                            # boto3
│   ├── requirements-dev.txt                        # pytest, moto, ruff
│   └── tests/
│       └── test_handler.py                         # Unit tests verifying atomic deduplication barrier
└── terraform/                                      # Production Cloud Infrastructure (IaC)
    ├── main.tf                                     # AWS Provider configuration
    ├── variables.tf                                # Configurable deployment parameters
    ├── terraform.tfvars.example                    # Sample production deployment variables
    ├── kinesis.tf                                  # Ingestion Kinesis Stream
    ├── storage_catalog.tf                          # S3 + Glue Partition Projection + Athena
    ├── alerting.tf                                 # SQS FIFO + DynamoDB + SNS + Lambda
    ├── observability.tf                            # CloudWatch Alarms (Lag, Shard, DLQ) & Dashboard
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

## 👨‍💻 Author & Lead Architect

<table>
  <tr>
    <td width="130" align="center" valign="middle">
      <img src="docs/images/guillen_logo.png" alt="Guillén Concepción" width="110" style="border-radius: 50%; border: 2px solid #0ea5e9;" />
    </td>
    <td>
      <h3 style="margin-top: 0; margin-bottom: 6px;">Guillén Concepción</h3>
      <p style="margin: 0; font-weight: 600; color: #0284c7;">Senior Data Scientist & MLOps Engineer</p>
      <p style="margin-top: 6px; margin-bottom: 10px; font-size: 13px; color: #475569;">
        Expert in designing, engineering, and deploying end-to-end Cloud-Native AI systems and resilient streaming architectures. Pragmatic and business-value driven, spanning research to audit-compliant production MLOps.
      </p>
      <div>
        <a href="https://www.linkedin.com/in/guillen-concepcion-25266b127"><img src="https://img.shields.io/badge/LinkedIn-0A66C2?style=flat-square&logo=linkedin&logoColor=white" alt="LinkedIn" /></a>
        <a href="https://github.com/GuillenConcepcion"><img src="https://img.shields.io/badge/GitHub-181717?style=flat-square&logo=github&logoColor=white" alt="GitHub" /></a>
        <a href="mailto:guillenconcepcion@gmail.com"><img src="https://img.shields.io/badge/Email-D14836?style=flat-square&logo=gmail&logoColor=white" alt="Email" /></a>
      </div>
    </td>
  </tr>
</table>

