# Streaming Data Pipeline: Apache Flink & AWS Kinesis for Real-Time IoT Monitoring and CEP

* **Author:** Guillen Concepcion (Senior Data Scientist & MLOps Engineer)
* **Status:** Proposed / In Review
* **Date:** 2026-11-11
* **Project Directory:** `experiments/streaming_flink_kinesis_pipeline`
* **Framework:** Elephant-Goldfish Model (EGM) / Project Odysseus

---

## Overview

This specification defines the architecture, data contracts, and implementation plan for a production-grade, distributed streaming data pipeline. The system ingests high-frequency telemetry from distributed industrial sensors, executes stateful sliding window aggregations and Complex Event Processing (CEP) with **Apache Flink (Java 17)**, guarantees transactional exactly-once commits of columnar Parquet data to **Amazon S3**, enables ad-hoc serverless analytics via **AWS Glue Data Catalog** and **Amazon Athena**, and routes incident alerts through a decoupled, idempotent **Amazon SQS FIFO $\rightarrow$ AWS Lambda $\rightarrow$ Amazon SNS** notification chain.

---

## The Problem

Industrial IoT deployments (turbines, manufacturing equipment, cold-chain transport) emit continuous sensor telemetry subject to clock skew, network jitter, and out-of-order delivery. Traditional micro-batch architectures (e.g., Spark Streaming) introduce sub-second latency that is too slow for kinetic/thermal catastrophe prevention. Conversely, naive event-driven architectures that invoke alerting functions synchronously from stream workers induce fatal backpressure, watermark freezes, and alert storms. 

This project solves these challenges by combining low-latency pipelined stream processing (sub-50 ms), deterministic event-time watermarking, stateful pattern matching with bounded RocksDB memory, and an asynchronously buffered, deduplicated alerting tier.

---

## Goals & Non-Goals

### Goals
1. **True Event-Time Processing:** Guarantee deterministic temporal windowing and pattern matching using payload-embedded timestamps (`event_timestamp`), with bounded out-of-order watermarking and automatic idleness handling across Kinesis shards.
2. **Complex Event Processing (CEP):** Detect multi-metric composite fault signatures (e.g., thermal spike followed by kinetic vibration anomaly within 30 seconds) using native Flink CEP.
3. **Decoupled Idempotent Alerting:** Eliminate streaming backpressure by dispatching alerts asynchronously to an Amazon SQS FIFO buffer, deduplicating alerts in AWS Lambda using an atomic TTL store (Amazon DynamoDB) before publishing to Amazon SNS.
4. **Transactional Storage & Zero-Crawler Catalog:** Commit Parquet files to Amazon S3 using Flink's two-phase commit `FileSink`, enabling immediate Athena SQL querying via AWS Glue Partition Projection (`projection.enabled = true`) without scheduled Glue Crawlers.
5. **Dual-Target Execution:** Deliver 100% functional parity between a local containerized environment (**Docker Compose + LocalStack + standalone Flink**) and cloud-native deployment (**Terraform + AWS Managed Service for Apache Flink**).

### Non-Goals
1. Dynamic online training of deep learning models inside Flink TaskManagers (feature extraction and anomaly scoring are in scope; gradient updates are out of scope).
2. Development of bespoke web frontend applications (visualization is delivered via Apache Zeppelin notebooks and Athena SQL).
3. Supporting legacy message brokers (RabbitMQ/JMS); architecture is specialized for AWS Kinesis Data Streams.

---

## Open Questions & Resolution Status

* **Question 1: PyFlink vs. Java 17:**  
  *Resolution:* Locked to **Java 17 with Maven**. PyFlink introduces a 300%–500% IPC serialization tax and lacks complete CEP API coverage for complex stateful conditions. Python is retained strictly for the synthetic telemetry generator and integration test harness.
* **Question 2: Alert Storm Mitigation during Flink Checkpoint Replay:**  
  *Resolution:* Each CEP pattern match generates a deterministic SHA-256 token:
  $$\text{IdempotencyKey} = \text{SHA256}(\text{sensor\_id} + \text{pattern\_id} + \text{window\_start} + \text{window\_end})$$
  The Lambda alerting worker performs a conditional `PutItem` with a 15-minute TTL in Amazon DynamoDB before triggering SNS.
* **Question 3: Watermark Progression on Quiet Shards:**  
  *Resolution:* Source watermarking configured with `.withIdleness(Duration.ofSeconds(30))` to prevent idle Kinesis shards from stalling global window execution.

---

## Critical User Journeys (CUJs)

### CUJ 1: Ingestion & Sliding Window Aggregation
* **Given:** A cluster of 1,000 industrial sensors emitting telemetry at 5,000 events/second into AWS Kinesis.
* **When:** Flink consumes the stream using `FlinkKinesisConsumer` with bounded out-of-order watermarks (5 seconds lag).
* **Then:** Flink continuously computes 1-minute sliding windows (sliding every 10 seconds) for each `sensor_id`, calculating average temperature, RMS vibration, and battery health, rolling transactional Parquet files into `s3://.../curated/` every 128 MB or 30 minutes.

### CUJ 2: Multi-Metric Kinetic/Thermal Fault Detection & Alerting
* **Given:** Sensor `SN-TURBINE-042` experiences a rapid thermal spike ($T > 85^\circ\text{C}$ or $\Delta T/\Delta t > 5^\circ\text{C}/\text{s}$), followed 18 seconds later by abnormal RMS vibration ($V_{\text{RMS}} > 4.5\text{ mm/s}$).
* **When:** The Flink CEP `PatternStream` detects the composite failure condition within the 30-second temporal constraint.
* **Then:** Flink emits an alert payload to Amazon SQS FIFO within 50 ms. AWS Lambda processes the batched message, verifies the SHA-256 token in DynamoDB, and dispatches a high-priority incident notification to the engineering team via Amazon SNS.

### CUJ 3: Serverless Ad-Hoc Analytics via Amazon Athena
* **Given:** Historical telemetry committed to S3 under `s3://.../curated/year=YYYY/month=MM/day=DD/hour=HH/`.
* **When:** An analytics engineer runs an ad-hoc query in Amazon Athena or Zeppelin.
* **Then:** Athena leverages Glue Partition Projection to prune S3 prefixes without crawler lag, executing sub-second partition pruning and returning aggregation metrics across millions of records.

---

## Technical Architecture & System Design

```text
+---------------------------------------------------------------------------------------------------+
|                                    PRODUCER & INGESTION TIER                                      |
|                                                                                                   |
|   +--------------------------+                         +--------------------------------------+   |
|   |  Python Telemetry        |   Batched KPL PutRecords|   AWS Kinesis Data Stream            |   |
|   |  Generator               | ----------------------->|   - 5 Shards (Provisioned/On-Demand) |   |
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
|   | FlinkKinesisConsumer (BoundedOutOfOrderness Watermarks: 5s lag)                           |   |
|   +-------------------------------------------------------------------------------------------+   |
|            |                                                                   |                  |
|            | Valid Stream (Keyed by sensor_id)                                 | Malformed / Late |
|            v                                                                   v                  |
|   +----------------------------------------------------+             +------------------------+   |
|   | Window Aggregators                                 |             | Side Output / DLQ      |   |
|   | - Sliding Window: 1 min, slide 10s                 |             | s3://.../dlq/malformed/|   |
|   | - RocksDB StateBackend + StateTTL (1 hour)         |             +------------------------+   |
|   | - Stable UIDs: .uid("window-aggregator")           |                                          |
|   +----------------------------------------------------+                                          |
|            |                                                                                      |
|            +------------------------------------+                                                 |
|            |                                    |                                                 |
|            v                                    v                                                 |
|   +---------------------------------+  +------------------------------------------------------+   |
|   | FileSink (Transactional Parquet)|  | CEP Engine: AnomalyPatterns                          |   |
|   | - Rolling: 128 MB / 30 min      |  | - Pattern: ThermalSpike -> followedBy(VibrationAlarm)|   |
|   | - Exact-Once 2-Phase Commit     |  |   within(30 seconds)                                 |   |
|   +---------------------------------+  | - Deterministic SHA-256 Idempotency Token            |   |
|            |                           +------------------------------------------------------+   |
+---------------------------------------------------------------------------------------------------+
             |                                    |
             | Parquet Partitions                 | SQS Sink (Asynchronous)
             v                                    v
+-----------------------------------+    +----------------------------------------------------------+
|       STORAGE & CATALOG TIER      |    |                 ALERTING & NOTIFICATION TIER             |
|                                   |    |                                                          |
|   +---------------------------+   |    |   +--------------------------------------------------+   |
|   | Amazon S3 Bucket          |   |    |   | Amazon SQS FIFO Queue                            |   |
|   | (s3://.../curated/)       |   |    |   | - MessageGroupId: sensor_id                      |   |
|   +---------------------------+   |    |   | - DeduplicationId: IdempotencyKey                |   |
|                 |                 |    |   +--------------------------------------------------+   |
|                 v                 |    |                            |                             |
|   +---------------------------+   |    |                            v Event Source Mapping        |
|   | AWS Glue Data Catalog     |   |    |   +--------------------------------------------------+   |
|   | - Partition Projection    |   |    |   | AWS Lambda Alerter                               |   |
|   +---------------------------+   |    |   | - BatchSize: 25, BatchWindow: 5s                 |   |
|                 |                 |    |   | - DynamoDB Atomic TTL Verification (15 min)      |   |
|                 v                 |    |   +--------------------------------------------------+   |
|   +---------------------------+   |    |                            |                             |
|   | Amazon Athena / Zeppelin  |   |    |                            v                             |
|   | Serverless SQL Analytics  |   |    |   +--------------------------------------------------+   |
|   +---------------------------+   |    |   | Amazon SNS Topic (Email / SMS Notifications)     |   |
|                                   |    |   +--------------------------------------------------+   |
+-----------------------------------+    +----------------------------------------------------------+
```

---

## Data Models & Schema Contracts

### 1. Ingestion Telemetry Schema (`SensorEvent.java`)
```json
{
  "sensor_id": "TURBINE-001",
  "device_type": "KINETIC_TURBINE",
  "event_timestamp": 1794396000000,
  "temperature_celsius": 88.4,
  "vibration_rms": 5.12,
  "pressure_kpa": 101.3,
  "power_draw_kw": 45.2,
  "battery_voltage": 24.1,
  "status_code": "OK"
}
```

### 2. Output Curated Parquet Schema
* `sensor_id`: `STRING`
* `window_start`: `TIMESTAMP(MILLIS)`
* `window_end`: `TIMESTAMP(MILLIS)`
* `avg_temperature`: `DOUBLE`
* `max_temperature`: `DOUBLE`
* `avg_vibration`: `DOUBLE`
* `max_vibration`: `DOUBLE`
* `sample_count`: `BIGINT`
* `anomaly_flag`: `BOOLEAN`
* Partition Keys: `year: STRING`, `month: STRING`, `day: STRING`, `hour: STRING`

### 3. Incident Alert Schema (`AlertEvent.java`)
```json
{
  "alert_id": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
  "sensor_id": "TURBINE-001",
  "pattern_name": "THERMAL_KINETIC_CATASTROPHE",
  "detected_timestamp": 1794396030000,
  "trigger_temperature": 88.4,
  "trigger_vibration": 5.12,
  "severity": "CRITICAL",
  "details": "Temperature spike exceeded 85C followed by vibration > 4.5 mm/s within 18.2s"
}
```

---

## Mathematical Formulation of Complex Event Processing (CEP)

The CEP engine detects catastrophic composite states using the following Non-deterministic Finite Automaton (NFA):

$$\begin{aligned}
\text{Pattern} = & \text{begin}("thermal\_spike") \\
& \text{.where}(e_1 \implies e_1.\text{temperature} > 85.0 \lor \Delta T/\Delta t > 5.0) \\
& \text{.followedBy}("kinetic\_vibration") \\
& \text{.where}(e_2 \implies e_2.\text{vibration\_rms} > 4.5 \lor z\text{-score} > 3.0) \\
& \text{.within}(\text{Time.seconds}(30))
\end{aligned}$$

---

## Detailed Implementation Plan (File-by-File Breakdown)

### 1. Build & Container Environment

#### `docker-compose.yml` (Create)
* **Scope:** Local development environment.
* **Services:**
  * `localstack`: Emulating Kinesis, S3, SQS, SNS, DynamoDB on port 4566.
  * `flink-jobmanager`: Apache Flink 1.18.1 JobManager with Web UI on port 8081.
  * `flink-taskmanager`: Flink TaskManager configured with RocksDB backend and 2 TaskSlots.

#### `Makefile` (Create)
* **Scope:** Developer workflow automation.
* **Targets:**
  * `make up`: Bootstraps LocalStack and Flink cluster.
  * `make init-aws`: Creates mock Kinesis streams, S3 buckets, SQS queues, and DynamoDB tables in LocalStack.
  * `make build`: Compiles Java Flink engine via Maven package.
  * `make submit`: Submits shaded Flink JAR to local JobManager via REST API.
  * `make generate-traffic`: Starts Python generator streaming synthetic records.
  * `make test`: Executes unit tests and integration tests.

---

### 2. Telemetry Ingestion Layer (`generator/`)

#### `generator/generator.py` (Create)
* **Scope:** High-throughput synthetic IoT generator.
* **Logic:** Generates realistic multi-sensor telemetry using multi-threading with Boto3 Kinesis batch puts (`put_records`). Supports injected failure profiles: linear temperature drift, Gaussian vibration shocks, and malformed JSON payloads. Configurable QPS (500 to 10,000 records/sec).

#### `generator/requirements.txt` (Create)
* **Scope:** Python dependencies: `boto3`, `numpy`, `scipy`.

---

### 3. Flink Streaming Engine (`flink-processor/`)

#### `flink-processor/pom.xml` (Create)
* **Scope:** Maven build configuration targeting Java 17.
* **Dependencies:**
  * `flink-streaming-java`: 1.18.1
  * `flink-clients`: 1.18.1
  * `flink-cep`: 1.18.1
  * `flink-connector-kinesis`: 4.2.0-1.18
  * `flink-connector-files`: 1.18.1
  * `flink-parquet`: 1.18.1
  * `flink-statebackend-rocksdb`: 1.18.1
  * `aws-java-sdk-sqs`: 1.12.x
  * `maven-shade-plugin`: Generates fat JAR with relocation.

#### `flink-processor/src/main/java/com/odysseus/streaming/StreamingJob.java` (Create)
* **Scope:** Core pipeline assembly and execution entry point.
* **Logic:** Configures `StreamExecutionEnvironment`, checkpointing (every 60s, `EXACTLY_ONCE`), RocksDB incremental backend, Kinesis consumer with `WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)).withIdleness(Duration.ofSeconds(30))`, sliding window aggregations, CEP pattern matching, Parquet S3 sink, and SQS alert sink.

#### `flink-processor/src/main/java/com/odysseus/streaming/model/SensorEvent.java` (Create)
* **Scope:** Strongly typed POJO representing the telemetry event, with Flink TypeInformation support.

#### `flink-processor/src/main/java/com/odysseus/streaming/model/AggregatedSensorMetric.java` (Create)
* **Scope:** POJO for windowed analytical aggregations.

#### `flink-processor/src/main/java/com/odysseus/streaming/model/AlertEvent.java` (Create)
* **Scope:** POJO for CEP incident alert payloads with SHA-256 idempotency calculation.

#### `flink-processor/src/main/java/com/odysseus/streaming/operators/WindowAggregators.java` (Create)
* **Scope:** Custom `AggregateFunction` and `ProcessWindowFunction` computing running mean, max, and variance across sliding windows with stable `.uid("sensor-window-agg")`.

#### `flink-processor/src/main/java/com/odysseus/streaming/cep/AnomalyPatterns.java` (Create)
* **Scope:** Flink CEP `Pattern<SensorEvent, ?>` definition implementing the thermal-kinetic catastrophe condition with `.within(Time.seconds(30))` and stable `.uid("sensor-cep-engine")`.

#### `flink-processor/src/main/java/com/odysseus/streaming/sink/SqsAlertSink.java` (Create)
* **Scope:** Non-blocking async SQS FIFO sink emitting alert events with deterministic message group IDs and deduplication tokens.

---

### 4. Alerting & Deduplication Layer (`lambda-alerter/`)

#### `lambda-alerter/handler.py` (Create)
* **Scope:** Event-driven AWS Lambda worker.
* **Logic:** Triggered by SQS FIFO batches. For each record:
  1. Reads `IdempotencyKey`.
  2. Executes conditional `PutItem` in DynamoDB (`attribute_not_exists(idempotency_key)` with 15-minute TTL).
  3. If unique, publishes formatted alert to Amazon SNS.
  4. If duplicate, acknowledges SQS record silently (idempotent no-op).

---

### 5. Infrastructure as Code (`terraform/`)

#### `terraform/main.tf` & `terraform/kinesis.tf` (Create)
* **Scope:** AWS Cloud infrastructure provisioning.
* **Resources:** `aws_kinesis_stream`, `aws_s3_bucket`, `aws_glue_catalog_database`, `aws_glue_catalog_table` (with Partition Projection), `aws_sqs_queue` (FIFO), `aws_dynamodb_table` (TTL enabled), `aws_sns_topic`, `aws_lambda_function`.

---

## Verification Plan

1. **Compilation:** `mvn clean package -DskipTests=false` in `flink-processor/` must build a shaded fat JAR without dependency conflicts.
2. **Unit Testing:**
   * Test CEP pattern state machine with Flink `test-harness` (`CEPMonitoringTest.java`), asserting that isolated temperature spikes do not trigger alerts, whereas combined events trigger within 30 seconds.
   * Test window aggregators with out-of-order records.
3. **End-to-End Integration Smoke Test (LocalStack):**
   * Execute `make up && make init-aws && make submit && make generate-traffic`.
   * Verify records appear in Kinesis stream.
   * Verify Flink Web UI displays active topology with zero backpressure.
   * Verify Parquet files land in LocalStack S3 (`s3://odysseus-telemetry/curated/`).
   * Verify simulated anomaly triggers exactly 1 alert message in SQS/SNS via Lambda logs without duplicates.

