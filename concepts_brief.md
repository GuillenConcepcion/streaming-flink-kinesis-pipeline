# Streaming Data Pipeline: Apache Flink and AWS Kinesis for IoT Real-Time Monitoring & CEP

* **Author:** Guillen Concepcion (Senior Data Scientist & MLOps Engineer)
* **Date:** 2026-11-11
* **Framework:** Elephant-Goldfish Model (EGM) / Project Odysseus (`experiments/streaming_flink_kinesis_pipeline`)
* **Reviewers:** `egm-design` (Design Critic) & `egm-implementation-readiness` (Technical Lead)
* **Status:** Aligned RFC / Ready for Technical Design (`egm-draft`)

---

## 1. Executive Overview

Design and construction of an end-to-end, production-grade distributed stream processing pipeline to ingest high-frequency IoT sensor telemetry, execute real-time stateful analytics and Complex Event Processing (CEP) with **Apache Flink**, persist transactionally partitioned Parquet data into **Amazon S3**, index schemas with **AWS Glue Data Catalog**, query historical streams with **Amazon Athena**, and dispatch non-blocking, debounced incident alerts via **Amazon SQS**, **AWS Lambda**, and **Amazon SNS**.

---

## 2. Synthesized Architectural Audit (Goldfish Review Findings)

The dual adversarial Goldfish review identified 6 structural failure modes and mandated concrete engineering mitigations:

```mermaid
flowchart LR
    subgraph INGESTION ["1. Ingestion Layer"]
        GEN["Python Telemetry Generator\n(Batched KPL / Bounded Jitter)"] -->|Sharded by sensor_id| KIN["AWS Kinesis Data Stream\n(5 Provisioned Shards / On-Demand)"]
    end

    subgraph FLINK_CORE ["2. Stateful Stream Processing (Java 17 Flink)"]
        KIN -->|withIdleness: 30s| SRC["Flink Kinesis Consumer\n(Stable UIDs & Watermarks)"]
        SRC --> WIN["Tumbling/Sliding Windows\n(RocksDB Incremental + StateTTL)"]
        SRC --> CEP["Flink CEP Pattern Stream\n(within 30s | Exact Math Predicates)"]
        SRC -.->|Malformed / Late Events| DLQ["S3 Side-Output DLQ\n(s3://.../dlq/malformed/)"]
    end

    subgraph STORAGE_QUERY ["3. Storage & Analytics"]
        WIN -->|Two-Phase Commit FileSink| S3["Amazon S3 Bucket\n(Parquet Rollover: 128 MB / 30 min)"]
        S3 --> GLUE["AWS Glue Catalog\n(Partition Projection Enabled)"]
        GLUE --> ATHENA["Amazon Athena / Zeppelin\n(Serverless SQL Querying)"]
    end

    subgraph ALERTING ["4. Non-Blocking Decoupled Alerting"]
        CEP -->|Idempotency Key (SHA-256)| SQS["Amazon SQS FIFO Queue\n(Decoupling Buffer)"]
        SQS -->|BatchSize: 10-50| LAMBDA["AWS Lambda Worker\n(DynamoDB TTL Deduplication)"]
        LAMBDA --> SNS["Amazon SNS Topic\n(Email / SMS Alert Notifications)"]
    end
```

### Critical Findings & Mandatory Mitigations:

1. **Decoupling Flink from Lambda via SQS FIFO (Eliminates Backpressure & Alert Storms):**
   * *Vulnerability:* Synchronous HTTP invocations from Flink to Lambda incur 50–150 ms network latency, halting watermark progression, triggering checkpoint timeouts, and exhausting regional Lambda concurrency during anomaly bursts.
   * *Mitigation:* Flink CEP sinks alert events asynchronously to an **Amazon SQS FIFO queue**. Lambda consumes from SQS with native event batching (`BatchSize: 10–50`, `BatchingWindowInSeconds: 5`) and concurrency clamps.

2. **Language Standardization on Java 17 with Maven:**
   * *Vulnerability:* PyFlink introduces a 300%–500% CPU serialization tax over gRPC/Py4J and lacks full CEP API coverage (iterative conditions, rich state access, and dynamic pattern timeouts).
   * *Mitigation:* The Flink streaming and CEP core engine is strictly authored in **Java 17**. Python is restricted to out-of-band roles (the synthetic telemetry generator, integration test harnesses, and downstream exploratory notebooks).

3. **Deterministic Idempotency Key & Deduplication Store:**
   * *Vulnerability:* When Flink task failures trigger a recovery replay from a checkpoint, uncommitted alert side-effects are re-evaluated, firing duplicate notifications to operators.
   * *Mitigation:* Every CEP match derives a deterministic token:
     $$\text{IdempotencyKey} = \text{SHA256}(\text{sensor\_id} + \text{pattern\_id} + \text{window\_start} + \text{window\_end})$$
     The Lambda consumer verifies this key against an atomic store with TTL (**Amazon DynamoDB** with 15-minute TTL) before publishing to SNS.

4. **Bounded RocksDB State & Watermark Idleness:**
   * *Vulnerability:* Indefinite accumulation of inactive sensor keys leads to TaskManager disk exhaustion, while quiet Kinesis shards freeze downstream event-time watermarks ($\min(WM_1, \dots, WM_n)$).
   * *Mitigation:* Mandate `StateTtlConfig` (`UpdateType.OnCreateAndWrite`) across all keyed states, enable RocksDB incremental checkpoints (`state.backend.incremental: true`), constrain all CEP patterns (`.within(Time.seconds(30))`), and configure `WatermarkStrategy.withIdleness(Duration.ofSeconds(30))`.

5. **Operational Topology Evolution & Stable UIDs:**
   * *Vulnerability:* Flink auto-generates operator hashes; any code modification or pipeline upgrade invalidates existing savepoints, preventing state restoration.
   * *Mitigation:* Explicit, immutable `.uid("<stable-operator-id>")` assignment for every stateful operator (`KeyedStream`, `Window`, `PatternStream`, `FileSink`), paired with an operational stop-and-resume savepoint protocol.

6. **AWS Glue Partition Projection (Zero-Crawler S3 Sync):**
   * *Vulnerability:* Flink `FileSink` outputs raw Parquet files to S3 but does not register partitions in Glue, requiring scheduled, costly Glue Crawlers ($0.44/DPU-hour).
   * *Mitigation:* Configure **Glue Partition Projection** (`projection.enabled = true`, `projection.dt.type = "date"`, `projection.hour.type = "integer"`), allowing Athena to resolve time-partitioned queries instantaneously without crawlers.

---

## 3. Mathematical Predicates for Complex Event Processing (CEP)

To eliminate ambiguity, the industrial telemetry generator and Flink CEP engine will implement standardized thermodynamic and kinetic anomaly criteria:

1. **Temperature Spike Condition:**
   $$T_t > 85.0^\circ\text{C} \quad \lor \quad \frac{\Delta T}{\Delta t} = \frac{T_t - T_{t-1}}{t_t - t_{t-1}} > 5.0^\circ\text{C}/\text{sec}$$

2. **Abnormal Vibration (Kinetic Anomaly):**
   $$V_{\text{RMS}} = \sqrt{\frac{1}{N}\sum_{i=1}^N v_i^2} > 4.5\text{ mm/s} \quad \lor \quad z\text{-score} = \frac{V_t - \mu_V}{\sigma_V} > 3.0$$

3. **Composite Catastrophic Failure Pattern (CEP Rule):**
   * Event $A$ (`Temperature Spike`) followed within $30\text{ seconds}$ by Event $B$ (`Abnormal Vibration`) for the same `sensor_id`, followed by sustained power draw deviation $> 20\%$.

---

## 4. Goals vs. Non-Goals

### In-Scope Goals
* **Multi-Sensor Synthetic Generator (Python):** Realistic industrial stream with configurable frequency (2,000–10,000 events/sec), injected anomalies, timestamp jitter, and malformed payload injection.
* **Flink Core Engine (Java 17 / Maven):**
  * Ingestion from Kinesis with bounded out-of-order watermarking.
  * Stateful sliding windows (1-minute window, 10-second slide) for baseline statistics.
  * CEP pattern detection for multi-sensor fault signatures.
  * Side-output routing for malformed or late-arriving records.
* **Storage & Catalog:** Partitioned Parquet sink via Flink `FileSink` to Amazon S3, with Glue Partition Projection and Athena analytics.
* **Alerting Buffer & Function:** SQS FIFO queue with batched Lambda execution and DynamoDB deduplication triggering SNS.
* **Dual Execution Infrastructure:**
  1. *Local Runner:* `docker-compose.yml` orchestrating Flink JobManager/TaskManager, LocalStack (Kinesis, S3, SQS, SNS, DynamoDB), and mock telemetry generator.
  2. *Cloud Runner:* Terraform configuration for AWS production deployment.

### Out-of-Scope Non-Goals
* Training online neural networks inside Flink streaming operators.
* Custom frontend UI dashboards (Grafana / Apache Zeppelin query notebooks provided instead).

---

## 5. Directory Structure for the Project

```text
experiments/streaming_flink_kinesis_pipeline/
├── concepts_brief.md                 # Aligned concept brief (EGM Step 1 & 2)
├── docs/
│   └── designs/
│       └── flink_kinesis_streaming.md # Full Technical Specification (EGM Step 3)
├── docker-compose.yml                # LocalStack + Apache Flink cluster
├── Makefile                          # Unified build, test, and run commands
├── generator/                        # Python telemetry generator & load injector
│   ├── generator.py
│   └── requirements.txt
├── flink-processor/                  # Java 17 Apache Flink Streaming & CEP
│   ├── pom.xml
│   └── src/main/java/com/odysseus/streaming/
│       ├── StreamingJob.java
│       ├── model/SensorEvent.java
│       ├── operators/WindowAggregators.java
│       └── cep/AnomalyPatterns.java
├── lambda-alerter/                   # AWS Lambda alert consumer (Python/Node)
│   └── handler.py
└── terraform/                        # AWS Cloud IaC provisioning
    ├── main.tf
    ├── kinesis.tf
    ├── s3_glue.tf
    └── sqs_sns.tf
```
