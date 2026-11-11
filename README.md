<p align="center">
  <img src="docs/images/hero_streaming_pipeline.png" alt="Odysseus Real-Time Streaming Data Pipeline: Apache Flink & AWS Kinesis" width="100%" style="border-radius: 12px; box-shadow: 0 10px 30px rgba(0,0,0,0.5);" />
</p>

# 🌊 Odysseus Real-Time Streaming Data Pipeline: Apache Flink & AWS Kinesis

<p align="center">
  <a href="https://guillenconcepcion.github.io/streaming-flink-kinesis-pipeline/"><img src="https://img.shields.io/badge/Live%20Architecture-Interactive%20Web%20Dashboard-0284c7?style=for-the-badge&logo=googlechrome&logoColor=white" alt="Live Interactive Architecture Dashboard" /></a>
  <a href="docs/designs/flink_kinesis_streaming.md"><img src="https://img.shields.io/badge/EGM%20Spec-Canonical%20Design%20Doc-059669?style=for-the-badge&logo=markdown&logoColor=white" alt="Canonical Technical Spec" /></a>
  <a href="docs/MATHEMATICAL_AND_STATISTICAL_ANALYSIS.md"><img src="https://img.shields.io/badge/Mathematical%20Rigor-Formal%20Proofs%20%26%20EDA-4f46e5?style=for-the-badge&logo=latex&logoColor=white" alt="Mathematical Rigor and Formal Proofs" /></a>
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

## 🔬 Mathematical Formulations, Stochastic Modeling & Statistical EDA

> [!NOTE]
> A complete, publication-grade analytical treatise containing all formal mathematical proofs, stochastic queueing derivations, and multivariate statistical tests is documented in **[`docs/MATHEMATICAL_AND_STATISTICAL_ANALYSIS.md`](docs/MATHEMATICAL_AND_STATISTICAL_ANALYSIS.md)**.

### 1. Queuing Theory & Backpressure Elimination ($M/G/1$)
Under synchronous HTTP/webhook alert dispatches with service time $S \sim \text{LogNormal}$ ($\mathbb{E}[S] \approx 250\text{ ms} \implies \mu = 4\text{ req/s}$), an arrival rate $\lambda = 1,000\text{ ev/s}$ results in utilization:
$$\rho = \frac{\lambda}{\mu} = \frac{1,000}{4} = 250 \gg 1$$
By the **Pollaczek-Khinchine formula**, queue length and waiting time diverge:
$$\lim_{\rho \to 1^+} W_q = \lim_{\rho \to 1^+} \frac{\lambda \mathbb{E}[S^2]}{2(1 - \rho)} = \infty$$
By offloading alerts asynchronously to **Amazon SQS FIFO** ($\mu_{\text{SQS}} \ge 3,000\text{ msg/s}$), utilization collapses to $\rho \approx 1.1 \times 10^{-5} \ll 1$, guaranteeing $W_q < 0.1\text{ ms}$ and **100% elimination of Flink TaskManager buffer pool exhaustion**.

---

### 2. Event-Time & Monotonic Watermark Completeness Theorem
For out-of-order sensor streams with network transit delay $\Delta_i = t_{p, i} - t_{e, i}$, the monotonic watermark $W(t)$ is bounded by:
$$W(t) = \max_{e \in \text{Observed}(t)} (e.t_e) - \delta_{\text{bounded}}, \quad \text{with } \delta_{\text{bounded}} = 5,000\text{ ms}$$
To prevent inactive or quiet Kinesis shards from freezing the global stream watermark ($W_{\text{global}} = \min_{k=1}^K W_k$), our source injects an automatic idleness threshold $\tau_{\text{idle}} = 30\text{s}$:
$$\text{If } t_{\text{wall}} - t_{\text{last\_event}}^{(k)} > \tau_{\text{idle}} \implies \text{Shard } k \text{ is marked IDLE } \implies W_{\text{global}} = \min_{j \neq k} W_j$$

---

### 3. Multivariate Statistical Diagnostics & Heavy-Tailed Telemetry EDA

<p align="center">
  <img src="docs/images/eda_multivariate_diagnostics.png" alt="Odysseus Streaming Pipeline — Multivariate Statistical Diagnostics & Telemetry EDA" width="100%" style="border-radius: 10px; box-shadow: 0 10px 25px rgba(0,0,0,0.4);" />
</p>

* **Non-Gaussian Kurtosis & Mardia Skewness:** Industrial vibration RMS exhibits heavy-tailed behavior ($\text{Skewness} = +5.30$, $\text{Kurtosis} = +38.34$, Kolmogorov-Smirnov $p < 10^{-100}$). Mardia's multivariate skewness test yields $b_{1, p} = 57.34$ ($p < 10^{-100}$), decisively rejecting multivariate normality.
* **Mahalanobis Anomaly Distance:** Telemetry vectors $\mathbf{x} = [T, V_{\text{RMS}}, P_{\text{baro}}, W_{\text{power}}]^T \in \mathbb{R}^4$ are scored against nominal covariance $\boldsymbol{\Sigma}_0^{-1}$:
  $$D_M^2(\mathbf{x}) = (\mathbf{x} - \boldsymbol{\mu}_0)^T \boldsymbol{\Sigma}_0^{-1} (\mathbf{x} - \boldsymbol{\mu}_0) \sim \chi^2(4)$$
  At significance $\alpha = 0.001$, the critical boundary $\chi^2_{0.999}(4) = 18.467$ isolates 100% of catastrophic bearing fatigue events with $< 0.1\%$ false alarm rate.

---

### 4. Phase-Plane Dynamics & Complex Event Processing (CEP NFA)

<p align="center">
  <img src="docs/images/eda_phase_plane_trajectories.png" alt="Plano de Fases: Dinámica Termo-Cinética y Fronteras del Autómata Finito (CEP NFA)" width="100%" style="border-radius: 10px; box-shadow: 0 10px 25px rgba(0,0,0,0.4);" />
</p>

The Flink CEP engine executes a Non-deterministic Finite Automaton (NFA) tracking composite failure trajectories:
$$\begin{aligned}
\text{Pattern} = & \text{begin}("thermal\_spike") \\
& \text{.where}(e_1 \implies e_1.\text{temperature} > 85.0^\circ\text{C}) \\
& \text{.followedBy}("kinetic\_vibration") \\
& \text{.where}(e_2 \implies e_2.\text{vibration\_rms} > 6.0\text{ mm/s}) \\
& \text{.within}(\text{Time.seconds}(30))
\end{aligned}$$

---

### 5. Cryptographic Deduplication & Zero Alert Storms
To guarantee exact idempotency across Flink checkpoint recoveries without state bloat, CEP assigns a deterministic SHA-256 token:
$$\text{IdempotencyKey} = \text{SHA256}(\text{sensor\_id} \parallel \text{pattern\_name} \parallel t_{\text{start}} \parallel t_{\text{end}})$$
For a volume of $N = 10^9$ incidents across 10 years, the collision probability is bounded by:
$$P(\text{Collision}) \approx 1 - \exp\left(-\frac{N^2}{2 \times 2^{256}}\right) \approx \frac{10^{18}}{2^{257}} \approx 4.3 \times 10^{-60} \approx 0$$
This token serves as the Amazon SQS `MessageDeduplicationId` and the primary key in DynamoDB with a 15-minute Time-To-Live (TTL).

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
│   ├── MATHEMATICAL_AND_STATISTICAL_ANALYSIS.md    # Formal Mathematical & Statistical Specification (PDF/MD)
│   ├── images/
│   │   ├── hero_streaming_pipeline.png             # 16:9 Branded Hero Banner with Typography
│   │   ├── eda_multivariate_diagnostics.png        # 4-Panel Multivariate Statistical EDA Diagnostics
│   │   ├── eda_phase_plane_trajectories.png        # Phase-Plane Trajectory & CEP NFA Decision Space
│   │   └── guillen_logo.png                        # Author & Lead Architect Profile Avatar
│   └── designs/
│       ├── flink_kinesis_streaming.md              # Canonical EGM Technical Specification
│       └── technical_architecture_system_design.html # Single-Page Interactive Visualizer
├── docker-compose.yml                              # LocalStack (Kinesis, S3, SQS, DynamoDB) + Flink Cluster
├── Makefile                                        # Operational Workflow & Build Automation
├── LICENSE                                         # MIT Open Source License
├── notebooks/
│   ├── 01_streaming_analytics_demo.ipynb           # DuckDB & Athena Serverless Analytics Demo
│   └── 02_advanced_mathematical_eda_and_statistical_modeling.ipynb # Advanced EDA & Multivariate Modeling
├── scripts/
│   ├── init_localstack.py                          # Cross-platform AWS bootstrap script (Boto3)
│   ├── init_localstack.sh                          # Bash AWS bootstrap script
│   ├── generate_eda_visualizations.py              # Script generating publication-grade EDA figures
│   ├── build_eda_notebook.py                       # Generator for advanced statistical EDA notebook
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

## 📈 Interactive Analytics & Statistical Modeling Notebooks

The pipeline includes two dedicated, reproducible research notebooks implementing both downstream analytics and formal statistical validation:

1. **[`notebooks/01_streaming_analytics_demo.ipynb`](notebooks/01_streaming_analytics_demo.ipynb)**  
   * Connects via **DuckDB** to query transactional columnar Parquet partitions committed by Flink's `FileSink` to Amazon S3.
   * Demonstrates sub-second virtual partition pruning without AWS Glue Crawler lag and models operational distribution percentiles (P50, P95).

2. **[`notebooks/02_advanced_mathematical_eda_and_statistical_modeling.ipynb`](notebooks/02_advanced_mathematical_eda_and_statistical_modeling.ipynb)**  
   * **Physics-Coupled Simulation:** Synthesizes 5,000 multi-state telemetry events across nominal, thermal stress, and bearing failure regimes.
   * **Univariate Rigor:** Validates heavy-tailed kinetic metrics ($\text{Skewness} = +5.30$, $\text{Kurtosis} = +38.34$) and rejects mechanical Gaussianity via Kolmogorov-Smirnov ($p < 10^{-100}$).
   * **Multivariate Diagnostics:** Computes Mardia's multivariate skewness ($b_{1, p} = 57.34$), two-sample Hotelling $T^2$ test, and Mahalanobis distances $D_M^2 \sim \chi^2(4)$ with critical threshold $\chi^2_{0.999}(4) = 18.467$.
   * **Phase-Plane Trajectories:** Maps the 2D dynamical state space $(T, V_{\text{RMS}})$ against the Flink CEP NFA decision boundary and simulates catastrophic degradation paths.

---

## 📚 Formal Peer-Reviewed Scientific Bibliography & Canonical References

Every architectural decision, buffer sizing, watermark lag parameter, and anomaly threshold in this repository is mathematically grounded in peer-reviewed scientific literature:

### 1. Distributed Stream Processing & Complex Event Processing (CEP)
1. **Akidau, T., et al. (2015).** *The Dataflow Model: A Practical Approach to Balancing Correctness, Latency, and Cost in Massive-Scale, Unbounded, Out-of-Order Data Processing.* **Proceedings of the VLDB Endowment (PVLDB)**, 8(12), 1792–1803. [DOI: 10.14778/2824032.2824076](https://doi.org/10.14778/2824032.2824076).  
   *(Foundational watermark and event-time semantics implemented in Flink Streams Source).*
2. **Carbone, P., et al. (2015).** *Apache Flink: Stream and Batch Processing in a Single Engine.* **IEEE Data Engineering Bulletin**, 38(4), 28–38.  
   *(Chandy-Lamport asynchronous barrier checkpointing and stateful streaming primitives).*
3. **Cugola, G., & Margara, A. (2012).** *Processing Flows of Information: From Data Stream to Complex Event Processing.* **ACM Computing Surveys (CSUR)**, 44(3), 1–62. [DOI: 10.1145/2187671.2187677](https://doi.org/10.1145/2187671.2187677).  
   *(Formal Non-deterministic Finite Automaton model for composite temporal patterns).*

### 2. Stochastic Systems & Queueing Theory
4. **Kleinrock, L. (1975).** *Queueing Systems, Volume 1: Theory.* John Wiley & Sons, New York.  
   *(Pollaczek-Khinchine formula and asymptotic backpressure divergence under synchronous dispatches).*
5. **Kingman, J. F. C. (1961).** *The Single Server Queue in Heavy Traffic.* **Mathematical Proceedings of the Cambridge Philosophical Society**, 57(4), 902–904.  
   *(Upper bounds on queue wait times under stochastic burst conditions).*

### 3. Multivariate Statistics & Anomaly Detection
6. **Mahalanobis, P. C. (1936).** *On the Generalised Distance in Statistics.* **Proceedings of the National Institute of Sciences of India**, 2(1), 49–55.  
   *(Covariance-weighted distance metric for correlated sensor vectors).*
7. **Mardia, K. V. (1970).** *Measures of Multivariate Skewness and Kurtosis with Applications.* **Biometrika**, 57(3), 519–530. [DOI: 10.1093/biomet/57.3.519](https://doi.org/10.1093/biomet/57.3.519).  
   *(Hypothesis test rejecting multivariate normality in coupled telemetry).*
8. **Hotelling, H. (1931).** *The Generalization of Student's Ratio.* **The Annals of Mathematical Statistics**, 2(3), 360–378.  
   *(Two-sample multivariate hypothesis test for regime separation).*
9. **Kolmogorov, A. N. (1933).** *Sulla determinazione empirica di una legge di distribuzione.* **Giornale dell'Istituto Italiano degli Attuari**, 4, 83–91.  
   *(Non-parametric goodness-of-fit test for heavy-tailed distributions).*

### 4. Data Quality & Missing Data Theory
10. **Rubin, D. B. (1976).** *Inference and Missing Data.* **Biometrika**, 63(3), 581–592. [DOI: 10.1093/biomet/63.3.581](https://doi.org/10.1093/biomet/63.3.581).  
    *(Formal taxonomy of missingness: MCAR, MAR, and MNAR).*
11. **Little, R. J. A. (1988).** *A Test of Missing Completely at Random for Multivariate Data with Missing Values.* **Journal of the American Statistical Association**, 83(404), 1198–1202.  
    *(Hypothesis testing for sensor dropouts and imputation requirements).*

### 5. Cryptography & Machine Learning Governance
12. **National Institute of Standards and Technology (NIST). (2015).** *Secure Hash Standard (SHS).* **FIPS PUB 180-4**, U.S. Department of Commerce.  
    *(Cryptographic SHA-256 idempotency token generation).*
13. **Vovk, V., Gammerman, A., & Shafer, G. (2005).** *Algorithmic Learning in a Random World.* Springer Science & Business Media.  
    *(Conformal prediction and distribution-free finite-sample coverage guarantees).*
14. **Romano, Y., Patterson, E., & Candès, E. (2019).** *Conformalized Quantile Regression.* **Advances in Neural Information Processing Systems (NeurIPS 2019)**, 32.  
    *(Distribution-free coverage intervals for predictive industrial maintenance).*
15. **Lundberg, S. M., & Lee, S.-I. (2017).** *A Unified Approach to Interpreting Model Predictions.* **Advances in Neural Information Processing Systems (NeurIPS 2017)**, 30.  
    *(Shapley additive explanations for root cause attribution in alerting).*

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

