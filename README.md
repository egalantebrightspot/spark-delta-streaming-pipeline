# distributed-data-pipeline

A distributed, real‑time data engineering system built with **Apache Spark Structured Streaming** and **Delta Lake**, designed to ingest IoT telemetry, enforce data quality and governance, and produce analytics‑ready and ML‑ready datasets across Bronze, Silver, and Gold layers. The project demonstrates enterprise‑grade engineering practices including schema enforcement, ACID transactions, observability, and cloud‑agnostic deployment.

---

## Overview

Modern organizations rely on continuous streams of telemetry from devices, sensors, and applications. This project implements a scalable, fault‑tolerant pipeline that processes these streams in real time, ensuring data quality, lineage, and reliability from ingestion to analytics.

The system follows the **Medallion Architecture**:

- **Bronze** — Raw ingestion with schema enforcement and ACID storage  
- **Silver** — Validation, deduplication, anomaly tagging, and enrichment  
- **Gold** — Aggregations, feature tables, and analytics‑ready outputs  

The pipeline is fully cloud‑agnostic and can run locally via Docker or on Databricks, EMR, or Azure Synapse.

---

## Architecture

The architecture consists of:

- **Synthetic IoT Telemetry Generator** producing realistic streaming JSON events  
- **Spark Structured Streaming** ingesting raw data into Delta Lake  
- **Governance-first Silver layer** applying quality rules and enrichment  
- **Gold layer** producing aggregated metrics and ML feature tables  
- **Monitoring layer** capturing throughput, latency, and anomaly trends  
- **Infrastructure layer** providing a local Spark cluster via Docker  

---

## Key Features

### Real-Time Streaming
Processes continuous telemetry streams with exactly-once semantics and checkpointing.

### Delta Lake Storage
Provides ACID transactions, schema evolution, time travel, and scalable storage.

### Governance & Data Quality
Includes schema enforcement, deduplication, anomaly tagging, and validation rules.

### ML-Ready Outputs
Generates feature tables and aggregated metrics for downstream modeling.

### Observability
Captures operational metrics and supports lightweight dashboards.

### Cloud-Agnostic Deployment
Runs locally or on any Spark-compatible cloud platform.

---

## Data Retention Strategy

Each medallion layer follows a distinct retention policy reflecting its role in the pipeline:

| Layer | Retention | Rationale |
|-------|-----------|-----------|
| **Bronze** | **7 days** | Raw landing data is transient; once validated and promoted to Silver, the raw files and Delta versions serve only as a short-term replay buffer for reprocessing or debugging. VACUUM reclaims storage after 7 days. |
| **Silver** | **30 days** | Cleaned and enriched records support ad-hoc investigation, quality audits, and backfill scenarios. Thirty days balances storage cost against operational usefulness. |
| **Gold** | **Indefinite** | Aggregated metrics, health scores, and ML feature tables are compact and serve as the system of record for analytics and model training. No automatic deletion. |

Retention is enforced by the `VACUUM` commands built into the Gold batch job (and optionally the Silver job). Delta Lake's `VACUUM` physically removes data files older than the retention threshold while preserving the current table state.

---

## Delta Lake Optimization

The pipeline includes built-in Delta Lake maintenance commands that run after each Gold batch aggregation:

- **`OPTIMIZE`** — Compacts small files into larger ones for faster downstream reads (bin-packing). This is critical after streaming jobs that produce many small Parquet files.
- **`VACUUM`** — Removes data files no longer referenced by the Delta transaction log, reclaiming disk space according to the retention policy above.

These commands are also available for manual use:

```python
from delta.tables import DeltaTable

dt = DeltaTable.forPath(spark, "data/delta/silver")
dt.optimize().executeCompaction()
dt.vacuum(retentionHours=24 * 30)  # 30-day retention for Silver
```

---

## Project Structure

```markdown
distributed-data-pipeline/
│
├── pipeline/
│   ├── bronze_ingest/
│   │   ├── bronze_ingest_stream.py   # Streaming ingestion to Bronze Delta
│   │   └── schema.py                 # IOT_TELEMETRY_SCHEMA definition
│   │
│   ├── silver_transform/
│   │   ├── silver_transform_job.py   # Streaming quality transforms
│   │   └── quality_rules.py          # Pure DataFrame transforms (no UDFs)
│   │
│   ├── gold_aggregations/
│   │   ├── gold_aggregations_job.py  # Batch aggregation + OPTIMIZE/VACUUM
│   │   └── feature_engineering.py    # Windowed aggregation functions
│   │
│   └── common/
│       ├── utils.py                  # SparkSession factory, config loader
│       ├── logging_config.py         # Centralized rotating-log setup
│       └── config.yaml               # Single source of truth for all config
│
├── generator/
│   └── iot_telemetry_generator.py    # Synthetic IoT data producer
│
├── monitoring/
│   ├── dashboard.py                  # Plotly HTML report generator
│   ├── live_dashboard.py             # Live Plotly Dash monitoring server
│   └── metrics_collector.py          # Pipeline metrics dataclass
│
├── scripts/
│   ├── validate_bronze.py            # Bronze layer validation
│   ├── validate_silver.py            # Silver layer validation
│   ├── validate_gold.py              # Full pipeline validation
│   └── generate_dashboard.py         # End-to-end pipeline + dashboard
│
├── notebooks/
│   ├── exploration.ipynb             # Medallion data evolution walkthrough
│   ├── anomaly_detection.ipynb       # Anomaly analysis & risk profiling
│   ├── pipeline_showcase.ipynb       # End-to-end showcase for reviewers
│   └── ml_anomaly_model.ipynb        # Random Forest on Gold ML features
│
├── tests/
│   ├── conftest.py                   # Shared fixtures, session-scoped Spark
│   ├── test_config.py                # Config loading tests
│   ├── test_bronze.py                # Bronze schema & ingestion tests
│   ├── test_silver.py                # Silver quality rules tests
│   ├── test_gold.py                  # Gold aggregation tests
│   └── test_integration.py           # Cross-layer integration tests
│
├── infra/docker/
│   ├── Dockerfile
│   ├── docker-compose.yml
│   ├── config.docker.yaml
│   └── entrypoint.sh
│
├── data/                             # Runtime data (gitignored)
│   ├── bronze_input/
│   └── delta/
│
├── requirements.txt
├── pyproject.toml
└── README.md