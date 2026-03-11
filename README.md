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

## Project Structure

```markdown
distributed-data-pipeline/
│
├── pipeline/
│   ├── bronze-ingest/
│   │   ├── bronze_ingest_stream.py
│   │   └── schema.py
│   │
│   ├── silver-transform/
│   │   ├── silver_transform_job.py
│   │   └── quality_rules.py
│   │
│   ├── gold-aggregations/
│   │   ├── gold_aggregations_job.py
│   │   └── feature_engineering.py
│   │
│   └── common/
│       ├── utils.py
│       ├── logging_config.py
│       └── config.yaml
│
├── generator/
│   └── iot_telemetry_generator.py
│
├── data/
│   ├── bronze_input/
│   ├── delta/
│   └── samples/
│
├── monitoring/
│   ├── dashboards/
│   └── metrics_collector.py
│
├── infra/
│   ├── docker/
│   │   ├── Dockerfile
│   │   └── docker-compose.yml
│   └── spark_cluster/
│       ├── spark-master/
│       └── spark-worker/
│
├── notebooks/
│   ├── exploration.ipynb
│   └── anomaly_detection.ipynb
│
├── tests/
│   ├── test_bronze.py
│   ├── test_silver.py
│   └── test_gold.py
│
├── architecture-diagram.png
└── README.md