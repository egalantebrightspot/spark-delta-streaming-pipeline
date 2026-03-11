"""Shared pytest fixtures and helpers for the test suite.

Provides a single session-scoped SparkSession, reusable test-data helpers,
and the canonical quality/gold config constants used across all test modules.
"""

import json
import pytest
from pathlib import Path

from pyspark.sql.functions import current_timestamp

from pipeline.bronze_ingest.schema import IOT_TELEMETRY_SCHEMA, CORRUPT_RECORD_COLUMN
from pipeline.bronze_ingest.bronze_ingest_stream import add_ingestion_metadata
from pipeline.silver_transform.quality_rules import (
    drop_corrupt_records,
    drop_nulls,
    deduplicate,
    enforce_schema,
    normalize_units,
    tag_anomalies,
    add_zscores,
    add_quality_score,
)


# ── Shared config constants ──────────────────────────────────────────────────

QUALITY_CONFIG = {
    "quality": {
        "max_temperature": 150.0,
        "min_temperature": -50.0,
        "max_humidity": 100.0,
        "min_humidity": 0.0,
        "max_pressure": 1100.0,
        "min_pressure": 900.0,
        "expected": {
            "temperature": {"mean": 22.0, "stddev": 5.0},
            "humidity": {"mean": 55.0, "stddev": 10.0},
            "pressure": {"mean": 1013.25, "stddev": 10.0},
        },
    },
    "gold": {
        "window_duration": "1 hour",
        "health_weights": {"quality": 0.4, "anomaly_rate": 0.3, "battery": 0.3},
        "risk_tiers": {"healthy": 0.8, "warning": 0.5},
    },
}


# ── Session-scoped Spark fixture ─────────────────────────────────────────────


@pytest.fixture(scope="session")
def spark():
    """Single SparkSession shared across the entire test run.

    Using one session avoids the latent teardown conflict where multiple
    per-module fixtures each call ``stop()`` on the singleton returned
    by ``getOrCreate()``.
    """
    from pipeline.common.utils import get_spark_session

    session = get_spark_session({
        "spark": {
            "master": "local[1]",
            "app_name": "test-pipeline",
            "shuffle_partitions": 1,
            "log_level": "WARN",
        }
    })
    yield session
    session.stop()


# ── Test data helpers ────────────────────────────────────────────────────────


def make_event(**overrides) -> dict:
    """Build a single IoT telemetry event dict with sensible defaults."""
    base = {
        "device_id": "device-0001",
        "timestamp": "2025-06-01T12:00:00.000000+00:00",
        "temperature": 22.5,
        "humidity": 55.0,
        "pressure": 1013.25,
        "battery_level": 88.0,
        "location": "factory-floor-A",
        "firmware_version": "1.0.0",
    }
    base.update(overrides)
    return base


def write_json(path, events):
    """Write a list of events (dicts or raw strings) as newline-delimited JSON."""
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        for e in events:
            if isinstance(e, str):
                f.write(e + "\n")
            else:
                f.write(json.dumps(e) + "\n")


def read_bronze(spark, path):
    """Read JSON with the Bronze schema and add ingestion metadata."""
    df = (
        spark.read
        .schema(IOT_TELEMETRY_SCHEMA)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT_RECORD_COLUMN)
        .json(str(path))
    )
    return add_ingestion_metadata(df)


def build_silver(spark, json_path, config=None):
    """Apply the full Bronze-to-Silver transform chain on raw JSON files."""
    if config is None:
        config = QUALITY_CONFIG
    df = read_bronze(spark, json_path)
    df = drop_corrupt_records(df)
    df = drop_nulls(df)
    df = deduplicate(df)
    df = enforce_schema(df)
    df = normalize_units(df)
    df = tag_anomalies(df, config)
    df = add_zscores(df, config)
    df = add_quality_score(df)
    return df.withColumn("_processed_at", current_timestamp())
