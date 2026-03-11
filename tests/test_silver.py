"""Tests for the Silver transformation layer."""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import Row

from pipeline.silver_transform.quality_rules import (
    tag_anomalies,
    drop_nulls,
    deduplicate,
)


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("test-silver")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def sample_config():
    return {
        "quality": {
            "max_temperature": 150.0,
            "min_temperature": -50.0,
            "max_humidity": 100.0,
            "min_humidity": 0.0,
        }
    }


def test_tag_anomalies_flags_out_of_range(spark, sample_config):
    data = [
        Row(device_id="d-001", timestamp="2025-01-01T00:00:00", temperature=200.0, humidity=55.0),
        Row(device_id="d-002", timestamp="2025-01-01T00:00:01", temperature=22.0, humidity=55.0),
    ]
    df = spark.createDataFrame(data)
    result = tag_anomalies(df, sample_config)

    rows = result.collect()
    assert rows[0]["_anomaly"] is True
    assert rows[1]["_anomaly"] is False


def test_drop_nulls_removes_missing_keys(spark):
    data = [
        Row(device_id="d-001", timestamp="2025-01-01T00:00:00"),
        Row(device_id=None, timestamp="2025-01-01T00:00:01"),
    ]
    df = spark.createDataFrame(data)
    result = drop_nulls(df)
    assert result.count() == 1


def test_deduplicate_removes_exact_duplicates(spark):
    data = [
        Row(device_id="d-001", timestamp="2025-01-01T00:00:00", value=1.0),
        Row(device_id="d-001", timestamp="2025-01-01T00:00:00", value=1.0),
        Row(device_id="d-002", timestamp="2025-01-01T00:00:01", value=2.0),
    ]
    df = spark.createDataFrame(data)
    result = deduplicate(df)
    assert result.count() == 2
