"""Tests for the Gold aggregation layer."""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import Row
from datetime import datetime

from pipeline.gold_aggregations.feature_engineering import compute_device_features


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("test-gold")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_compute_device_features_aggregates(spark):
    data = [
        Row(device_id="d-001", timestamp=datetime(2025, 1, 1, 0, 0, 0),
            temperature=22.0, humidity=55.0, pressure=1013.0,
            battery_level=90.0, _anomaly=False),
        Row(device_id="d-001", timestamp=datetime(2025, 1, 1, 0, 30, 0),
            temperature=24.0, humidity=60.0, pressure=1014.0,
            battery_level=88.0, _anomaly=False),
        Row(device_id="d-001", timestamp=datetime(2025, 1, 1, 0, 15, 0),
            temperature=300.0, humidity=55.0, pressure=1013.0,
            battery_level=90.0, _anomaly=True),
    ]
    df = spark.createDataFrame(data)
    result = compute_device_features(df, window_duration="1 hour")

    rows = result.collect()
    assert len(rows) == 1
    assert rows[0]["reading_count"] == 2
    assert rows[0]["avg_temperature"] == pytest.approx(23.0, abs=0.01)
