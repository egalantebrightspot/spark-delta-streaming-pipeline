"""Tests for the Bronze ingestion layer."""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import Row

from pipeline.bronze_ingest.schema import IOT_TELEMETRY_SCHEMA
from pipeline.bronze_ingest.bronze_ingest_stream import add_ingestion_metadata


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("test-bronze")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_schema_has_required_fields():
    field_names = [f.name for f in IOT_TELEMETRY_SCHEMA.fields]
    assert "device_id" in field_names
    assert "timestamp" in field_names
    assert "temperature" in field_names
    assert "humidity" in field_names


def test_add_ingestion_metadata(spark):
    data = [Row(device_id="d-001", timestamp="2025-01-01T00:00:00", temperature=22.0,
                humidity=55.0, pressure=1013.0, battery_level=90.0,
                location="factory", firmware_version="1.0.0")]
    df = spark.createDataFrame(data)
    result = add_ingestion_metadata(df)

    assert "_ingested_at" in result.columns
    assert "_source_file" in result.columns
    assert result.count() == 1
