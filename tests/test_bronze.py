"""Tests for the Bronze ingestion layer.

All tests use JVM-only operations (JSON file reads, built-in Spark functions,
Delta writes) to avoid the Python-worker crash on Windows + Python 3.12+
(SPARK-53759).  No ``createDataFrame`` from Python objects is used.
"""

import json
import pytest
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from pipeline.bronze_ingest.schema import (
    IOT_TELEMETRY_SCHEMA,
    CORRUPT_RECORD_COLUMN,
)
from pipeline.bronze_ingest.bronze_ingest_stream import add_ingestion_metadata


@pytest.fixture(scope="session")
def spark():
    from pipeline.common.utils import get_spark_session

    session = get_spark_session({
        "spark": {
            "master": "local[1]",
            "app_name": "test-bronze",
            "shuffle_partitions": 1,
            "log_level": "WARN",
        }
    })
    yield session
    session.stop()


def _make_event(**overrides) -> dict:
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


def _write_json(path, events):
    with open(path, "w") as f:
        for e in events:
            f.write(json.dumps(e) + "\n")


def _read_with_schema(spark, path):
    return (
        spark.read
        .schema(IOT_TELEMETRY_SCHEMA)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT_RECORD_COLUMN)
        .json(str(path))
    )


# -- Schema structure ---------------------------------------------------------


def test_schema_has_required_fields():
    field_names = [f.name for f in IOT_TELEMETRY_SCHEMA.fields]
    for name in [
        "device_id", "timestamp", "temperature", "humidity",
        "pressure", "battery_level", "location", "firmware_version",
    ]:
        assert name in field_names
    assert CORRUPT_RECORD_COLUMN in field_names


def test_schema_field_types():
    from pyspark.sql.types import StringType, DoubleType, TimestampType

    type_map = {f.name: type(f.dataType) for f in IOT_TELEMETRY_SCHEMA.fields}
    assert type_map["device_id"] is StringType
    assert type_map["timestamp"] is TimestampType
    for numeric in ["temperature", "humidity", "pressure", "battery_level"]:
        assert type_map[numeric] is DoubleType, f"{numeric} should be DoubleType"
    assert type_map["location"] is StringType
    assert type_map["firmware_version"] is StringType
    assert type_map[CORRUPT_RECORD_COLUMN] is StringType


def test_schema_field_count():
    assert len(IOT_TELEMETRY_SCHEMA.fields) == 9


def test_schema_nullable_flags():
    fields = {f.name: f.nullable for f in IOT_TELEMETRY_SCHEMA.fields}
    assert fields["device_id"] is False
    assert fields["timestamp"] is False
    assert fields["temperature"] is True
    assert fields["humidity"] is True


# -- Ingestion metadata (JVM-only via JSON read) -----------------------------


def test_add_ingestion_metadata(spark, tmp_path):
    _write_json(tmp_path / "meta.json", [_make_event()])
    df = _read_with_schema(spark, tmp_path)
    result = add_ingestion_metadata(df)

    assert "_ingested_at" in result.columns
    assert "_source_file" in result.columns
    assert result.count() == 1


# -- JSON parsing / schema enforcement ---------------------------------------


def test_valid_json_parses_correctly(spark, tmp_path):
    _write_json(tmp_path / "valid.json", [_make_event()])
    df = _read_with_schema(spark, tmp_path)

    assert df.count() == 1
    row = df.first()
    assert row["device_id"] == "device-0001"
    assert row["temperature"] == 22.5
    assert row[CORRUPT_RECORD_COLUMN] is None


def test_timestamp_parsed_as_timestamp_type(spark, tmp_path):
    _write_json(tmp_path / "ts.json", [_make_event()])
    df = _read_with_schema(spark, tmp_path)

    ts_field = next(f for f in df.schema.fields if f.name == "timestamp")
    from pyspark.sql.types import TimestampType
    assert isinstance(ts_field.dataType, TimestampType)
    assert df.first()["timestamp"] is not None


def test_multiple_events_in_batch(spark, tmp_path):
    events = [_make_event(device_id=f"device-{i:04d}") for i in range(10)]
    _write_json(tmp_path / "batch.json", events)
    df = _read_with_schema(spark, tmp_path)

    assert df.count() == 10
    assert df.select("device_id").distinct().count() == 10


def test_corrupt_json_captured(spark, tmp_path):
    (tmp_path / "corrupt.json").write_text("this is not json\n")
    df = _read_with_schema(spark, tmp_path)

    assert df.count() == 1
    row = df.first()
    assert row[CORRUPT_RECORD_COLUMN] is not None
    assert row["device_id"] is None


def test_mixed_valid_and_corrupt(spark, tmp_path):
    with open(tmp_path / "mixed.json", "w") as f:
        f.write(json.dumps(_make_event()) + "\n")
        f.write("bad record\n")
        f.write(json.dumps(_make_event(device_id="device-0002")) + "\n")

    df = _read_with_schema(spark, tmp_path).cache()

    assert df.count() == 3
    valid = df.filter(col(CORRUPT_RECORD_COLUMN).isNull())
    corrupt = df.filter(col(CORRUPT_RECORD_COLUMN).isNotNull())
    assert valid.count() == 2
    assert corrupt.count() == 1
    df.unpersist()


# -- Schema enforcement edge cases --------------------------------------------


def test_extra_json_fields_ignored(spark, tmp_path):
    """Fields not in the schema should be silently dropped."""
    event = _make_event()
    event["rogue_field"] = "should_not_appear"
    event["extra_number"] = 999
    _write_json(tmp_path / "extra.json", [event])
    df = _read_with_schema(spark, tmp_path)

    assert df.count() == 1
    assert "rogue_field" not in df.columns
    assert "extra_number" not in df.columns
    assert df.first()["device_id"] == "device-0001"


def test_wrong_type_for_numeric_field(spark, tmp_path):
    """A string in a DoubleType field should produce a corrupt record."""
    raw = '{"device_id":"d1","timestamp":"2025-06-01T12:00:00+00:00","temperature":"not_a_number","humidity":55.0,"pressure":1013.0,"battery_level":88.0,"location":"A","firmware_version":"1.0.0"}\n'
    (tmp_path / "badtype.json").write_text(raw)
    df = _read_with_schema(spark, tmp_path)

    row = df.first()
    assert row["temperature"] is None or row[CORRUPT_RECORD_COLUMN] is not None


def test_empty_json_file(spark, tmp_path):
    (tmp_path / "empty.json").write_text("")
    df = _read_with_schema(spark, tmp_path)
    assert df.count() == 0


def test_null_device_id_in_json(spark, tmp_path):
    """device_id: null in valid JSON structure should still parse."""
    raw = '{"device_id":null,"timestamp":"2025-06-01T12:00:00+00:00","temperature":22.0,"humidity":55.0,"pressure":1013.0,"battery_level":88.0,"location":"A","firmware_version":"1.0.0"}\n'
    (tmp_path / "nullid.json").write_text(raw)
    df = _read_with_schema(spark, tmp_path)

    assert df.count() == 1
    assert df.first()["device_id"] is None


def test_partial_json_missing_optional_fields(spark, tmp_path):
    """JSON with only required fields should parse without corrupt flag."""
    raw = '{"device_id":"d1","timestamp":"2025-06-01T12:00:00+00:00"}\n'
    (tmp_path / "partial.json").write_text(raw)
    df = _read_with_schema(spark, tmp_path)

    row = df.first()
    assert row["device_id"] == "d1"
    assert row["temperature"] is None
    assert row["humidity"] is None
    assert row[CORRUPT_RECORD_COLUMN] is None


def test_numeric_precision_preserved(spark, tmp_path):
    _write_json(tmp_path / "precision.json", [
        _make_event(temperature=22.123456789),
    ])
    df = _read_with_schema(spark, tmp_path)
    temp = df.first()["temperature"]
    assert abs(temp - 22.123456789) < 1e-6


# -- Delta round-trip ---------------------------------------------------------


def test_bronze_delta_round_trip(spark, tmp_path):
    input_dir = tmp_path / "input"
    delta_dir = tmp_path / "delta_bronze"
    input_dir.mkdir()

    events = [_make_event(device_id=f"device-{i:04d}") for i in range(5)]
    _write_json(input_dir / "batch.json", events)

    df = _read_with_schema(spark, input_dir)
    enriched = add_ingestion_metadata(df)
    enriched.write.format("delta").mode("overwrite").save(str(delta_dir))

    result = spark.read.format("delta").load(str(delta_dir))
    assert result.count() == 5
    assert "_ingested_at" in result.columns
    assert "_source_file" in result.columns
    assert CORRUPT_RECORD_COLUMN in result.columns
