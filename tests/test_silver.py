"""Tests for the Silver transformation layer.

Same strategy as test_bronze.py: all data comes from JSON files read via
Spark (JVM-only), no createDataFrame from Python objects.
"""

import json
import pytest
from pathlib import Path

from pyspark.sql.functions import col

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
from pipeline.bronze_ingest.schema import IOT_TELEMETRY_SCHEMA, CORRUPT_RECORD_COLUMN
from pipeline.bronze_ingest.bronze_ingest_stream import add_ingestion_metadata


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
}


@pytest.fixture(scope="session")
def spark():
    from pipeline.common.utils import get_spark_session

    session = get_spark_session({
        "spark": {
            "master": "local[1]",
            "app_name": "test-silver",
            "shuffle_partitions": 1,
            "log_level": "WARN",
        }
    })
    yield session
    session.stop()


def _event(**overrides) -> dict:
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
            if isinstance(e, str):
                f.write(e + "\n")
            else:
                f.write(json.dumps(e) + "\n")


def _read_bronze(spark, path):
    """Read JSON with the Bronze schema + add ingestion metadata."""
    df = (
        spark.read
        .schema(IOT_TELEMETRY_SCHEMA)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT_RECORD_COLUMN)
        .json(str(path))
    )
    return add_ingestion_metadata(df)


# ── drop_corrupt_records ─────────────────────────────────────────────────────


class TestDropCorruptRecords:
    def test_removes_corrupt_rows(self, spark, tmp_path):
        _write_json(tmp_path / "mixed.json", [
            _event(),
            "this is not json",
            _event(device_id="device-0002"),
        ])
        df = _read_bronze(spark, tmp_path).cache()
        assert df.count() == 3

        result = drop_corrupt_records(df)
        assert result.count() == 2
        assert "_corrupt_record" not in result.columns
        df.unpersist()

    def test_all_valid_passes_through(self, spark, tmp_path):
        _write_json(tmp_path / "valid.json", [_event(), _event(device_id="d2")])
        df = _read_bronze(spark, tmp_path).cache()
        result = drop_corrupt_records(df)
        assert result.count() == 2
        df.unpersist()

    def test_no_corrupt_column_is_noop(self, spark, tmp_path):
        _write_json(tmp_path / "valid.json", [_event()])
        df = _read_bronze(spark, tmp_path).drop("_corrupt_record")
        result = drop_corrupt_records(df)
        assert result.count() == 1


# ── drop_nulls ───────────────────────────────────────────────────────────────


class TestDropNulls:
    def test_drops_null_device_id(self, spark, tmp_path):
        _write_json(tmp_path / "data.json", [
            _event(),
            "this is not json",
        ])
        df = _read_bronze(spark, tmp_path)
        clean = drop_corrupt_records(df)
        result = drop_nulls(clean)
        assert result.count() == 1

    def test_keeps_rows_with_null_optional_fields(self, spark, tmp_path):
        evt = _event()
        del evt["location"]
        del evt["firmware_version"]
        _write_json(tmp_path / "data.json", [evt])
        df = _read_bronze(spark, tmp_path)
        result = drop_nulls(drop_corrupt_records(df))
        assert result.count() == 1


# ── deduplicate ──────────────────────────────────────────────────────────────


class TestDeduplicate:
    def test_removes_exact_duplicates(self, spark, tmp_path):
        evt = _event()
        _write_json(tmp_path / "data.json", [evt, evt, evt])
        df = _read_bronze(spark, tmp_path)
        result = deduplicate(drop_corrupt_records(df))
        assert result.count() == 1

    def test_keeps_different_timestamps(self, spark, tmp_path):
        _write_json(tmp_path / "data.json", [
            _event(timestamp="2025-06-01T12:00:00+00:00"),
            _event(timestamp="2025-06-01T12:01:00+00:00"),
        ])
        df = _read_bronze(spark, tmp_path)
        result = deduplicate(drop_corrupt_records(df))
        assert result.count() == 2

    def test_keeps_different_devices_same_timestamp(self, spark, tmp_path):
        _write_json(tmp_path / "data.json", [
            _event(device_id="device-0001"),
            _event(device_id="device-0002"),
        ])
        df = _read_bronze(spark, tmp_path)
        result = deduplicate(drop_corrupt_records(df))
        assert result.count() == 2


# ── enforce_schema ───────────────────────────────────────────────────────────


class TestEnforceSchema:
    def test_drops_source_file_column(self, spark, tmp_path):
        _write_json(tmp_path / "data.json", [_event()])
        df = _read_bronze(spark, tmp_path)
        assert "_source_file" in df.columns
        result = enforce_schema(df)
        assert "_source_file" not in result.columns

    def test_preserves_ingested_at(self, spark, tmp_path):
        _write_json(tmp_path / "data.json", [_event()])
        df = _read_bronze(spark, tmp_path)
        result = enforce_schema(df)
        assert "_ingested_at" in result.columns


# ── normalize_units ──────────────────────────────────────────────────────────


class TestNormalizeUnits:
    def test_clamps_negative_humidity_to_zero(self, spark, tmp_path):
        _write_json(tmp_path / "data.json", [_event(humidity=-10.0)])
        df = _read_bronze(spark, tmp_path)
        result = normalize_units(drop_corrupt_records(df))
        assert result.first()["humidity"] == 0.0

    def test_clamps_high_humidity_to_100(self, spark, tmp_path):
        _write_json(tmp_path / "data.json", [_event(humidity=150.0)])
        df = _read_bronze(spark, tmp_path)
        result = normalize_units(drop_corrupt_records(df))
        assert result.first()["humidity"] == 100.0

    def test_normal_humidity_unchanged(self, spark, tmp_path):
        _write_json(tmp_path / "data.json", [_event(humidity=55.0)])
        df = _read_bronze(spark, tmp_path)
        result = normalize_units(drop_corrupt_records(df))
        assert result.first()["humidity"] == 55.0


# ── tag_anomalies ────────────────────────────────────────────────────────────


class TestTagAnomalies:
    def test_normal_readings_no_anomaly(self, spark, tmp_path):
        _write_json(tmp_path / "data.json", [_event()])
        df = drop_corrupt_records(_read_bronze(spark, tmp_path))
        result = tag_anomalies(df, QUALITY_CONFIG)
        row = result.first()
        assert row["_is_anomaly"] is False
        assert row["_is_temp_anomaly"] is False
        assert row["_is_humidity_anomaly"] is False
        assert row["_is_pressure_anomaly"] is False
        assert row["_anomaly_details"] is None

    def test_high_temperature_flagged(self, spark, tmp_path):
        _write_json(tmp_path / "data.json", [_event(temperature=200.0)])
        df = drop_corrupt_records(_read_bronze(spark, tmp_path))
        result = tag_anomalies(df, QUALITY_CONFIG)
        row = result.first()
        assert row["_is_temp_anomaly"] is True
        assert row["_is_anomaly"] is True
        assert "temperature_out_of_range" in row["_anomaly_details"]

    def test_low_temperature_flagged(self, spark, tmp_path):
        _write_json(tmp_path / "data.json", [_event(temperature=-80.0)])
        df = drop_corrupt_records(_read_bronze(spark, tmp_path))
        result = tag_anomalies(df, QUALITY_CONFIG)
        assert result.first()["_is_temp_anomaly"] is True

    def test_pressure_anomaly_flagged(self, spark, tmp_path):
        _write_json(tmp_path / "data.json", [_event(pressure=1200.0)])
        df = drop_corrupt_records(_read_bronze(spark, tmp_path))
        result = tag_anomalies(df, QUALITY_CONFIG)
        row = result.first()
        assert row["_is_pressure_anomaly"] is True
        assert "pressure_out_of_range" in row["_anomaly_details"]

    def test_multiple_anomalies_listed(self, spark, tmp_path):
        _write_json(tmp_path / "data.json", [
            _event(temperature=200.0, pressure=1200.0),
        ])
        df = drop_corrupt_records(_read_bronze(spark, tmp_path))
        result = tag_anomalies(df, QUALITY_CONFIG)
        details = result.first()["_anomaly_details"]
        assert "temperature_out_of_range" in details
        assert "pressure_out_of_range" in details


# ── add_zscores ──────────────────────────────────────────────────────────────


class TestAddZscores:
    def test_zscore_at_mean_is_zero(self, spark, tmp_path):
        _write_json(tmp_path / "data.json", [
            _event(temperature=22.0, humidity=55.0, pressure=1013.25),
        ])
        df = drop_corrupt_records(_read_bronze(spark, tmp_path))
        result = add_zscores(df, QUALITY_CONFIG)
        row = result.first()
        assert row["_temperature_zscore"] == 0.0
        assert row["_humidity_zscore"] == 0.0
        assert row["_pressure_zscore"] == 0.0

    def test_zscore_one_stddev_above(self, spark, tmp_path):
        _write_json(tmp_path / "data.json", [
            _event(temperature=27.0),  # mean=22, stddev=5 -> z=1.0
        ])
        df = drop_corrupt_records(_read_bronze(spark, tmp_path))
        result = add_zscores(df, QUALITY_CONFIG)
        assert result.first()["_temperature_zscore"] == 1.0

    def test_zscore_null_field_is_null(self, spark, tmp_path):
        evt = _event()
        del evt["pressure"]
        _write_json(tmp_path / "data.json", [evt])
        df = drop_corrupt_records(_read_bronze(spark, tmp_path))
        result = add_zscores(df, QUALITY_CONFIG)
        assert result.first()["_pressure_zscore"] is None


# ── add_quality_score ────────────────────────────────────────────────────────


class TestAddQualityScore:
    def test_perfect_score_for_complete_clean_record(self, spark, tmp_path):
        _write_json(tmp_path / "data.json", [_event()])
        df = drop_corrupt_records(_read_bronze(spark, tmp_path))
        df = tag_anomalies(df, QUALITY_CONFIG)
        result = add_quality_score(df)
        assert result.first()["_quality_score"] == 1.0

    def test_anomaly_reduces_score(self, spark, tmp_path):
        _write_json(tmp_path / "data.json", [_event(temperature=200.0)])
        df = drop_corrupt_records(_read_bronze(spark, tmp_path))
        df = tag_anomalies(df, QUALITY_CONFIG)
        result = add_quality_score(df)
        score = result.first()["_quality_score"]
        assert score == pytest.approx(0.9, abs=0.01)

    def test_missing_fields_reduce_score(self, spark, tmp_path):
        evt = _event()
        del evt["location"]
        del evt["firmware_version"]
        _write_json(tmp_path / "data.json", [evt])
        df = drop_corrupt_records(_read_bronze(spark, tmp_path))
        df = tag_anomalies(df, QUALITY_CONFIG)
        result = add_quality_score(df)
        score = result.first()["_quality_score"]
        # 4/6 completeness * 1.0 validity = ~0.6667
        assert 0.6 < score < 0.7


# ── Anomaly boundary tests ───────────────────────────────────────────────────


class TestAnomalyBoundaries:
    def test_temperature_exactly_at_max_not_anomaly(self, spark, tmp_path):
        _write_json(tmp_path / "data.json", [_event(temperature=150.0)])
        df = drop_corrupt_records(_read_bronze(spark, tmp_path))
        result = tag_anomalies(df, QUALITY_CONFIG)
        assert result.first()["_is_temp_anomaly"] is False

    def test_temperature_just_above_max_is_anomaly(self, spark, tmp_path):
        _write_json(tmp_path / "data.json", [_event(temperature=150.01)])
        df = drop_corrupt_records(_read_bronze(spark, tmp_path))
        result = tag_anomalies(df, QUALITY_CONFIG)
        assert result.first()["_is_temp_anomaly"] is True

    def test_temperature_exactly_at_min_not_anomaly(self, spark, tmp_path):
        _write_json(tmp_path / "data.json", [_event(temperature=-50.0)])
        df = drop_corrupt_records(_read_bronze(spark, tmp_path))
        result = tag_anomalies(df, QUALITY_CONFIG)
        assert result.first()["_is_temp_anomaly"] is False

    def test_pressure_exactly_at_max_not_anomaly(self, spark, tmp_path):
        _write_json(tmp_path / "data.json", [_event(pressure=1100.0)])
        df = drop_corrupt_records(_read_bronze(spark, tmp_path))
        result = tag_anomalies(df, QUALITY_CONFIG)
        assert result.first()["_is_pressure_anomaly"] is False

    def test_pressure_just_below_min_is_anomaly(self, spark, tmp_path):
        _write_json(tmp_path / "data.json", [_event(pressure=899.99)])
        df = drop_corrupt_records(_read_bronze(spark, tmp_path))
        result = tag_anomalies(df, QUALITY_CONFIG)
        assert result.first()["_is_pressure_anomaly"] is True


# ── Z-score edge cases ──────────────────────────────────────────────────────


class TestZscoreEdgeCases:
    def test_negative_zscore(self, spark, tmp_path):
        _write_json(tmp_path / "data.json", [_event(temperature=17.0)])
        df = drop_corrupt_records(_read_bronze(spark, tmp_path))
        result = add_zscores(df, QUALITY_CONFIG)
        assert result.first()["_temperature_zscore"] == -1.0

    def test_extreme_outlier_zscore(self, spark, tmp_path):
        _write_json(tmp_path / "data.json", [_event(temperature=200.0)])
        df = drop_corrupt_records(_read_bronze(spark, tmp_path))
        result = add_zscores(df, QUALITY_CONFIG)
        z = result.first()["_temperature_zscore"]
        assert z > 3.0  # (200-22)/5 = 35.6

    def test_zscore_precision(self, spark, tmp_path):
        _write_json(tmp_path / "data.json", [_event(temperature=23.0)])
        df = drop_corrupt_records(_read_bronze(spark, tmp_path))
        result = add_zscores(df, QUALITY_CONFIG)
        assert result.first()["_temperature_zscore"] == 0.2  # (23-22)/5 = 0.2


# ── Normalization edge cases ─────────────────────────────────────────────────


class TestNormalizationEdgeCases:
    def test_humidity_exactly_zero_unchanged(self, spark, tmp_path):
        _write_json(tmp_path / "data.json", [_event(humidity=0.0)])
        df = drop_corrupt_records(_read_bronze(spark, tmp_path))
        result = normalize_units(df)
        assert result.first()["humidity"] == 0.0

    def test_humidity_exactly_100_unchanged(self, spark, tmp_path):
        _write_json(tmp_path / "data.json", [_event(humidity=100.0)])
        df = drop_corrupt_records(_read_bronze(spark, tmp_path))
        result = normalize_units(df)
        assert result.first()["humidity"] == 100.0

    def test_temperature_not_clamped(self, spark, tmp_path):
        """Temperature should NOT be clamped -- left raw for anomaly detection."""
        _write_json(tmp_path / "data.json", [_event(temperature=-80.0)])
        df = drop_corrupt_records(_read_bronze(spark, tmp_path))
        result = normalize_units(df)
        assert result.first()["temperature"] == -80.0


# ── Custom column overrides ──────────────────────────────────────────────────


class TestCustomColumns:
    def test_drop_nulls_custom_required_cols(self, spark, tmp_path):
        evt = _event()
        del evt["location"]
        _write_json(tmp_path / "data.json", [evt])
        df = drop_corrupt_records(_read_bronze(spark, tmp_path))
        result = drop_nulls(df, required_cols=["device_id", "location"])
        assert result.count() == 0

    def test_deduplicate_custom_key(self, spark, tmp_path):
        _write_json(tmp_path / "data.json", [
            _event(device_id="d1", temperature=20.0),
            _event(device_id="d1", temperature=25.0),
        ])
        df = drop_corrupt_records(_read_bronze(spark, tmp_path))
        result = deduplicate(df, key_cols=["device_id"])
        assert result.count() == 1


# ── Quality score edge cases ────────────────────────────────────────────────


class TestQualityScoreEdgeCases:
    def test_all_optional_fields_missing(self, spark, tmp_path):
        raw = '{"device_id":"d1","timestamp":"2025-06-01T12:00:00+00:00"}\n'
        (tmp_path / "data.json").write_text(raw)
        df = _read_bronze(spark, tmp_path)
        df = drop_corrupt_records(df)
        df = tag_anomalies(df, QUALITY_CONFIG)
        result = add_quality_score(df)
        score = result.first()["_quality_score"]
        assert score == 0.0

    def test_anomaly_plus_missing_fields(self, spark, tmp_path):
        evt = _event(temperature=200.0)
        del evt["location"]
        del evt["firmware_version"]
        del evt["battery_level"]
        _write_json(tmp_path / "data.json", [evt])
        df = drop_corrupt_records(_read_bronze(spark, tmp_path))
        df = tag_anomalies(df, QUALITY_CONFIG)
        result = add_quality_score(df)
        score = result.first()["_quality_score"]
        # 3/6 completeness * 0.9 anomaly penalty = 0.45
        assert score == pytest.approx(0.45, abs=0.01)

    def test_quality_score_range(self, spark, tmp_path):
        """Quality scores should always be between 0.0 and 1.0."""
        _write_json(tmp_path / "data.json", [
            _event(),
            _event(temperature=200.0),
        ])
        df = drop_corrupt_records(_read_bronze(spark, tmp_path))
        df = tag_anomalies(df, QUALITY_CONFIG)
        result = add_quality_score(df)
        scores = [r["_quality_score"] for r in result.collect()]
        assert all(0.0 <= s <= 1.0 for s in scores)


# ── Full pipeline (batch simulation) ────────────────────────────────────────


class TestFullSilverPipeline:
    def test_end_to_end_transform(self, spark, tmp_path):
        _write_json(tmp_path / "data.json", [
            _event(device_id="d1", timestamp="2025-06-01T12:00:00+00:00"),
            _event(device_id="d1", timestamp="2025-06-01T12:00:00+00:00"),  # dup
            _event(device_id="d2", timestamp="2025-06-01T12:01:00+00:00",
                   temperature=200.0),
            "this is not json",
        ])
        df = _read_bronze(spark, tmp_path).cache()
        assert df.count() == 4

        config = QUALITY_CONFIG
        df = drop_corrupt_records(df)
        df = drop_nulls(df)
        df = deduplicate(df)
        df = enforce_schema(df)
        df = normalize_units(df)
        df = tag_anomalies(df, config)
        df = add_zscores(df, config)
        df = add_quality_score(df)

        assert df.count() == 2
        assert "_corrupt_record" not in df.columns
        assert "_source_file" not in df.columns
        assert "_ingested_at" in df.columns

        anomalous = df.filter(col("_is_anomaly")).first()
        assert anomalous["device_id"] == "d2"
        assert "temperature_out_of_range" in anomalous["_anomaly_details"]
        assert anomalous["_quality_score"] < 1.0

        clean = df.filter(~col("_is_anomaly")).first()
        assert clean["_quality_score"] == 1.0

    def test_delta_round_trip(self, spark, tmp_path):
        from pyspark.sql.functions import current_timestamp

        input_dir = tmp_path / "input"
        input_dir.mkdir()
        _write_json(input_dir / "data.json", [
            _event(device_id="d1"),
            _event(device_id="d2"),
        ])

        df = _read_bronze(spark, tmp_path / "input")
        df = drop_corrupt_records(df)
        df = deduplicate(df)
        df = enforce_schema(df)
        df = normalize_units(df)
        df = tag_anomalies(df, QUALITY_CONFIG)
        df = add_zscores(df, QUALITY_CONFIG)
        df = add_quality_score(df)
        df = df.withColumn("_processed_at", current_timestamp())

        delta_path = str(tmp_path / "delta_silver")
        df.write.format("delta").mode("overwrite").save(delta_path)

        result = spark.read.format("delta").load(delta_path)
        assert result.count() == 2
        expected_cols = {
            "device_id", "timestamp", "temperature", "humidity", "pressure",
            "battery_level", "location", "firmware_version", "_ingested_at",
            "_is_temp_anomaly", "_is_humidity_anomaly", "_is_pressure_anomaly",
            "_is_anomaly", "_anomaly_details",
            "_temperature_zscore", "_humidity_zscore", "_pressure_zscore",
            "_quality_score", "_processed_at",
        }
        assert expected_cols.issubset(set(result.columns))
