"""Tests for the Gold aggregation layer.

Creates a Silver-like DataFrame from JSON test data by applying the Silver
transforms, then tests each Gold aggregation function against it.
All Spark operations are JVM-native (no Python UDFs).
"""

import json
import pytest
from pathlib import Path

from pyspark.sql.functions import col, current_timestamp

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
from pipeline.gold_aggregations.feature_engineering import (
    compute_device_summary,
    compute_anomaly_summary,
    compute_device_health,
    compute_ml_features,
)
from pipeline.gold_aggregations.gold_aggregations_job import (
    gold_table_path,
    build_and_write,
    GOLD_TABLES,
)

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


@pytest.fixture(scope="session")
def spark():
    from pipeline.common.utils import get_spark_session

    session = get_spark_session({
        "spark": {
            "master": "local[1]",
            "app_name": "test-gold",
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
        "temperature": 22.0,
        "humidity": 55.0,
        "pressure": 1013.25,
        "battery_level": 88.0,
        "location": "factory-floor-A",
        "firmware_version": "1.0.0",
    }
    base.update(overrides)
    return base


def _write_json(path, events):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        for e in events:
            f.write(json.dumps(e) + "\n")


def _build_silver(spark, json_path) -> "DataFrame":
    """Read JSON with Bronze schema and apply full Silver transform chain."""
    df = (
        spark.read
        .schema(IOT_TELEMETRY_SCHEMA)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT_RECORD_COLUMN)
        .json(str(json_path))
    )
    df = add_ingestion_metadata(df)
    df = drop_corrupt_records(df)
    df = drop_nulls(df)
    df = deduplicate(df)
    df = enforce_schema(df)
    df = normalize_units(df)
    df = tag_anomalies(df, QUALITY_CONFIG)
    df = add_zscores(df, QUALITY_CONFIG)
    df = add_quality_score(df)
    return df.withColumn("_processed_at", current_timestamp())


@pytest.fixture(scope="session")
def silver_df(spark, tmp_path_factory):
    """Session-scoped Silver DataFrame with known characteristics."""
    data_dir = tmp_path_factory.mktemp("gold_test_data")
    events = [
        # device-0001: 3 normal events in the same hour
        _event(device_id="device-0001", timestamp="2025-06-01T12:00:00+00:00",
               temperature=20.0, humidity=50.0, battery_level=90.0),
        _event(device_id="device-0001", timestamp="2025-06-01T12:01:00+00:00",
               temperature=22.0, humidity=55.0, battery_level=89.0),
        _event(device_id="device-0001", timestamp="2025-06-01T12:02:00+00:00",
               temperature=24.0, humidity=60.0, battery_level=88.0),
        # device-0002: 2 events, one anomalous temperature
        _event(device_id="device-0002", timestamp="2025-06-01T12:00:00+00:00",
               temperature=22.0, humidity=55.0, battery_level=75.0),
        _event(device_id="device-0002", timestamp="2025-06-01T12:01:00+00:00",
               temperature=200.0, humidity=55.0, battery_level=74.0),
        # device-0003: 1 event with anomalous pressure and low battery
        _event(device_id="device-0003", timestamp="2025-06-01T12:00:00+00:00",
               temperature=22.0, humidity=55.0, pressure=1200.0,
               battery_level=15.0),
    ]
    _write_json(data_dir / "test.json", events)
    return _build_silver(spark, data_dir)


# ── compute_device_summary ───────────────────────────────────────────────────


class TestDeviceSummary:
    def test_one_row_per_device_per_window(self, silver_df):
        result = compute_device_summary(silver_df, "1 hour")
        assert result.count() == 3  # 3 devices

    def test_event_counts(self, silver_df):
        result = compute_device_summary(silver_df, "1 hour")
        rows = {r["device_id"]: r for r in result.collect()}
        assert rows["device-0001"]["event_count"] == 3
        assert rows["device-0002"]["event_count"] == 2
        assert rows["device-0003"]["event_count"] == 1

    def test_avg_temperature(self, silver_df):
        result = compute_device_summary(silver_df, "1 hour")
        d1 = [r for r in result.collect() if r["device_id"] == "device-0001"][0]
        assert d1["avg_temperature"] == 22.0  # (20+22+24)/3

    def test_anomaly_rate(self, silver_df):
        result = compute_device_summary(silver_df, "1 hour")
        rows = {r["device_id"]: r for r in result.collect()}
        assert rows["device-0001"]["anomaly_count"] == 0
        assert rows["device-0001"]["anomaly_rate"] == 0.0
        assert rows["device-0002"]["anomaly_count"] == 1
        assert rows["device-0002"]["anomaly_rate"] == 0.5
        assert rows["device-0003"]["anomaly_count"] == 1

    def test_has_window_columns(self, silver_df):
        result = compute_device_summary(silver_df, "1 hour")
        assert "window_start" in result.columns
        assert "window_end" in result.columns

    def test_schema_completeness(self, silver_df):
        result = compute_device_summary(silver_df, "1 hour")
        expected = {
            "device_id", "window_start", "window_end", "event_count",
            "avg_temperature", "stddev_temperature",
            "min_temperature", "max_temperature",
            "avg_humidity", "avg_pressure", "avg_battery_level",
            "anomaly_count", "anomaly_rate", "avg_quality_score",
        }
        assert expected == set(result.columns)


# ── compute_anomaly_summary ──────────────────────────────────────────────────


class TestAnomalySummary:
    def test_per_type_counts(self, silver_df):
        result = compute_anomaly_summary(silver_df, "1 hour")
        rows = {r["device_id"]: r for r in result.collect()}

        assert rows["device-0001"]["total_anomaly_count"] == 0
        assert rows["device-0002"]["temp_anomaly_count"] == 1
        assert rows["device-0002"]["humidity_anomaly_count"] == 0
        assert rows["device-0003"]["pressure_anomaly_count"] == 1

    def test_anomaly_rate_calculation(self, silver_df):
        result = compute_anomaly_summary(silver_df, "1 hour")
        d2 = [r for r in result.collect() if r["device_id"] == "device-0002"][0]
        assert d2["anomaly_rate"] == 0.5  # 1 anomaly / 2 events

    def test_schema_completeness(self, silver_df):
        result = compute_anomaly_summary(silver_df, "1 hour")
        expected = {
            "device_id", "window_start", "window_end", "total_events",
            "temp_anomaly_count", "humidity_anomaly_count",
            "pressure_anomaly_count", "total_anomaly_count", "anomaly_rate",
        }
        assert expected == set(result.columns)


# ── compute_device_health ────────────────────────────────────────────────────


class TestDeviceHealth:
    def test_healthy_device_gets_high_score(self, silver_df):
        result = compute_device_health(silver_df, QUALITY_CONFIG, "1 hour")
        d1 = [r for r in result.collect() if r["device_id"] == "device-0001"][0]
        assert d1["health_score"] > 0.8
        assert d1["risk_tier"] == "healthy"

    def test_anomalous_device_lower_score(self, silver_df):
        result = compute_device_health(silver_df, QUALITY_CONFIG, "1 hour")
        rows = {r["device_id"]: r for r in result.collect()}
        assert rows["device-0002"]["health_score"] < rows["device-0001"]["health_score"]

    def test_low_battery_reduces_score(self, silver_df):
        result = compute_device_health(silver_df, QUALITY_CONFIG, "1 hour")
        d3 = [r for r in result.collect() if r["device_id"] == "device-0003"][0]
        # Low battery (15%) + anomaly -> lower score
        assert d3["health_score"] < 0.8

    def test_risk_tiers_assigned(self, silver_df):
        result = compute_device_health(silver_df, QUALITY_CONFIG, "1 hour")
        tiers = {r["device_id"]: r["risk_tier"] for r in result.collect()}
        assert all(t in ("healthy", "warning", "critical") for t in tiers.values())

    def test_schema_completeness(self, silver_df):
        result = compute_device_health(silver_df, QUALITY_CONFIG, "1 hour")
        expected = {
            "device_id", "window_start", "window_end", "event_count",
            "avg_quality_score", "anomaly_rate", "avg_battery_level",
            "health_score", "risk_tier",
        }
        assert expected == set(result.columns)


# ── compute_ml_features ──────────────────────────────────────────────────────


class TestMLFeatures:
    def test_one_row_per_device_window(self, silver_df):
        result = compute_ml_features(silver_df, "1 hour")
        assert result.count() == 3

    def test_sensor_ranges(self, silver_df):
        result = compute_ml_features(silver_df, "1 hour")
        d1 = [r for r in result.collect() if r["device_id"] == "device-0001"][0]
        assert d1["temp_min"] == 20.0
        assert d1["temp_max"] == 24.0
        assert d1["temp_range"] == 4.0

    def test_zscore_features_populated(self, silver_df):
        result = compute_ml_features(silver_df, "1 hour")
        row = result.first()
        assert row["temp_zscore_abs_mean"] is not None
        assert row["hum_zscore_abs_mean"] is not None
        assert row["pres_zscore_abs_mean"] is not None

    def test_anomaly_features(self, silver_df):
        result = compute_ml_features(silver_df, "1 hour")
        d2 = [r for r in result.collect() if r["device_id"] == "device-0002"][0]
        assert d2["anomaly_count"] == 1
        assert d2["temp_anomaly_count"] == 1
        assert d2["anomaly_rate"] == 0.5

    def test_feature_count(self, silver_df):
        result = compute_ml_features(silver_df, "1 hour")
        # 33 feature columns in total
        assert len(result.columns) >= 30

    def test_schema_completeness(self, silver_df):
        result = compute_ml_features(silver_df, "1 hour")
        expected_prefixes = [
            "device_id", "window_start", "window_end", "event_count",
            "temp_mean", "temp_stddev", "temp_min", "temp_max", "temp_range",
            "hum_mean", "hum_stddev", "hum_min", "hum_max", "hum_range",
            "pres_mean", "pres_stddev", "pres_min", "pres_max", "pres_range",
            "battery_mean", "battery_min", "battery_max",
            "anomaly_count", "anomaly_rate",
            "quality_mean", "quality_min",
        ]
        for c in expected_prefixes:
            assert c in result.columns, f"Missing column: {c}"


# ── Edge cases: single event ─────────────────────────────────────────────────


class TestSingleEventEdgeCases:
    """When a device has only one event in a window, stddev should be null
    and min/max should equal the single value."""

    @pytest.fixture()
    def single_event_silver(self, spark, tmp_path_factory):
        d = tmp_path_factory.mktemp("single")
        _write_json(d / "data.json", [
            _event(device_id="solo", temperature=25.0, humidity=60.0,
                   pressure=1010.0, battery_level=80.0),
        ])
        return _build_silver(spark, d)

    def test_stddev_is_null_for_single_event(self, single_event_silver):
        result = compute_device_summary(single_event_silver, "1 hour")
        row = result.first()
        assert row["stddev_temperature"] is None

    def test_min_equals_max_for_single_event(self, single_event_silver):
        result = compute_device_summary(single_event_silver, "1 hour")
        row = result.first()
        assert row["min_temperature"] == row["max_temperature"]

    def test_ml_features_stddev_null(self, single_event_silver):
        result = compute_ml_features(single_event_silver, "1 hour")
        row = result.first()
        assert row["temp_stddev"] is None
        assert row["temp_range"] == 0.0


# ── Edge cases: all anomalous ────────────────────────────────────────────────


class TestAllAnomalousDevice:
    """When every event for a device is anomalous, rates should be 1.0."""

    @pytest.fixture()
    def all_anomaly_silver(self, spark, tmp_path_factory):
        d = tmp_path_factory.mktemp("all_anom")
        events = [
            _event(device_id="bad-sensor",
                   timestamp=f"2025-06-01T12:0{i}:00+00:00",
                   temperature=200.0, battery_level=10.0)
            for i in range(5)
        ]
        _write_json(d / "data.json", events)
        return _build_silver(spark, d)

    def test_anomaly_rate_is_one(self, all_anomaly_silver):
        result = compute_device_summary(all_anomaly_silver, "1 hour")
        row = result.first()
        assert row["anomaly_rate"] == 1.0
        assert row["anomaly_count"] == 5

    def test_anomaly_summary_all_temp(self, all_anomaly_silver):
        result = compute_anomaly_summary(all_anomaly_silver, "1 hour")
        row = result.first()
        assert row["temp_anomaly_count"] == 5
        assert row["humidity_anomaly_count"] == 0
        assert row["pressure_anomaly_count"] == 0

    def test_health_score_is_low(self, all_anomaly_silver):
        result = compute_device_health(
            all_anomaly_silver, QUALITY_CONFIG, "1 hour")
        row = result.first()
        assert row["risk_tier"] in ("warning", "critical")
        assert row["health_score"] < 0.8


# ── Health score formula verification ────────────────────────────────────────


class TestHealthScoreFormula:
    """Verify the exact health_score calculation with known inputs."""

    def test_exact_formula_device_0001(self, silver_df):
        result = compute_device_health(silver_df, QUALITY_CONFIG, "1 hour")
        d1 = [r for r in result.collect() if r["device_id"] == "device-0001"][0]

        quality_w, anomaly_w, battery_w = 0.4, 0.3, 0.3
        avg_quality = d1["avg_quality_score"]
        anomaly_rate = d1["anomaly_rate"]
        avg_battery = d1["avg_battery_level"]

        expected = (quality_w * avg_quality
                    + anomaly_w * (1.0 - anomaly_rate)
                    + battery_w * (avg_battery / 100.0))
        assert d1["health_score"] == pytest.approx(expected, abs=0.001)

    def test_critical_tier_threshold(self, silver_df):
        low_config = {
            **QUALITY_CONFIG,
            "gold": {
                "window_duration": "1 hour",
                "health_weights": {"quality": 0.4, "anomaly_rate": 0.3, "battery": 0.3},
                "risk_tiers": {"healthy": 0.99, "warning": 0.98},
            },
        }
        result = compute_device_health(silver_df, low_config, "1 hour")
        tiers = {r["device_id"]: r["risk_tier"] for r in result.collect()}
        assert "critical" in tiers.values()


# ── Different window durations ───────────────────────────────────────────────


class TestWindowDurations:
    @pytest.fixture()
    def multi_hour_silver(self, spark, tmp_path_factory):
        d = tmp_path_factory.mktemp("multi_hour")
        events = [
            _event(device_id="d1", timestamp="2025-06-01T12:00:00+00:00"),
            _event(device_id="d1", timestamp="2025-06-01T12:30:00+00:00"),
            _event(device_id="d1", timestamp="2025-06-01T13:00:00+00:00"),
            _event(device_id="d1", timestamp="2025-06-01T13:30:00+00:00"),
        ]
        _write_json(d / "data.json", events)
        return _build_silver(spark, d)

    def test_30_minute_window_splits_events(self, multi_hour_silver):
        result = compute_device_summary(multi_hour_silver, "30 minutes")
        assert result.count() >= 2

    def test_2_hour_window_combines_events(self, multi_hour_silver):
        result = compute_device_summary(multi_hour_silver, "2 hours")
        assert result.count() <= 2


# ── Aggregated-at timestamp ──────────────────────────────────────────────────


class TestAggregatedTimestamp:
    def test_aggregated_at_added_by_build_and_write(self, spark, silver_df, tmp_path):
        silver_path = str(tmp_path / "silver_ts")
        silver_df.write.format("delta").mode("overwrite").save(silver_path)

        config = {
            **QUALITY_CONFIG,
            "paths": {
                "delta_silver": silver_path,
                "delta_gold": str(tmp_path / "gold_ts"),
            },
        }
        build_and_write(spark, config)

        for table in GOLD_TABLES:
            path = gold_table_path(config, table)
            df = spark.read.format("delta").load(path)
            assert "_aggregated_at" in df.columns
            ts = df.first()["_aggregated_at"]
            assert ts is not None


# ── ML features battery statistics ───────────────────────────────────────────


class TestMLFeaturesBattery:
    def test_battery_range(self, silver_df):
        result = compute_ml_features(silver_df, "1 hour")
        d1 = [r for r in result.collect() if r["device_id"] == "device-0001"][0]
        assert d1["battery_min"] == 88.0
        assert d1["battery_max"] == 90.0
        assert d1["battery_mean"] == pytest.approx(89.0, abs=0.01)


# ── Delta round-trip ─────────────────────────────────────────────────────────


class TestGoldDeltaRoundTrip:
    def test_build_and_write_produces_all_tables(self, spark, silver_df, tmp_path):
        silver_path = str(tmp_path / "silver")
        silver_df.write.format("delta").mode("overwrite").save(silver_path)

        config = {
            **QUALITY_CONFIG,
            "paths": {
                "delta_silver": silver_path,
                "delta_gold": str(tmp_path / "gold"),
            },
        }

        counts = build_and_write(spark, config)

        assert set(counts.keys()) == {
            "device_summary", "anomaly_summary",
            "device_health", "ml_features",
        }
        for name, cnt in counts.items():
            assert cnt > 0, f"{name} produced 0 rows"

    def test_gold_tables_are_delta(self, spark, silver_df, tmp_path):
        silver_path = str(tmp_path / "silver_rt")
        silver_df.write.format("delta").mode("overwrite").save(silver_path)

        gold_base = str(tmp_path / "gold_rt")
        config = {
            **QUALITY_CONFIG,
            "paths": {
                "delta_silver": silver_path,
                "delta_gold": gold_base,
            },
        }

        build_and_write(spark, config)

        for table in ["device_summary", "anomaly_summary",
                      "device_health", "ml_features"]:
            path = gold_table_path(config, table)
            df = spark.read.format("delta").load(path)
            assert "_aggregated_at" in df.columns
            assert df.count() > 0
