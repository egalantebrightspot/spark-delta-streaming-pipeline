"""Cross-layer integration tests: Bronze -> Silver -> Gold.

Validates the full pipeline flow with realistic data, ensuring that
records flow correctly through all three layers and that data governance
properties are maintained end-to-end.
"""

import pytest
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
from pipeline.gold_aggregations.gold_aggregations_job import (
    build_and_write,
    gold_table_path,
    GOLD_TABLES,
)

from tests.conftest import QUALITY_CONFIG, make_event, write_json


pytestmark = [pytest.mark.integration, pytest.mark.slow]

PIPELINE_CONFIG = {
    **QUALITY_CONFIG,
}


class TestBronzeToSilverToGold:
    """Full pipeline: raw JSON -> Bronze Delta -> Silver Delta -> Gold Delta."""

    def _run_bronze(self, spark, raw_path, bronze_delta_path):
        df = (
            spark.read
            .schema(IOT_TELEMETRY_SCHEMA)
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", CORRUPT_RECORD_COLUMN)
            .json(str(raw_path))
        )
        enriched = add_ingestion_metadata(df)
        enriched.write.format("delta").mode("overwrite").save(str(bronze_delta_path))
        return spark.read.format("delta").load(str(bronze_delta_path))

    def _run_silver(self, bronze_df, silver_delta_path):
        df = drop_corrupt_records(bronze_df)
        df = drop_nulls(df)
        df = deduplicate(df)
        df = enforce_schema(df)
        df = normalize_units(df)
        df = tag_anomalies(df, PIPELINE_CONFIG)
        df = add_zscores(df, PIPELINE_CONFIG)
        df = add_quality_score(df)
        df = df.withColumn("_processed_at", current_timestamp())
        df.write.format("delta").mode("overwrite").save(str(silver_delta_path))
        return df

    def test_full_pipeline_flow(self, spark, tmp_path):
        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()
        bronze_dir = tmp_path / "bronze"
        silver_dir = tmp_path / "silver"
        gold_dir = tmp_path / "gold"

        events = [
            # Clean devices
            make_event(device_id="d1", timestamp="2025-06-01T12:00:00+00:00",
                       temperature=21.0, battery_level=95.0),
            make_event(device_id="d1", timestamp="2025-06-01T12:01:00+00:00",
                       temperature=22.0, battery_level=94.0),
            make_event(device_id="d1", timestamp="2025-06-01T12:02:00+00:00",
                       temperature=23.0, battery_level=93.0),
            # Anomalous device
            make_event(device_id="d2", timestamp="2025-06-01T12:00:00+00:00",
                       temperature=200.0, battery_level=50.0),
            make_event(device_id="d2", timestamp="2025-06-01T12:01:00+00:00",
                       temperature=22.0, battery_level=49.0),
            # Duplicate (should be removed)
            make_event(device_id="d1", timestamp="2025-06-01T12:00:00+00:00",
                       temperature=21.0, battery_level=95.0),
            # Corrupt record (should be removed)
            "this is not valid json",
            # Low battery device with pressure anomaly
            make_event(device_id="d3", timestamp="2025-06-01T12:00:00+00:00",
                       temperature=22.0, pressure=1200.0, battery_level=10.0),
        ]
        write_json(raw_dir / "events.json", events)

        # Bronze
        bronze_df = self._run_bronze(spark, raw_dir, bronze_dir)
        assert bronze_df.count() == 8  # all rows including corrupt/dups

        # Silver
        silver_df = self._run_silver(bronze_df, silver_dir)
        silver_from_delta = spark.read.format("delta").load(str(silver_dir))
        assert silver_from_delta.count() == 6  # 8 - 1 corrupt - 1 dup
        assert "_corrupt_record" not in silver_from_delta.columns
        assert "_source_file" not in silver_from_delta.columns

        # Gold
        config = {
            **PIPELINE_CONFIG,
            "paths": {
                "delta_silver": str(silver_dir),
                "delta_gold": str(gold_dir),
            },
        }
        counts = build_and_write(spark, config)

        assert len(counts) == 4
        for table_name, count in counts.items():
            assert count > 0, f"{table_name} has 0 rows"

    def test_anomaly_propagation_bronze_to_gold(self, spark, tmp_path):
        """Verify that a temp anomaly in raw data shows up in Gold anomaly_summary."""
        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()
        bronze_dir = tmp_path / "bronze"
        silver_dir = tmp_path / "silver"
        gold_dir = tmp_path / "gold"

        events = [
            make_event(device_id="sensor-A", timestamp="2025-06-01T12:00:00+00:00",
                       temperature=200.0),
            make_event(device_id="sensor-A", timestamp="2025-06-01T12:01:00+00:00",
                       temperature=22.0),
        ]
        write_json(raw_dir / "events.json", events)

        bronze_df = self._run_bronze(spark, raw_dir, bronze_dir)
        self._run_silver(bronze_df, silver_dir)

        config = {
            **PIPELINE_CONFIG,
            "paths": {
                "delta_silver": str(silver_dir),
                "delta_gold": str(gold_dir),
            },
        }
        build_and_write(spark, config)

        anomaly_df = spark.read.format("delta").load(
            gold_table_path(config, "anomaly_summary"))
        row = anomaly_df.first()
        assert row["temp_anomaly_count"] == 1
        assert row["anomaly_rate"] == 0.5

    def test_quality_score_survives_all_layers(self, spark, tmp_path):
        """Quality score computed in Silver should influence Gold health."""
        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()
        bronze_dir = tmp_path / "bronze"
        silver_dir = tmp_path / "silver"
        gold_dir = tmp_path / "gold"

        events = [
            make_event(device_id="clean", timestamp="2025-06-01T12:00:00+00:00"),
        ]
        write_json(raw_dir / "events.json", events)

        bronze_df = self._run_bronze(spark, raw_dir, bronze_dir)
        self._run_silver(bronze_df, silver_dir)

        config = {
            **PIPELINE_CONFIG,
            "paths": {
                "delta_silver": str(silver_dir),
                "delta_gold": str(gold_dir),
            },
        }
        build_and_write(spark, config)

        health_df = spark.read.format("delta").load(
            gold_table_path(config, "device_health"))
        row = health_df.first()
        assert row["avg_quality_score"] == 1.0
        assert row["risk_tier"] == "healthy"

    def test_dedup_prevents_inflated_counts(self, spark, tmp_path):
        """Duplicates removed in Silver should not inflate Gold counts."""
        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()
        bronze_dir = tmp_path / "bronze"
        silver_dir = tmp_path / "silver"
        gold_dir = tmp_path / "gold"

        same_event = make_event(device_id="duper",
                                timestamp="2025-06-01T12:00:00+00:00")
        events = [same_event] * 10
        write_json(raw_dir / "events.json", events)

        bronze_df = self._run_bronze(spark, raw_dir, bronze_dir)
        assert bronze_df.count() == 10

        self._run_silver(bronze_df, silver_dir)
        silver_df = spark.read.format("delta").load(str(silver_dir))
        assert silver_df.count() == 1

        config = {
            **PIPELINE_CONFIG,
            "paths": {
                "delta_silver": str(silver_dir),
                "delta_gold": str(gold_dir),
            },
        }
        build_and_write(spark, config)

        summary = spark.read.format("delta").load(
            gold_table_path(config, "device_summary"))
        assert summary.first()["event_count"] == 1

    def test_gold_idempotent_overwrite(self, spark, tmp_path):
        """Running Gold twice on the same Silver should produce identical results."""
        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()
        bronze_dir = tmp_path / "bronze"
        silver_dir = tmp_path / "silver"
        gold_dir = tmp_path / "gold"

        events = [
            make_event(device_id="d1", timestamp="2025-06-01T12:00:00+00:00"),
            make_event(device_id="d2", timestamp="2025-06-01T12:01:00+00:00"),
        ]
        write_json(raw_dir / "events.json", events)

        bronze_df = self._run_bronze(spark, raw_dir, bronze_dir)
        self._run_silver(bronze_df, silver_dir)

        config = {
            **PIPELINE_CONFIG,
            "paths": {
                "delta_silver": str(silver_dir),
                "delta_gold": str(gold_dir),
            },
        }

        counts_1 = build_and_write(spark, config)
        counts_2 = build_and_write(spark, config)

        for table in GOLD_TABLES:
            assert counts_1[table] == counts_2[table]
