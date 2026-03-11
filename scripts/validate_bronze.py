"""Comprehensive validation of the Bronze streaming layer.

Checks:
  1. JSON files are being consumed
  2. Delta files are being created
  3. The table is queryable (schema, data quality, metadata)
  4. Checkpointing provides exactly-once semantics
"""

import os
import sys
import shutil
from pathlib import Path

from generator.iot_telemetry_generator import write_batch
from pipeline.common.utils import get_spark_session, ensure_path
from pipeline.bronze_ingest.bronze_ingest_stream import (
    read_raw_stream,
    add_ingestion_metadata,
    write_bronze_stream,
)
from pipeline.bronze_ingest.schema import CORRUPT_RECORD_COLUMN

PASS = "PASS"
FAIL = "FAIL"
results = []


def check(name, condition, detail=""):
    status = PASS if condition else FAIL
    results.append((name, status, detail))
    suffix = f" -- {detail}" if detail else ""
    print(f"  [{status}] {name}{suffix}")


def print_banner(text):
    print(f"\n{'=' * 70}")
    print(f"  {text}")
    print(f"{'=' * 70}")


def run_stream(spark, config, timeout=20):
    raw = read_raw_stream(spark, config)
    enriched = add_ingestion_metadata(raw)
    query = write_bronze_stream(enriched, config)
    query.awaitTermination(timeout=timeout)
    query.stop()
    return query


def main():
    print_banner("BRONZE LAYER VALIDATION")

    config = {
        "spark": {
            "master": "local[1]",
            "app_name": "bronze-validate",
            "shuffle_partitions": 1,
            "log_level": "WARN",
        },
        "paths": {
            "bronze_input": "data/bronze_input",
            "delta_bronze": "data/delta/bronze",
            "checkpoints": "data/delta/checkpoints",
        },
        "streaming": {
            "trigger_interval": "5 seconds",
            "max_files_per_trigger": 100,
        },
    }

    # Clean slate
    if Path("data").exists():
        shutil.rmtree("data")

    input_dir = ensure_path(config["paths"]["bronze_input"])
    ensure_path(config["paths"]["delta_bronze"])
    ensure_path(config["paths"]["checkpoints"])

    # ── Phase 1: Generate telemetry ──────────────────────────────

    print("\n--- Phase 1: Generate synthetic telemetry ---")
    for _ in range(4):
        write_batch(input_dir, batch_size=25)

    json_files = sorted(input_dir.glob("*.json"))
    check("JSON files generated", len(json_files) == 4, f"{len(json_files)} files")

    total_lines = sum(1 for f in json_files for _ in open(f))
    check("Events in JSON files", total_lines == 100, f"{total_lines} events")

    # ── Phase 2: Run Bronze streaming job ────────────────────────

    print("\n--- Phase 2: Run Bronze streaming job (first pass) ---")
    spark = get_spark_session(config)
    run_stream(spark, config)
    print("  Streaming job completed first pass.")

    # ── Check 1: JSON files consumed ─────────────────────────────

    print("\n--- Check 1: JSON files consumed ---")
    bronze_df = spark.read.format("delta").load("data/delta/bronze")
    row_count = bronze_df.count()
    check(
        "All JSON events ingested",
        row_count == 100,
        f"{row_count} rows in Delta (expected 100)",
    )
    distinct_files = bronze_df.select("_source_file").distinct().count()
    check(
        "All 4 source files consumed",
        distinct_files == 4,
        f"{distinct_files} distinct source files",
    )

    # ── Check 2: Delta files created ─────────────────────────────

    print("\n--- Check 2: Delta files created ---")
    delta_path = Path("data/delta/bronze")
    delta_log = delta_path / "_delta_log"
    parquet_files = list(delta_path.glob("*.parquet"))
    check("Delta _delta_log exists", delta_log.exists())
    check(
        "Parquet data files exist",
        len(parquet_files) > 0,
        f"{len(parquet_files)} parquet file(s)",
    )
    log_jsons = list(delta_log.glob("*.json"))
    check(
        "Delta transaction log entries",
        len(log_jsons) > 0,
        f"{len(log_jsons)} log file(s)",
    )

    # ── Check 3: Table is queryable ──────────────────────────────

    print("\n--- Check 3: Delta table is queryable ---")

    expected_raw = [
        "device_id", "timestamp", "temperature", "humidity",
        "pressure", "battery_level", "location", "firmware_version",
    ]
    columns = bronze_df.columns
    check(
        "Has all raw schema fields",
        all(c in columns for c in expected_raw),
        f"found {len([c for c in expected_raw if c in columns])}/{len(expected_raw)}",
    )
    check("Has _ingested_at metadata", "_ingested_at" in columns)
    check("Has _source_file metadata", "_source_file" in columns)
    check("Has _corrupt_record column", CORRUPT_RECORD_COLUMN in columns)

    null_devices = bronze_df.filter(bronze_df.device_id.isNull()).count()
    check("No null device_ids", null_devices == 0, f"{null_devices} nulls")

    null_ts = bronze_df.filter(bronze_df.timestamp.isNull()).count()
    check("Timestamps parsed correctly", null_ts == 0, f"{null_ts} null timestamps")

    corrupt = bronze_df.filter(
        bronze_df[CORRUPT_RECORD_COLUMN].isNotNull()
    ).count()
    check("No corrupt records", corrupt == 0, f"{corrupt} corrupt")

    distinct_devices = bronze_df.select("device_id").distinct().count()
    check(
        "Multiple devices present",
        distinct_devices > 1,
        f"{distinct_devices} distinct devices",
    )

    from pyspark.sql.functions import min as spark_min, max as spark_max
    ts_stats = bronze_df.select(
        spark_min("timestamp").alias("earliest"),
        spark_max("timestamp").alias("latest"),
    ).first()
    check(
        "Timestamp range is valid",
        ts_stats["earliest"] is not None and ts_stats["latest"] is not None,
        f"{ts_stats['earliest']} to {ts_stats['latest']}",
    )

    print("\n  Sample rows:")
    bronze_df.select(
        "device_id", "timestamp", "temperature", "humidity",
        "location", "_ingested_at",
    ).show(5, truncate=False)

    # ── Check 4: Checkpoint and exactly-once ─────────────────────

    print("--- Check 4: Checkpoint and exactly-once semantics ---")
    cp_path = Path("data/delta/checkpoints/bronze")
    check("Checkpoint directory exists", cp_path.exists())

    cp_offsets = list(cp_path.glob("offsets/*"))
    cp_commits = list(cp_path.glob("commits/*"))
    check(
        "Offset files recorded",
        len(cp_offsets) > 0,
        f"{len(cp_offsets)} offset(s)",
    )
    check(
        "Commit files recorded",
        len(cp_commits) > 0,
        f"{len(cp_commits)} commit(s)",
    )

    print("\n  Adding 2 more batches and restarting stream...")
    for _ in range(2):
        write_batch(input_dir, batch_size=25)

    run_stream(spark, config)

    bronze_df2 = spark.read.format("delta").load("data/delta/bronze")
    row_count2 = bronze_df2.count()
    check(
        "New rows appended after restart",
        row_count2 == 150,
        f"{row_count2} total rows (expected 150)",
    )
    check(
        "No duplicates (exactly-once)",
        row_count2 == 150,
        "checkpoint prevented reprocessing of first 4 files",
    )

    # ── Delta table history ──────────────────────────────────────

    print("\n  Delta table version history:")
    from delta.tables import DeltaTable
    dt = DeltaTable.forPath(spark, "data/delta/bronze")
    for h in dt.history().select("version", "timestamp", "operation").collect():
        print(f"    v{h['version']} | {h['operation']:25s} | {h['timestamp']}")

    spark.stop()

    # ── Summary ──────────────────────────────────────────────────

    passed = sum(1 for _, s, _ in results if s == PASS)
    failed = sum(1 for _, s, _ in results if s == FAIL)

    print_banner(
        f"RESULT: {passed} passed, {failed} failed out of {len(results)} checks"
    )
    if failed:
        print("\n  FAILED checks:")
        for name, status, detail in results:
            if status == FAIL:
                print(f"    - {name}: {detail}")
    else:
        print("\n  ALL CHECKS PASSED -- Bronze layer is solid.")
    print()


if __name__ == "__main__":
    main()
