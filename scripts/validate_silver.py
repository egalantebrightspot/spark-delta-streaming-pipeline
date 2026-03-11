"""End-to-end validation of the Silver transformation layer.

Generates data -> Bronze -> Silver and verifies:
  1. Corrupt/null records are dropped
  2. Duplicates are removed
  3. Anomaly flags are correct
  4. Z-scores and quality scores are populated
  5. Schema is clean (no Bronze-internal columns)
  6. Delta table is queryable with full history
  7. Checkpoint semantics work (restart adds only new data)
"""

import json
import shutil
from pathlib import Path

from generator.iot_telemetry_generator import write_batch
from pipeline.common.utils import get_spark_session, ensure_path
from pipeline.bronze_ingest.bronze_ingest_stream import (
    read_raw_stream as read_bronze_raw,
    add_ingestion_metadata,
    write_bronze_stream,
)
from pipeline.silver_transform.silver_transform_job import (
    read_bronze_stream,
    apply_transformations,
    write_silver_stream,
)

PASS = "PASS"
FAIL = "FAIL"
results = []


def check(name, condition, detail=""):
    status = PASS if condition else FAIL
    results.append((name, status, detail))
    suffix = f" -- {detail}" if detail else ""
    print(f"  [{status}] {name}{suffix}")


def banner(text):
    print(f"\n{'=' * 70}")
    print(f"  {text}")
    print(f"{'=' * 70}")


def run_stream(build_fn, timeout=25):
    query = build_fn()
    query.awaitTermination(timeout=timeout)
    query.stop()


CONFIG = {
    "spark": {
        "master": "local[1]",
        "app_name": "silver-validate",
        "shuffle_partitions": 1,
        "log_level": "WARN",
    },
    "paths": {
        "bronze_input": "data/bronze_input",
        "delta_bronze": "data/delta/bronze",
        "delta_silver": "data/delta/silver",
        "checkpoints": "data/delta/checkpoints",
    },
    "streaming": {
        "trigger_interval": "5 seconds",
        "max_files_per_trigger": 100,
    },
    "quality": {
        "max_temperature": 150.0,
        "min_temperature": -50.0,
        "max_humidity": 100.0,
        "min_humidity": 0.0,
        "max_pressure": 1100.0,
        "min_pressure": 900.0,
        "dedup_window_hours": 24,
        "expected": {
            "temperature": {"mean": 22.0, "stddev": 5.0},
            "humidity": {"mean": 55.0, "stddev": 10.0},
            "pressure": {"mean": 1013.25, "stddev": 10.0},
        },
    },
}


def inject_bad_data(input_dir: Path):
    """Write a file with duplicates, corrupt JSON, and anomalous readings."""
    events = [
        # duplicate pair
        {
            "device_id": "dup-device",
            "timestamp": "2025-06-01T12:00:00.000000+00:00",
            "temperature": 22.0, "humidity": 55.0, "pressure": 1013.0,
            "battery_level": 90.0, "location": "factory-floor-A",
            "firmware_version": "1.0.0",
        },
        {
            "device_id": "dup-device",
            "timestamp": "2025-06-01T12:00:00.000000+00:00",
            "temperature": 22.0, "humidity": 55.0, "pressure": 1013.0,
            "battery_level": 90.0, "location": "factory-floor-A",
            "firmware_version": "1.0.0",
        },
        # anomalous temperature
        {
            "device_id": "hot-device",
            "timestamp": "2025-06-01T12:01:00.000000+00:00",
            "temperature": 200.0, "humidity": 55.0, "pressure": 1013.0,
            "battery_level": 80.0, "location": "warehouse-1",
            "firmware_version": "2.0.0-beta",
        },
        # anomalous pressure
        {
            "device_id": "pressure-device",
            "timestamp": "2025-06-01T12:02:00.000000+00:00",
            "temperature": 22.0, "humidity": 55.0, "pressure": 1200.0,
            "battery_level": 80.0, "location": "warehouse-1",
            "firmware_version": "1.0.0",
        },
    ]
    path = input_dir / "bad_batch.json"
    with open(path, "w") as f:
        for e in events:
            f.write(json.dumps(e) + "\n")
        f.write("this is corrupt json\n")

    return {
        "total_lines": 5,
        "corrupt": 1,
        "duplicates": 1,
        "anomalous": 2,
        "expected_clean": 3,  # 5 - 1 corrupt - 1 dup = 3
    }


def main():
    banner("SILVER LAYER VALIDATION")

    if Path("data").exists():
        shutil.rmtree("data")

    for p in ["bronze_input", "delta_bronze", "delta_silver", "checkpoints"]:
        ensure_path(CONFIG["paths"].get(p, f"data/delta/{p}"))

    # ── Phase 1: Generate data with known characteristics ────────

    print("\n--- Phase 1: Generate telemetry (good + bad data) ---")
    input_dir = Path(CONFIG["paths"]["bronze_input"])

    for _ in range(3):
        write_batch(input_dir, batch_size=25)
    bad_stats = inject_bad_data(input_dir)

    json_files = list(input_dir.glob("*.json"))
    total_lines = sum(1 for f in json_files for _ in open(f))
    print(f"  Generated {len(json_files)} files, {total_lines} total lines")
    print(f"  Bad batch: {bad_stats}")

    # ── Phase 2: Run Bronze ──────────────────────────────────────

    print("\n--- Phase 2: Run Bronze ingestion ---")
    spark = get_spark_session(CONFIG)

    def bronze_job():
        raw = read_bronze_raw(spark, CONFIG)
        enriched = add_ingestion_metadata(raw)
        return write_bronze_stream(enriched, CONFIG)

    run_stream(bronze_job)

    bronze_df = spark.read.format("delta").load("data/delta/bronze")
    bronze_count = bronze_df.count()
    print(f"  Bronze rows: {bronze_count}")
    check("Bronze ingested all lines", bronze_count == total_lines,
          f"{bronze_count} rows")

    # ── Phase 3: Run Silver ──────────────────────────────────────

    print("\n--- Phase 3: Run Silver transformation ---")

    def silver_job():
        bronze = read_bronze_stream(spark, CONFIG)
        silver = apply_transformations(bronze, CONFIG)
        return write_silver_stream(silver, CONFIG)

    run_stream(silver_job)

    silver_df = spark.read.format("delta").load("data/delta/silver")
    silver_count = silver_df.count()
    print(f"  Silver rows: {silver_count}")

    # ── Check 1: Corrupt records dropped ─────────────────────────

    print("\n--- Check 1: Corrupt and null records dropped ---")
    check(
        "Silver has fewer rows than Bronze",
        silver_count < bronze_count,
        f"Bronze={bronze_count}, Silver={silver_count}",
    )
    check(
        "No _corrupt_record column in Silver",
        "_corrupt_record" not in silver_df.columns,
    )

    # ── Check 2: Duplicates removed ──────────────────────────────

    print("\n--- Check 2: Duplicates removed ---")
    from pyspark.sql.functions import col, count

    dup_check = (
        silver_df
        .groupBy("device_id", "timestamp")
        .agg(count("*").alias("cnt"))
        .filter(col("cnt") > 1)
    )
    dup_count = dup_check.count()
    check("No duplicate device_id+timestamp pairs", dup_count == 0,
          f"{dup_count} duplicate groups")

    # ── Check 3: Anomaly flags ───────────────────────────────────

    print("\n--- Check 3: Anomaly flags ---")
    anomaly_cols = [
        "_is_temp_anomaly", "_is_humidity_anomaly",
        "_is_pressure_anomaly", "_is_anomaly", "_anomaly_details",
    ]
    for c in anomaly_cols:
        check(f"Column {c} exists", c in silver_df.columns)

    anomalous = silver_df.filter(col("_is_anomaly")).count()
    check("Anomalous records flagged", anomalous >= bad_stats["anomalous"],
          f"{anomalous} anomalous rows")

    temp_anomalies = silver_df.filter(col("_is_temp_anomaly")).count()
    check("Temperature anomalies detected", temp_anomalies >= 1,
          f"{temp_anomalies} rows")

    pres_anomalies = silver_df.filter(col("_is_pressure_anomaly")).count()
    check("Pressure anomalies detected", pres_anomalies >= 1,
          f"{pres_anomalies} rows")

    details_sample = (
        silver_df
        .filter(col("_anomaly_details").isNotNull())
        .select("device_id", "_anomaly_details")
        .collect()
    )
    print("  Anomaly details sample:")
    for r in details_sample[:5]:
        print(f"    {r['device_id']}: {r['_anomaly_details']}")

    # ── Check 4: Z-scores and quality scores ─────────────────────

    print("\n--- Check 4: Z-scores and quality scores ---")
    zscore_cols = ["_temperature_zscore", "_humidity_zscore", "_pressure_zscore"]
    for c in zscore_cols:
        check(f"Column {c} exists", c in silver_df.columns)

    check("Column _quality_score exists", "_quality_score" in silver_df.columns)

    from pyspark.sql.functions import min as spark_min, max as spark_max, avg

    qs_stats = silver_df.select(
        spark_min("_quality_score").alias("min_qs"),
        spark_max("_quality_score").alias("max_qs"),
        avg("_quality_score").alias("avg_qs"),
    ).first()
    check("Quality scores in [0, 1]",
          qs_stats["min_qs"] >= 0 and qs_stats["max_qs"] <= 1.0,
          f"min={qs_stats['min_qs']:.4f}, max={qs_stats['max_qs']:.4f}, avg={qs_stats['avg_qs']:.4f}")

    perfect = silver_df.filter(col("_quality_score") == 1.0).count()
    imperfect = silver_df.filter(col("_quality_score") < 1.0).count()
    check("Mix of perfect and imperfect quality scores",
          perfect > 0 and imperfect > 0,
          f"{perfect} perfect, {imperfect} imperfect")

    # ── Check 5: Schema cleanliness ──────────────────────────────

    print("\n--- Check 5: Silver schema ---")
    check("No _source_file column", "_source_file" not in silver_df.columns)
    check("Has _ingested_at (lineage)", "_ingested_at" in silver_df.columns)
    check("Has _processed_at", "_processed_at" in silver_df.columns)

    print("\n  Silver schema:")
    silver_df.printSchema()

    print("  Sample Silver rows:")
    silver_df.select(
        "device_id", "temperature", "humidity",
        "_is_anomaly", "_temperature_zscore", "_quality_score",
    ).show(10, truncate=False)

    # ── Check 6: Delta table history ─────────────────────────────

    print("--- Check 6: Delta table history ---")
    from delta.tables import DeltaTable
    dt = DeltaTable.forPath(spark, "data/delta/silver")
    hist = dt.history().select("version", "timestamp", "operation").collect()
    check("Delta versions recorded", len(hist) > 0, f"{len(hist)} versions")
    for h in hist:
        print(f"    v{h['version']} | {h['operation']:25s} | {h['timestamp']}")

    # ── Check 7: Checkpoint restart (exactly-once) ───────────────

    print("\n--- Check 7: Checkpoint and exactly-once ---")
    cp_path = Path("data/delta/checkpoints/silver")
    check("Silver checkpoint exists", cp_path.exists())

    print("  Adding 2 more generator batches and re-running Bronze + Silver...")
    for _ in range(2):
        write_batch(input_dir, batch_size=25)

    run_stream(bronze_job)
    run_stream(silver_job)

    silver_df2 = spark.read.format("delta").load("data/delta/silver")
    new_count = silver_df2.count()
    added = new_count - silver_count
    check(
        "New rows appended after restart",
        new_count > silver_count,
        f"{silver_count} -> {new_count} (+{added})",
    )

    dup_check2 = (
        silver_df2
        .groupBy("device_id", "timestamp")
        .agg(count("*").alias("cnt"))
        .filter(col("cnt") > 1)
    )
    check("Still no duplicates after restart", dup_check2.count() == 0)

    spark.stop()

    # ── Summary ──────────────────────────────────────────────────

    passed = sum(1 for _, s, _ in results if s == PASS)
    failed = sum(1 for _, s, _ in results if s == FAIL)
    banner(f"RESULT: {passed} passed, {failed} failed out of {len(results)} checks")
    if failed:
        print("\n  FAILED checks:")
        for name, status, detail in results:
            if status == FAIL:
                print(f"    - {name}: {detail}")
    else:
        print("\n  ALL CHECKS PASSED -- Silver layer is solid.")
    print()


if __name__ == "__main__":
    main()
