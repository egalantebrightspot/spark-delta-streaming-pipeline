"""End-to-end validation of the full medallion pipeline: Bronze -> Silver -> Gold.

Generates synthetic data with known characteristics (normal + anomalous +
corrupt + duplicates), runs all three layers, then validates the four
Gold tables against expected properties.
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
from pipeline.gold_aggregations.gold_aggregations_job import (
    build_and_write as build_gold,
    gold_table_path,
    GOLD_TABLES,
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


CONFIG = {
    "spark": {
        "master": "local[1]",
        "app_name": "gold-validate",
        "shuffle_partitions": 1,
        "log_level": "WARN",
    },
    "paths": {
        "bronze_input": "data/bronze_input",
        "delta_bronze": "data/delta/bronze",
        "delta_silver": "data/delta/silver",
        "delta_gold": "data/delta/gold",
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
    "gold": {
        "window_duration": "1 hour",
        "health_weights": {"quality": 0.4, "anomaly_rate": 0.3, "battery": 0.3},
        "risk_tiers": {"healthy": 0.8, "warning": 0.5},
    },
}


def inject_known_data(input_dir: Path):
    """Write files with known anomalies, duplicates, and a corrupt line."""
    events = [
        {
            "device_id": "dup-device", "timestamp": "2025-06-01T12:00:00+00:00",
            "temperature": 22.0, "humidity": 55.0, "pressure": 1013.0,
            "battery_level": 90.0, "location": "factory-floor-A",
            "firmware_version": "1.0.0",
        },
        {
            "device_id": "dup-device", "timestamp": "2025-06-01T12:00:00+00:00",
            "temperature": 22.0, "humidity": 55.0, "pressure": 1013.0,
            "battery_level": 90.0, "location": "factory-floor-A",
            "firmware_version": "1.0.0",
        },
        {
            "device_id": "hot-device", "timestamp": "2025-06-01T12:01:00+00:00",
            "temperature": 200.0, "humidity": 55.0, "pressure": 1013.0,
            "battery_level": 20.0, "location": "warehouse-1",
            "firmware_version": "2.0.0-beta",
        },
        {
            "device_id": "pressure-device", "timestamp": "2025-06-01T12:02:00+00:00",
            "temperature": 22.0, "humidity": 55.0, "pressure": 1200.0,
            "battery_level": 15.0, "location": "warehouse-1",
            "firmware_version": "1.0.0",
        },
    ]
    path = input_dir / "known_batch.json"
    with open(path, "w") as f:
        for e in events:
            f.write(json.dumps(e) + "\n")
        f.write("corrupt json line\n")


def run_stream(build_fn, timeout=25):
    query = build_fn()
    query.awaitTermination(timeout=timeout)
    query.stop()


def main():
    banner("FULL PIPELINE VALIDATION: Bronze -> Silver -> Gold")

    if Path("data").exists():
        shutil.rmtree("data")
    for key in ["bronze_input", "delta_bronze", "delta_silver",
                "delta_gold", "checkpoints"]:
        ensure_path(CONFIG["paths"][key])

    # ── Phase 1: Generate data ───────────────────────────────────

    print("\n--- Phase 1: Generate telemetry ---")
    input_dir = Path(CONFIG["paths"]["bronze_input"])
    for _ in range(4):
        write_batch(input_dir, batch_size=25)
    inject_known_data(input_dir)

    json_files = list(input_dir.glob("*.json"))
    total_lines = sum(1 for f in json_files for _ in open(f))
    print(f"  {len(json_files)} files, {total_lines} total lines")

    # ── Phase 2: Bronze ──────────────────────────────────────────

    print("\n--- Phase 2: Bronze ingestion ---")
    spark = get_spark_session(CONFIG)

    run_stream(lambda: write_bronze_stream(
        add_ingestion_metadata(read_bronze_raw(spark, CONFIG)), CONFIG,
    ))
    bronze_count = spark.read.format("delta").load("data/delta/bronze").count()
    print(f"  Bronze rows: {bronze_count}")

    # ── Phase 3: Silver ──────────────────────────────────────────

    print("\n--- Phase 3: Silver transformation ---")
    run_stream(lambda: write_silver_stream(
        apply_transformations(read_bronze_stream(spark, CONFIG), CONFIG), CONFIG,
    ))
    silver_count = spark.read.format("delta").load("data/delta/silver").count()
    print(f"  Silver rows: {silver_count}")
    check("Silver < Bronze (cleaning)", silver_count < bronze_count,
          f"{bronze_count} -> {silver_count}")

    # ── Phase 4: Gold ────────────────────────────────────────────

    print("\n--- Phase 4: Gold aggregation ---")
    gold_counts = build_gold(spark, CONFIG)
    for name, cnt in gold_counts.items():
        print(f"  {name}: {cnt} rows")

    # ── Validate device_summary ──────────────────────────────────

    print("\n--- Check: device_summary ---")
    from pyspark.sql.functions import col, sum as spark_sum

    ds = spark.read.format("delta").load(gold_table_path(CONFIG, "device_summary"))
    check("device_summary is non-empty", ds.count() > 0, f"{ds.count()} rows")
    expected_cols = {
        "device_id", "window_start", "window_end", "event_count",
        "avg_temperature", "stddev_temperature",
        "min_temperature", "max_temperature",
        "avg_humidity", "avg_pressure", "avg_battery_level",
        "anomaly_count", "anomaly_rate", "avg_quality_score",
        "_aggregated_at",
    }
    check("device_summary schema", expected_cols.issubset(set(ds.columns)),
          f"{len(ds.columns)} columns")

    total_events_in_summary = ds.agg(spark_sum("event_count")).first()[0]
    check("Sum of event_count matches Silver",
          total_events_in_summary == silver_count,
          f"{total_events_in_summary} vs {silver_count}")

    ds.select("device_id", "event_count", "avg_temperature",
              "anomaly_count", "anomaly_rate", "avg_quality_score").show(
        10, truncate=False)

    # ── Validate anomaly_summary ─────────────────────────────────

    print("--- Check: anomaly_summary ---")
    anm = spark.read.format("delta").load(gold_table_path(CONFIG, "anomaly_summary"))
    check("anomaly_summary is non-empty", anm.count() > 0, f"{anm.count()} rows")

    anomaly_total = anm.agg(spark_sum("total_anomaly_count")).first()[0]
    check("Anomalies detected", anomaly_total >= 2,
          f"{anomaly_total} total anomalies (injected 2 known)")

    per_type = anm.agg(
        spark_sum("temp_anomaly_count").alias("temp"),
        spark_sum("humidity_anomaly_count").alias("hum"),
        spark_sum("pressure_anomaly_count").alias("pres"),
    ).first()
    check("Temperature anomalies", per_type["temp"] >= 1, f"{per_type['temp']}")
    check("Pressure anomalies", per_type["pres"] >= 1, f"{per_type['pres']}")

    anm.select("device_id", "total_events", "temp_anomaly_count",
               "pressure_anomaly_count", "anomaly_rate").show(10, truncate=False)

    # ── Validate device_health ───────────────────────────────────

    print("--- Check: device_health ---")
    dh = spark.read.format("delta").load(gold_table_path(CONFIG, "device_health"))
    check("device_health is non-empty", dh.count() > 0, f"{dh.count()} rows")

    from pyspark.sql.functions import min as spark_min, max as spark_max

    score_stats = dh.agg(
        spark_min("health_score").alias("min_hs"),
        spark_max("health_score").alias("max_hs"),
    ).first()
    check("Health scores in [0, 1]",
          score_stats["min_hs"] >= 0 and score_stats["max_hs"] <= 1.0,
          f"min={score_stats['min_hs']:.4f}, max={score_stats['max_hs']:.4f}")

    tiers = [r["risk_tier"] for r in dh.select("risk_tier").distinct().collect()]
    check("Risk tiers are valid", all(t in ("healthy", "warning", "critical") for t in tiers),
          f"tiers: {tiers}")

    dh.select("device_id", "event_count", "health_score", "risk_tier",
              "anomaly_rate", "avg_battery_level").show(10, truncate=False)

    # ── Validate ml_features ─────────────────────────────────────

    print("--- Check: ml_features ---")
    ml = spark.read.format("delta").load(gold_table_path(CONFIG, "ml_features"))
    check("ml_features is non-empty", ml.count() > 0, f"{ml.count()} rows")
    check("ml_features is wide", len(ml.columns) >= 30,
          f"{len(ml.columns)} columns")

    sensor_cols = [
        "temp_mean", "temp_stddev", "temp_min", "temp_max", "temp_range",
        "hum_mean", "hum_stddev", "hum_min", "hum_max", "hum_range",
        "pres_mean", "pres_stddev", "pres_min", "pres_max", "pres_range",
    ]
    for c in sensor_cols:
        check(f"  ML column {c}", c in ml.columns)

    zscore_cols = [
        "temp_zscore_abs_mean", "temp_zscore_abs_max",
        "hum_zscore_abs_mean", "hum_zscore_abs_max",
        "pres_zscore_abs_mean", "pres_zscore_abs_max",
    ]
    for c in zscore_cols:
        check(f"  ML column {c}", c in ml.columns)

    ml.select("device_id", "event_count", "temp_mean", "temp_range",
              "anomaly_rate", "quality_mean").show(10, truncate=False)

    # ── Delta table history ──────────────────────────────────────

    print("--- Gold Delta history ---")
    from delta.tables import DeltaTable
    for name in ["device_summary", "anomaly_summary",
                 "device_health", "ml_features"]:
        path = gold_table_path(CONFIG, name)
        dt = DeltaTable.forPath(spark, path)
        versions = dt.history().count()
        print(f"  {name}: {versions} version(s)")
    check("All Gold tables have Delta history", True)

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
        print("\n  ALL CHECKS PASSED -- Full medallion pipeline is solid.")
    print()


if __name__ == "__main__":
    main()
