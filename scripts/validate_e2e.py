"""End-to-end integration test: generator -> Bronze -> Silver -> Gold.

Validates the full pipeline as a cohesive system by:
  Phase 1  Generate 80 events, stream through Bronze + Silver
  Phase 2  Generate 80 more, stream through Bronze + Silver (row growth)
  Phase 3  Run Gold batch, validate all four tables

Checks
------
  - Bronze/Silver/Gold tables all populate
  - Row counts increase between phases
  - Anomaly rate reflects the configured 5% probability (with margin)
  - Device health scores vary across devices
  - ML feature table has no nulls in engineered columns
"""

import shutil
import time
from pathlib import Path

from pyspark.sql.functions import col, countDistinct

from generator.iot_telemetry_generator import write_batch
from pipeline.common.utils import get_spark_session, ensure_path
from pipeline.bronze_ingest.bronze_ingest_stream import (
    read_raw_stream,
    add_ingestion_metadata,
    write_bronze_stream,
)
from pipeline.silver_transform.silver_transform_job import (
    read_bronze_stream,
    apply_transformations,
    write_silver_stream,
)
from pipeline.gold_aggregations.gold_aggregations_job import (
    build_and_write,
    gold_table_path,
)


PASS = "PASS"
FAIL = "FAIL"
results = []


def check(name, condition, detail=""):
    status = PASS if condition else FAIL
    results.append((name, status, detail))
    marker = "+" if condition else "X"
    suffix = f" -- {detail}" if detail else ""
    print(f"  [{marker}] {name}{suffix}")


def banner(text):
    print(f"\n{'=' * 70}")
    print(f"  {text}")
    print(f"{'=' * 70}")


def section(text):
    print(f"\n--- {text} ---")


CONFIG = {
    "spark": {
        "master": "local[*]",
        "app_name": "e2e-integration-test",
        "shuffle_partitions": 4,
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
        "output_mode": "append",
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
        "scoring": {
            "anomaly_penalty": 0.9,
            "optional_fields": [
                "temperature", "humidity", "pressure",
                "battery_level", "location", "firmware_version",
            ],
        },
    },
    "gold": {
        "window_duration": "1 hour",
        "health_weights": {"quality": 0.4, "anomaly_rate": 0.3, "battery": 0.3},
        "risk_tiers": {"healthy": 0.8, "warning": 0.5},
    },
}

BATCH_SIZE = 20
BATCHES_PER_PHASE = 4
EVENTS_PER_PHASE = BATCH_SIZE * BATCHES_PER_PHASE
NUM_DEVICES = 50
ANOMALY_PROB = 0.05

DEVICE_IDS = [f"device-{i:04d}" for i in range(1, NUM_DEVICES + 1)]

ML_NON_NULL_COLS = [
    "device_id", "window_start", "window_end", "event_count",
    "temp_mean", "temp_min", "temp_max", "temp_range",
    "hum_mean", "hum_min", "hum_max", "hum_range",
    "pres_mean", "pres_min", "pres_max", "pres_range",
    "battery_mean", "battery_min", "battery_max",
    "anomaly_count", "anomaly_rate",
    "temp_anomaly_count", "hum_anomaly_count", "pres_anomaly_count",
    "quality_mean", "quality_min",
]


def generate_phase(output_dir: Path, label: str):
    """Write multiple batches of synthetic telemetry."""
    for i in range(BATCHES_PER_PHASE):
        path = write_batch(output_dir, BATCH_SIZE, DEVICE_IDS, ANOMALY_PROB)
        print(f"  {label} batch {i + 1}/{BATCHES_PER_PHASE} -> {path.name}")
        time.sleep(0.1)


def run_stream(build_fn, timeout=30):
    query = build_fn()
    query.awaitTermination(timeout=timeout)
    query.stop()


def main():
    banner("END-TO-END INTEGRATION TEST")

    if Path("data").exists():
        shutil.rmtree("data")
    for key in ["bronze_input", "delta_bronze", "delta_silver",
                "delta_gold", "checkpoints"]:
        ensure_path(CONFIG["paths"][key])

    spark = get_spark_session(CONFIG)
    input_dir = Path(CONFIG["paths"]["bronze_input"])

    # ── Phase 1: initial data ────────────────────────────────────

    section("Phase 1: generate initial data")
    generate_phase(input_dir, "Phase-1")

    section("Phase 1: Bronze ingestion")
    run_stream(lambda: write_bronze_stream(
        add_ingestion_metadata(read_raw_stream(spark, CONFIG)), CONFIG,
    ))
    bronze_1 = spark.read.format("delta").load(CONFIG["paths"]["delta_bronze"])
    bronze_count_1 = bronze_1.count()
    check(f"Bronze has {EVENTS_PER_PHASE} rows after phase 1",
          bronze_count_1 == EVENTS_PER_PHASE, f"{bronze_count_1}")

    section("Phase 1: Silver transformation")
    run_stream(lambda: write_silver_stream(
        apply_transformations(read_bronze_stream(spark, CONFIG), CONFIG), CONFIG,
    ))
    silver_1 = spark.read.format("delta").load(CONFIG["paths"]["delta_silver"])
    silver_count_1 = silver_1.count()
    check("Silver has rows after phase 1", silver_count_1 > 0, f"{silver_count_1}")
    check("Silver count <= Bronze count (quality filtering)",
          silver_count_1 <= bronze_count_1,
          f"silver={silver_count_1}, bronze={bronze_count_1}")

    # ── Phase 2: growth check ────────────────────────────────────

    section("Phase 2: generate more data")
    generate_phase(input_dir, "Phase-2")

    section("Phase 2: Bronze ingestion")
    run_stream(lambda: write_bronze_stream(
        add_ingestion_metadata(read_raw_stream(spark, CONFIG)), CONFIG,
    ))
    bronze_2 = spark.read.format("delta").load(CONFIG["paths"]["delta_bronze"])
    bronze_count_2 = bronze_2.count()
    check("Bronze row count increased",
          bronze_count_2 > bronze_count_1,
          f"{bronze_count_1} -> {bronze_count_2}")
    check(f"Bronze has {2 * EVENTS_PER_PHASE} rows after phase 2",
          bronze_count_2 == 2 * EVENTS_PER_PHASE, f"{bronze_count_2}")

    section("Phase 2: Silver transformation")
    run_stream(lambda: write_silver_stream(
        apply_transformations(read_bronze_stream(spark, CONFIG), CONFIG), CONFIG,
    ))
    silver_2 = spark.read.format("delta").load(CONFIG["paths"]["delta_silver"])
    silver_count_2 = silver_2.count()
    check("Silver row count increased",
          silver_count_2 > silver_count_1,
          f"{silver_count_1} -> {silver_count_2}")

    # ── Anomaly rate ─────────────────────────────────────────────

    section("Anomaly rate validation")
    total = silver_count_2
    anomaly_count = silver_2.filter(col("_is_anomaly")).count()
    anomaly_rate = anomaly_count / total if total > 0 else 0
    check(
        f"Anomaly rate in plausible range (0-15% for 5% config)",
        0 <= anomaly_rate <= 0.15,
        f"{anomaly_count}/{total} = {anomaly_rate:.2%}",
    )

    # ── Phase 3: Gold aggregation ────────────────────────────────

    section("Phase 3: Gold aggregation")
    gold_counts = build_and_write(spark, CONFIG)
    for name, cnt in gold_counts.items():
        check(f"Gold {name} is non-empty", cnt > 0, f"{cnt} rows")

    # ── Gold: device health variance ─────────────────────────────

    section("Device health score variance")
    dh = spark.read.format("delta").load(gold_table_path(CONFIG, "device_health"))
    health_rows = dh.collect()
    scores = [r["health_score"] for r in health_rows]
    tiers = set(r["risk_tier"] for r in health_rows)

    min_score = min(scores)
    max_score = max(scores)
    spread = max_score - min_score
    check("Health scores vary across devices",
          spread > 0.01,
          f"min={min_score:.4f}, max={max_score:.4f}, spread={spread:.4f}")

    num_unique = len(set(round(s, 4) for s in scores))
    check("Multiple distinct health scores exist",
          num_unique > 1, f"{num_unique} distinct scores")

    n_devices_health = dh.select(countDistinct("device_id")).collect()[0][0]
    check("Health table covers multiple devices",
          n_devices_health > 1, f"{n_devices_health} devices")

    # ── Gold: ML features null check ─────────────────────────────

    section("ML feature table completeness")
    ml = spark.read.format("delta").load(gold_table_path(CONFIG, "ml_features"))
    ml_count = ml.count()
    check("ML features table non-empty", ml_count > 0, f"{ml_count} rows")

    for c in ML_NON_NULL_COLS:
        if c in ml.columns:
            null_count = ml.filter(col(c).isNull()).count()
            check(f"ML feature '{c}' has no nulls",
                  null_count == 0,
                  f"{null_count} nulls in {ml_count} rows" if null_count else "")

    # ── Gold: _aggregated_at present everywhere ──────────────────

    section("Gold metadata")
    for table in ["device_summary", "anomaly_summary", "device_health", "ml_features"]:
        gdf = spark.read.format("delta").load(gold_table_path(CONFIG, table))
        check(f"Gold {table} has _aggregated_at", "_aggregated_at" in gdf.columns)

    # ── Pipeline cohesion: row flow ──────────────────────────────

    section("Pipeline cohesion")
    check("Bronze -> Silver row flow is monotonic (Silver <= Bronze)",
          silver_count_2 <= bronze_count_2,
          f"Bronze={bronze_count_2}, Silver={silver_count_2}")

    gold_device_rows = gold_counts.get("device_summary", 0)
    silver_devices = silver_2.select(countDistinct("device_id")).collect()[0][0]
    check("Gold device_summary covers Silver devices",
          gold_device_rows >= silver_devices,
          f"summary_rows={gold_device_rows}, silver_devices={silver_devices}")

    spark.stop()

    # ── Final summary ────────────────────────────────────────────

    passed = sum(1 for _, s, _ in results if s == PASS)
    failed = sum(1 for _, s, _ in results if s == FAIL)

    banner(f"E2E TEST: {passed} passed, {failed} failed out of {len(results)}")
    if failed:
        print("\n  FAILED checks:")
        for name, status, detail in results:
            if status == FAIL:
                print(f"    X {name}: {detail}")
    else:
        print("\n  ALL E2E CHECKS PASSED")
    print()

    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
