"""Data-quality and governance acceptance test.

Creates a single static file with deterministic test vectors, runs it
through Bronze -> Silver -> Gold, and inspects every row at each layer
to confirm exact governance behavior.

Test vectors:
  1. valid_clean       — normal readings, all fields present
  2. valid_clean_2     — second clean device (for aggregation checks)
  3. neg_humidity      — humidity = -15.0 (should clamp to 0)
  4. extreme_temp      — temperature = 300.0 (anomaly + z >> 3)
  5. low_pressure      — pressure = 800.0 (anomaly + z << -3)
  6. multi_anomaly     — temperature = -80, pressure = 1200 (both flags)
  7. missing_device_id — device_id omitted (should be dropped)
  8. missing_timestamp — timestamp omitted (should be dropped)
  9. anomaly_quality   — temperature = 200, missing location + firmware
  10. dup_row_a        — duplicate pair (same device_id + timestamp)
  11. dup_row_b        — duplicate pair (same device_id + timestamp)
  12. corrupt_json     — raw string "this is not json"

Expected after Silver:
  - 12 Bronze rows -> 8 Silver rows (drop 1 corrupt, 2 null-key, 1 dup)
"""

import json
import shutil
from pathlib import Path

from pyspark.sql.functions import col

from pipeline.common.utils import get_spark_session, ensure_path
from pipeline.bronze_ingest.schema import IOT_TELEMETRY_SCHEMA, CORRUPT_RECORD_COLUMN
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
        "master": "local[1]",
        "app_name": "governance-test",
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
    },
    "gold": {
        "window_duration": "1 hour",
        "health_weights": {"quality": 0.4, "anomaly_rate": 0.3, "battery": 0.3},
        "risk_tiers": {"healthy": 0.8, "warning": 0.5},
    },
}


def build_test_file(output_dir: Path):
    """Create a single deterministic test file with all governance edge cases."""
    base_ts = "2025-06-01T12:00:00.000000+00:00"

    def ts(minute):
        return f"2025-06-01T12:{minute:02d}:00.000000+00:00"

    rows = [
        # 1. Clean record — all fields, normal values
        json.dumps({
            "device_id": "valid_clean", "timestamp": ts(0),
            "temperature": 22.0, "humidity": 55.0, "pressure": 1013.25,
            "battery_level": 90.0, "location": "floor-A", "firmware_version": "1.0.0",
        }),
        # 2. Second clean device
        json.dumps({
            "device_id": "valid_clean_2", "timestamp": ts(1),
            "temperature": 24.0, "humidity": 60.0, "pressure": 1010.0,
            "battery_level": 85.0, "location": "floor-B", "firmware_version": "1.0.0",
        }),
        # 3. Negative humidity — should clamp to 0.0
        json.dumps({
            "device_id": "neg_humidity", "timestamp": ts(2),
            "temperature": 22.0, "humidity": -15.0, "pressure": 1013.0,
            "battery_level": 80.0, "location": "floor-A", "firmware_version": "1.0.0",
        }),
        # 4. Extreme temperature — anomaly + z-score >> 3
        json.dumps({
            "device_id": "extreme_temp", "timestamp": ts(3),
            "temperature": 300.0, "humidity": 55.0, "pressure": 1013.0,
            "battery_level": 75.0, "location": "floor-A", "firmware_version": "1.0.0",
        }),
        # 5. Low pressure — anomaly + z-score << -3
        json.dumps({
            "device_id": "low_pressure", "timestamp": ts(4),
            "temperature": 22.0, "humidity": 55.0, "pressure": 800.0,
            "battery_level": 70.0, "location": "floor-C", "firmware_version": "1.0.0",
        }),
        # 6. Multi-anomaly — both temp AND pressure out of range
        json.dumps({
            "device_id": "multi_anomaly", "timestamp": ts(5),
            "temperature": -80.0, "humidity": 55.0, "pressure": 1200.0,
            "battery_level": 50.0, "location": "floor-D", "firmware_version": "1.0.0",
        }),
        # 7. Missing device_id — should be DROPPED by drop_nulls
        json.dumps({
            "timestamp": ts(6),
            "temperature": 22.0, "humidity": 55.0, "pressure": 1013.0,
            "battery_level": 90.0, "location": "floor-A", "firmware_version": "1.0.0",
        }),
        # 8. Missing timestamp — should be DROPPED by drop_nulls
        json.dumps({
            "device_id": "no_timestamp",
            "temperature": 22.0, "humidity": 55.0, "pressure": 1013.0,
            "battery_level": 90.0, "location": "floor-A", "firmware_version": "1.0.0",
        }),
        # 9. Anomaly + missing optional fields — quality score hit
        json.dumps({
            "device_id": "anomaly_quality", "timestamp": ts(8),
            "temperature": 200.0, "humidity": 55.0, "pressure": 1013.0,
            "battery_level": 80.0,
        }),
        # 10-11. Duplicate pair — should collapse to 1 row
        json.dumps({
            "device_id": "dup_device", "timestamp": ts(9),
            "temperature": 22.0, "humidity": 55.0, "pressure": 1013.0,
            "battery_level": 88.0, "location": "floor-A", "firmware_version": "1.0.0",
        }),
        json.dumps({
            "device_id": "dup_device", "timestamp": ts(9),
            "temperature": 22.0, "humidity": 55.0, "pressure": 1013.0,
            "battery_level": 88.0, "location": "floor-A", "firmware_version": "1.0.0",
        }),
        # 12. Corrupt JSON
        "this is not valid json",
    ]

    path = output_dir / "governance_test.json"
    path.write_text("\n".join(rows) + "\n")
    return path


def run_stream(build_fn, timeout=25):
    query = build_fn()
    query.awaitTermination(timeout=timeout)
    query.stop()


def main():
    banner("DATA-QUALITY & GOVERNANCE ACCEPTANCE TEST")

    if Path("data").exists():
        shutil.rmtree("data")
    for key in ["bronze_input", "delta_bronze", "delta_silver",
                "delta_gold", "checkpoints"]:
        ensure_path(CONFIG["paths"][key])

    # ── Build the test file ──────────────────────────────────────

    section("Test file creation")
    input_dir = Path(CONFIG["paths"]["bronze_input"])
    test_file = build_test_file(input_dir)
    line_count = sum(1 for _ in open(test_file))
    print(f"  Created {test_file} with {line_count} lines")
    check("Test file has 12 lines", line_count == 12, f"{line_count}")

    # ── Bronze ───────────────────────────────────────────────────

    section("Bronze ingestion")
    spark = get_spark_session(CONFIG)

    run_stream(lambda: write_bronze_stream(
        add_ingestion_metadata(read_bronze_raw(spark, CONFIG)), CONFIG,
    ))

    bronze = spark.read.format("delta").load(CONFIG["paths"]["delta_bronze"])
    bronze_count = bronze.count()
    check("Bronze ingested all 12 lines", bronze_count == 12, f"{bronze_count} rows")

    bronze_corrupt = bronze.filter(col(CORRUPT_RECORD_COLUMN).isNotNull())
    check("1 corrupt record captured in Bronze", bronze_corrupt.count() == 1)

    bronze_with_meta = bronze.filter(col("_ingested_at").isNotNull())
    check("All Bronze rows have _ingested_at", bronze_with_meta.count() == bronze_count)

    bronze_with_src = bronze.filter(col("_source_file").isNotNull())
    check("All Bronze rows have _source_file", bronze_with_src.count() == bronze_count)

    # ── Silver ───────────────────────────────────────────────────

    section("Silver transformation")
    run_stream(lambda: write_silver_stream(
        apply_transformations(read_bronze_stream(spark, CONFIG), CONFIG), CONFIG,
    ))

    silver = spark.read.format("delta").load(CONFIG["paths"]["delta_silver"])
    silver_count = silver.count()
    rows = {r["device_id"]: r for r in silver.collect()}

    # Row counts
    check(
        "Silver has exactly 8 rows (12 - 1 corrupt - 2 null-key - 1 dup)",
        silver_count == 8,
        f"{silver_count} rows",
    )

    # Corrupt / null-key rows dropped
    check("No _corrupt_record column in Silver", "_corrupt_record" not in silver.columns)
    check("No _source_file column in Silver", "_source_file" not in silver.columns)
    check("_processed_at present", "_processed_at" in silver.columns)
    check("_ingested_at preserved", "_ingested_at" in silver.columns)

    device_ids = set(rows.keys())
    check("Missing-device_id row dropped", None not in device_ids)
    check("Missing-timestamp row (no_timestamp) dropped", "no_timestamp" not in device_ids)

    # Deduplication
    check(
        "dup_device appears exactly once",
        "dup_device" in device_ids,
        f"present={('dup_device' in device_ids)}",
    )

    # ── Normalization: negative humidity clamped ──────────────────

    section("Rule: negative humidity -> 0.0")
    neg_hum = rows.get("neg_humidity")
    if neg_hum:
        check("neg_humidity.humidity == 0.0", neg_hum["humidity"] == 0.0,
              f"actual={neg_hum['humidity']}")
    else:
        check("neg_humidity row exists", False, "MISSING")

    # ── Anomaly: extreme temperature ─────────────────────────────

    section("Rule: extreme temperature triggers _is_temp_anomaly")
    ext_temp = rows.get("extreme_temp")
    if ext_temp:
        check("extreme_temp._is_temp_anomaly is True", ext_temp["_is_temp_anomaly"] is True)
        check("extreme_temp._is_anomaly is True", ext_temp["_is_anomaly"] is True)
        check(
            "extreme_temp._anomaly_details contains 'temperature_out_of_range'",
            "temperature_out_of_range" in (ext_temp["_anomaly_details"] or ""),
        )
        # z = (300 - 22) / 5 = 55.6
        z = ext_temp["_temperature_zscore"]
        check("extreme_temp z-score > 3", z is not None and z > 3.0, f"z={z}")
        check("extreme_temp z-score ~ 55.6", z is not None and abs(z - 55.6) < 0.1,
              f"z={z}")
    else:
        check("extreme_temp row exists", False, "MISSING")

    # ── Anomaly: low pressure ────────────────────────────────────

    section("Rule: low pressure triggers _is_pressure_anomaly")
    low_p = rows.get("low_pressure")
    if low_p:
        check("low_pressure._is_pressure_anomaly is True",
              low_p["_is_pressure_anomaly"] is True)
        check("low_pressure._is_temp_anomaly is False",
              low_p["_is_temp_anomaly"] is False)
        # z = (800 - 1013.25) / 10 = -21.325
        z = low_p["_pressure_zscore"]
        check("low_pressure z-score < -3", z is not None and z < -3.0, f"z={z}")
        check("low_pressure z-score ~ -21.3", z is not None and abs(z - (-21.325)) < 0.1,
              f"z={z}")
    else:
        check("low_pressure row exists", False, "MISSING")

    # ── Anomaly: multi-anomaly ───────────────────────────────────

    section("Rule: multi-anomaly flags both temperature AND pressure")
    multi = rows.get("multi_anomaly")
    if multi:
        check("multi_anomaly._is_temp_anomaly is True",
              multi["_is_temp_anomaly"] is True)
        check("multi_anomaly._is_pressure_anomaly is True",
              multi["_is_pressure_anomaly"] is True)
        check("multi_anomaly._is_anomaly is True",
              multi["_is_anomaly"] is True)
        details = multi["_anomaly_details"] or ""
        check("Details contain 'temperature_out_of_range'",
              "temperature_out_of_range" in details, f"details={details}")
        check("Details contain 'pressure_out_of_range'",
              "pressure_out_of_range" in details, f"details={details}")
    else:
        check("multi_anomaly row exists", False, "MISSING")

    # ── Quality score: clean record ──────────────────────────────

    section("Rule: _quality_score = 1.0 for clean, complete records")
    clean = rows.get("valid_clean")
    if clean:
        check("valid_clean._quality_score == 1.0",
              clean["_quality_score"] == 1.0,
              f"actual={clean['_quality_score']}")
        check("valid_clean._is_anomaly is False",
              clean["_is_anomaly"] is False)
    else:
        check("valid_clean row exists", False, "MISSING")

    # ── Quality score: anomaly reduces score ─────────────────────

    section("Rule: _quality_score < 1.0 when anomaly present")
    ext_temp = rows.get("extreme_temp")
    if ext_temp:
        check("extreme_temp._quality_score == 0.9 (anomaly penalty)",
              abs(ext_temp["_quality_score"] - 0.9) < 0.01,
              f"actual={ext_temp['_quality_score']}")
    else:
        check("extreme_temp row exists", False, "MISSING")

    # ── Quality score: anomaly + missing fields ──────────────────

    section("Rule: missing optional fields + anomaly compound penalty")
    aq = rows.get("anomaly_quality")
    if aq:
        # 4/6 completeness * 0.9 penalty = 0.6
        check("anomaly_quality._quality_score ~ 0.6",
              abs(aq["_quality_score"] - 0.6) < 0.01,
              f"actual={aq['_quality_score']}")
        check("anomaly_quality._is_temp_anomaly is True",
              aq["_is_temp_anomaly"] is True)
        check("anomaly_quality.location is None (missing)",
              aq["location"] is None)
        check("anomaly_quality.firmware_version is None (missing)",
              aq["firmware_version"] is None)
    else:
        check("anomaly_quality row exists", False, "MISSING")

    # ── Clean rows: no anomaly flags ─────────────────────────────

    section("Rule: clean rows have no anomaly flags")
    for device_id in ["valid_clean", "valid_clean_2", "neg_humidity", "dup_device"]:
        r = rows.get(device_id)
        if r:
            check(f"{device_id}._is_anomaly is False", r["_is_anomaly"] is False)
        else:
            check(f"{device_id} row exists", False, "MISSING")

    # ── Silver summary ───────────────────────────────────────────

    section("Silver summary table")
    silver.select(
        "device_id", "temperature", "humidity",
        "_is_anomaly", "_anomaly_details",
        "_temperature_zscore", "_pressure_zscore",
        "_quality_score",
    ).orderBy("device_id").show(10, truncate=False)

    # ── Gold ─────────────────────────────────────────────────────

    section("Gold aggregation")
    gold_counts = build_gold(spark, CONFIG)
    for name, cnt in gold_counts.items():
        check(f"Gold {name} is non-empty", cnt > 0, f"{cnt} rows")

    # Check _aggregated_at in all Gold tables
    for table in ["device_summary", "anomaly_summary", "device_health", "ml_features"]:
        path = gold_table_path(CONFIG, table)
        gdf = spark.read.format("delta").load(path)
        check(f"Gold {table} has _aggregated_at", "_aggregated_at" in gdf.columns)

    # Health: anomalous devices should have lower health scores
    dh = spark.read.format("delta").load(gold_table_path(CONFIG, "device_health"))
    health_rows = {r["device_id"]: r for r in dh.collect()}

    clean_row = health_rows.get("valid_clean")
    anom_row = health_rows.get("extreme_temp")
    if clean_row and anom_row:
        clean_score = clean_row["health_score"]
        anom_score = anom_row["health_score"]
        check(
            "Gold: clean device health > anomalous device health",
            clean_score > anom_score,
            f"clean={clean_score:.4f}, anomalous={anom_score:.4f}",
        )
    else:
        check("Gold: clean and anomalous devices both present in health table", False)

    multi_row = health_rows.get("multi_anomaly")
    if multi_row:
        multi_tier = multi_row["risk_tier"]
        check(
            "Gold: multi_anomaly risk tier is warning or critical",
            multi_tier in ("warning", "critical"),
            f"tier={multi_tier}",
        )
    else:
        check("Gold: multi_anomaly present in health table", False)

    section("Gold device_health table")
    dh.select(
        "device_id", "event_count", "health_score", "risk_tier",
        "anomaly_rate", "avg_quality_score",
    ).orderBy("device_id").show(10, truncate=False)

    spark.stop()

    # ── Final summary ────────────────────────────────────────────

    passed = sum(1 for _, s, _ in results if s == PASS)
    failed = sum(1 for _, s, _ in results if s == FAIL)

    banner(f"GOVERNANCE TEST: {passed} passed, {failed} failed out of {len(results)}")
    if failed:
        print("\n  FAILED checks:")
        for name, status, detail in results:
            if status == FAIL:
                print(f"    X {name}: {detail}")
    else:
        print("\n  ALL GOVERNANCE CHECKS PASSED")
    print()

    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
