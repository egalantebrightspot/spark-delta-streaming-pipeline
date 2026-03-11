"""Generate the pipeline observability dashboard.

Runs the full medallion pipeline (Bronze -> Silver -> Gold) on fresh
data, then produces a standalone HTML dashboard with Plotly charts.

Usage:
    python scripts/generate_dashboard.py
"""

import json
import shutil
from pathlib import Path

from generator.iot_telemetry_generator import write_batch
from pipeline.common.utils import get_spark_session, ensure_path, load_config
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
from pipeline.gold_aggregations.gold_aggregations_job import build_and_write as build_gold
from monitoring.dashboard import generate_html_report


def inject_mixed_data(input_dir: Path):
    """Inject data with known anomalies for a richer dashboard."""
    anomalous_events = [
        {
            "device_id": f"stress-device-{i:02d}",
            "timestamp": f"2025-06-01T12:{i:02d}:00+00:00",
            "temperature": 200.0 + i * 10,
            "humidity": 55.0,
            "pressure": 1013.0,
            "battery_level": max(10, 90 - i * 15),
            "location": "warehouse-1",
            "firmware_version": "2.0.0-beta",
        }
        for i in range(5)
    ]
    pressure_events = [
        {
            "device_id": f"pressure-dev-{i:02d}",
            "timestamp": f"2025-06-01T12:{10 + i:02d}:00+00:00",
            "temperature": 22.0,
            "humidity": 55.0,
            "pressure": 1200.0 + i * 50,
            "battery_level": 15.0,
            "location": "basement-hvac",
            "firmware_version": "1.0.0",
        }
        for i in range(3)
    ]

    path = input_dir / "anomaly_stress.json"
    with open(path, "w") as f:
        for e in anomalous_events + pressure_events:
            f.write(json.dumps(e) + "\n")
        f.write("corrupt record for dashboard\n")


def run_stream(build_fn, timeout=25):
    query = build_fn()
    query.awaitTermination(timeout=timeout)
    query.stop()


def main():
    print("=" * 60)
    print("  Generating Pipeline Observability Dashboard")
    print("=" * 60)

    config = load_config()

    if Path("data").exists():
        shutil.rmtree("data")
    for key in ["bronze_input", "delta_bronze", "delta_silver",
                "delta_gold", "checkpoints"]:
        ensure_path(config["paths"][key])

    input_dir = Path(config["paths"]["bronze_input"])

    print("\n  [1/5] Generating synthetic data...")
    for _ in range(6):
        write_batch(input_dir, batch_size=30)
    inject_mixed_data(input_dir)

    spark = get_spark_session(config)

    print("  [2/5] Running Bronze ingestion...")
    run_stream(lambda: write_bronze_stream(
        add_ingestion_metadata(read_bronze_raw(spark, config)), config,
    ))

    print("  [3/5] Running Silver transformation...")
    run_stream(lambda: write_silver_stream(
        apply_transformations(read_bronze_stream(spark, config), config), config,
    ))

    print("  [4/5] Running Gold aggregation...")
    counts = build_gold(spark, config)
    for name, cnt in counts.items():
        print(f"         {name}: {cnt} rows")

    print("  [5/5] Generating dashboard...")
    output = generate_html_report(spark, config)

    spark.stop()

    print(f"\n  Dashboard saved to: {output}")
    print("  Open in a browser to view.")
    print("=" * 60)


if __name__ == "__main__":
    main()
