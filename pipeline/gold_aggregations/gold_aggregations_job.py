"""Gold layer: produce aggregated metrics and ML-ready feature tables.

Reads the Silver Delta table in batch mode and writes four Gold tables:

  device_summary  -- per-device windowed sensor statistics
  anomaly_summary -- anomaly breakdown by device and type
  device_health   -- composite health score with risk tier
  ml_features     -- wide feature vector for model consumption

Batch mode is the natural fit for the Gold layer because aggregations
need enough data to be statistically meaningful and downstream consumers
(BI dashboards, ML models) tolerate refresh latency.
"""

from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp

from pipeline.common.utils import load_config, get_spark_session, ensure_path
from pipeline.common.logging_config import get_logger
from pipeline.gold_aggregations.feature_engineering import (
    compute_device_summary,
    compute_anomaly_summary,
    compute_device_health,
    compute_ml_features,
)

logger = get_logger("gold.aggregations")

GOLD_TABLES = [
    "device_summary",
    "anomaly_summary",
    "device_health",
    "ml_features",
]


def read_silver_batch(spark, config: dict) -> DataFrame:
    return spark.read.format("delta").load(config["paths"]["delta_silver"])


def gold_table_path(config: dict, table_name: str) -> str:
    return str(Path(config["paths"]["delta_gold"]) / table_name)


def build_and_write(spark, config: dict) -> dict[str, int]:
    """Read Silver, compute all Gold tables, write as overwrite-mode Delta.

    Returns a dict mapping table name to row count.
    """
    gold_cfg = config.get("gold", {})
    window_duration = gold_cfg.get("window_duration", "1 hour")

    silver_df = read_silver_batch(spark, config)
    silver_count = silver_df.count()
    logger.info("Silver input rows: %d", silver_count)

    builders = {
        "device_summary": lambda: compute_device_summary(
            silver_df, window_duration,
        ),
        "anomaly_summary": lambda: compute_anomaly_summary(
            silver_df, window_duration,
        ),
        "device_health": lambda: compute_device_health(
            silver_df, config, window_duration,
        ),
        "ml_features": lambda: compute_ml_features(
            silver_df, window_duration,
        ),
    }

    counts: dict[str, int] = {}
    for name, build_fn in builders.items():
        path = gold_table_path(config, name)
        ensure_path(path)

        df = build_fn().withColumn("_aggregated_at", current_timestamp())
        df.write.format("delta").mode("overwrite").save(path)

        row_count = spark.read.format("delta").load(path).count()
        counts[name] = row_count
        logger.info("  %s -> %d rows (%s)", name, row_count, path)

    return counts


def main():
    config = load_config()
    spark = get_spark_session(config)

    ensure_path(config["paths"]["delta_gold"])

    logger.info("Starting Gold batch aggregation")
    counts = build_and_write(spark, config)

    total = sum(counts.values())
    logger.info(
        "Gold aggregation complete: %d rows across %d tables",
        total, len(counts),
    )


if __name__ == "__main__":
    main()
