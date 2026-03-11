"""Silver layer: validate, deduplicate, tag anomalies, and enrich telemetry.

Reads the Bronze Delta table as a stream, applies governance transforms
(dedup, corrupt-record removal, schema enforcement, anomaly detection,
unit normalization, z-scores, quality scoring), and writes to the Silver
Delta table with exactly-once checkpoint semantics.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp

from pipeline.common.utils import load_config, get_spark_session, ensure_path
from pipeline.common.logging_config import get_logger
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

logger = get_logger("silver.transform")


def read_bronze_stream(spark, config: dict) -> DataFrame:
    return (
        spark.readStream
        .format("delta")
        .load(config["paths"]["delta_bronze"])
    )


def apply_transformations(df: DataFrame, config: dict) -> DataFrame:
    """Apply the full Silver transformation chain.

    Order matters:
      1. Watermark (must precede streaming dropDuplicates)
      2. Drop corrupt records and null-key rows
      3. Deduplicate by device_id + timestamp
      4. Drop Bronze-internal columns
      5. Normalize sensor readings
      6. Anomaly flags (per-field + composite)
      7. Z-scores against expected population
      8. Composite quality score
      9. Processing timestamp
    """
    quality_cfg = config.get("quality", {})
    dedup_window = quality_cfg.get("dedup_window_hours", 24)

    df = df.withWatermark("timestamp", f"{dedup_window} hours")
    df = drop_corrupt_records(df)
    df = drop_nulls(df)
    df = deduplicate(df)
    df = enforce_schema(df)
    df = normalize_units(df)
    df = tag_anomalies(df, config)
    df = add_zscores(df, config)
    df = add_quality_score(df, config)
    df = df.withColumn("_processed_at", current_timestamp())
    return df


def write_silver_stream(df: DataFrame, config: dict):
    paths = config["paths"]
    streaming_cfg = config.get("streaming", {})
    checkpoint = f"{paths['checkpoints']}/silver"

    return (
        df.writeStream
        .format("delta")
        .outputMode(streaming_cfg.get("output_mode", "append"))
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime=streaming_cfg.get("trigger_interval", "10 seconds"))
        .start(paths["delta_silver"])
    )


def main():
    config = load_config()
    spark = get_spark_session(config)

    ensure_path(config["paths"]["delta_silver"])
    ensure_path(config["paths"]["checkpoints"])

    logger.info("Starting Silver transformation stream")
    bronze = read_bronze_stream(spark, config)
    silver = apply_transformations(bronze, config)
    query = write_silver_stream(silver, config)

    logger.info("Silver stream active - awaiting termination")
    query.awaitTermination()


if __name__ == "__main__":
    main()
