"""Silver layer: validate, deduplicate, tag anomalies, and enrich telemetry."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp

from pipeline.common.utils import load_config, get_spark_session, ensure_path
from pipeline.common.logging_config import get_logger
from pipeline.silver_transform.quality_rules import (
    tag_anomalies,
    drop_nulls,
    deduplicate,
)

logger = get_logger("silver.transform")


def read_bronze_stream(spark, config: dict) -> DataFrame:
    return (
        spark.readStream
        .format("delta")
        .load(config["paths"]["delta_bronze"])
    )


def apply_transformations(df: DataFrame, config: dict) -> DataFrame:
    df = drop_nulls(df)
    df = deduplicate(df)
    df = tag_anomalies(df, config)
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

    logger.info("Starting Silver transformation stream")
    bronze = read_bronze_stream(spark, config)
    silver = apply_transformations(bronze, config)
    query = write_silver_stream(silver, config)

    logger.info("Silver stream active — awaiting termination")
    query.awaitTermination()


if __name__ == "__main__":
    main()
