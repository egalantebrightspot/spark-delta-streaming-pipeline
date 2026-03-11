"""Gold layer: produce aggregated metrics and ML-ready feature tables."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp

from pipeline.common.utils import load_config, get_spark_session, ensure_path
from pipeline.common.logging_config import get_logger
from pipeline.gold_aggregations.feature_engineering import compute_device_features

logger = get_logger("gold.aggregations")


def read_silver_stream(spark, config: dict) -> DataFrame:
    return (
        spark.readStream
        .format("delta")
        .load(config["paths"]["delta_silver"])
    )


def build_gold_tables(df: DataFrame) -> DataFrame:
    features = compute_device_features(df, window_duration="1 hour")
    return features.withColumn("_aggregated_at", current_timestamp())


def write_gold_stream(df: DataFrame, config: dict):
    paths = config["paths"]
    streaming_cfg = config.get("streaming", {})
    checkpoint = f"{paths['checkpoints']}/gold"

    return (
        df.writeStream
        .format("delta")
        .outputMode("complete")
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime=streaming_cfg.get("trigger_interval", "10 seconds"))
        .start(paths["delta_gold"])
    )


def main():
    config = load_config()
    spark = get_spark_session(config)

    ensure_path(config["paths"]["delta_gold"])

    logger.info("Starting Gold aggregation stream")
    silver = read_silver_stream(spark, config)
    gold = build_gold_tables(silver)
    query = write_gold_stream(gold, config)

    logger.info("Gold stream active — awaiting termination")
    query.awaitTermination()


if __name__ == "__main__":
    main()
