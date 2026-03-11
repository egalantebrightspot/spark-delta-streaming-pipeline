"""Bronze layer: ingest raw IoT telemetry JSON into a Delta table."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, input_file_name

from pipeline.common.utils import load_config, get_spark_session, ensure_path
from pipeline.common.logging_config import get_logger
from pipeline.bronze_ingest.schema import IOT_TELEMETRY_SCHEMA

logger = get_logger("bronze.ingest")


def read_raw_stream(spark, config: dict) -> DataFrame:
    paths = config["paths"]
    streaming_cfg = config.get("streaming", {})

    return (
        spark.readStream
        .schema(IOT_TELEMETRY_SCHEMA)
        .option("maxFilesPerTrigger", streaming_cfg.get("max_files_per_trigger", 100))
        .json(paths["bronze_input"])
    )


def add_ingestion_metadata(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", input_file_name())
    )


def write_bronze_stream(df: DataFrame, config: dict):
    paths = config["paths"]
    streaming_cfg = config.get("streaming", {})
    checkpoint = f"{paths['checkpoints']}/bronze"

    return (
        df.writeStream
        .format("delta")
        .outputMode(streaming_cfg.get("output_mode", "append"))
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime=streaming_cfg.get("trigger_interval", "10 seconds"))
        .start(paths["delta_bronze"])
    )


def main():
    config = load_config()
    spark = get_spark_session(config)

    ensure_path(config["paths"]["bronze_input"])
    ensure_path(config["paths"]["delta_bronze"])

    logger.info("Starting Bronze ingestion stream")
    raw = read_raw_stream(spark, config)
    enriched = add_ingestion_metadata(raw)
    query = write_bronze_stream(enriched, config)

    logger.info("Bronze stream active — awaiting termination")
    query.awaitTermination()


if __name__ == "__main__":
    main()
