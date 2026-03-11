"""Feature engineering utilities for the Gold layer."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    avg,
    stddev,
    count,
    window,
)


def compute_device_features(df: DataFrame, window_duration: str = "1 hour") -> DataFrame:
    return (
        df
        .filter(col("_anomaly") == False)  # noqa: E712
        .groupBy(
            window(col("timestamp"), window_duration),
            col("device_id"),
        )
        .agg(
            avg("temperature").alias("avg_temperature"),
            stddev("temperature").alias("std_temperature"),
            avg("humidity").alias("avg_humidity"),
            avg("pressure").alias("avg_pressure"),
            avg("battery_level").alias("avg_battery"),
            count("*").alias("reading_count"),
        )
        .select(
            col("device_id"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "avg_temperature",
            "std_temperature",
            "avg_humidity",
            "avg_pressure",
            "avg_battery",
            "reading_count",
        )
    )
