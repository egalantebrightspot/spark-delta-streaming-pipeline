"""Data quality rules applied during the Silver transformation layer."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit


def tag_anomalies(df: DataFrame, config: dict) -> DataFrame:
    quality = config.get("quality", {})
    max_temp = quality.get("max_temperature", 150.0)
    min_temp = quality.get("min_temperature", -50.0)
    max_hum = quality.get("max_humidity", 100.0)
    min_hum = quality.get("min_humidity", 0.0)

    return df.withColumn(
        "_anomaly",
        when(
            (col("temperature") > max_temp)
            | (col("temperature") < min_temp)
            | (col("humidity") > max_hum)
            | (col("humidity") < min_hum),
            lit(True),
        ).otherwise(lit(False)),
    )


def drop_nulls(df: DataFrame, required_cols: list[str] = None) -> DataFrame:
    if required_cols is None:
        required_cols = ["device_id", "timestamp"]
    return df.dropna(subset=required_cols)


def deduplicate(df: DataFrame, key_cols: list[str] = None) -> DataFrame:
    if key_cols is None:
        key_cols = ["device_id", "timestamp"]
    return df.dropDuplicates(key_cols)
