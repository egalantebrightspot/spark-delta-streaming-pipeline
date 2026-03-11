"""Feature engineering functions for the Gold aggregation layer.

All functions are pure DataFrame -> DataFrame transforms using only
built-in Spark SQL functions (JVM-native, no Python UDFs).

Gold tables produced
--------------------
device_summary  : per-device windowed sensor statistics
anomaly_summary : anomaly breakdown by device and type
device_health   : composite health score with risk-tier classification
ml_features     : wide feature vector for model consumption
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    abs as spark_abs,
    avg,
    col,
    count,
    lit,
    max as spark_max,
    min as spark_min,
    round as spark_round,
    stddev,
    sum as spark_sum,
    when,
    window,
)


# ── Device Summary ───────────────────────────────────────────────────────────


def compute_device_summary(
    df: DataFrame, window_duration: str = "1 hour",
) -> DataFrame:
    """Per-device windowed aggregate statistics.

    One row per (device_id, time-window) with core sensor stats,
    anomaly count / rate, and average quality score.
    """
    return (
        df
        .groupBy(window(col("timestamp"), window_duration), col("device_id"))
        .agg(
            count("*").alias("event_count"),
            spark_round(avg("temperature"), 2).alias("avg_temperature"),
            spark_round(stddev("temperature"), 2).alias("stddev_temperature"),
            spark_round(spark_min("temperature"), 2).alias("min_temperature"),
            spark_round(spark_max("temperature"), 2).alias("max_temperature"),
            spark_round(avg("humidity"), 2).alias("avg_humidity"),
            spark_round(avg("pressure"), 2).alias("avg_pressure"),
            spark_round(avg("battery_level"), 2).alias("avg_battery_level"),
            spark_sum(
                when(col("_is_anomaly"), lit(1)).otherwise(lit(0))
            ).alias("anomaly_count"),
            spark_round(avg("_quality_score"), 4).alias("avg_quality_score"),
        )
        .withColumn(
            "anomaly_rate",
            spark_round(col("anomaly_count") / col("event_count"), 4),
        )
        .select(
            col("device_id"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "event_count",
            "avg_temperature", "stddev_temperature",
            "min_temperature", "max_temperature",
            "avg_humidity", "avg_pressure", "avg_battery_level",
            "anomaly_count", "anomaly_rate", "avg_quality_score",
        )
    )


# ── Anomaly Summary ─────────────────────────────────────────────────────────


def compute_anomaly_summary(
    df: DataFrame, window_duration: str = "1 hour",
) -> DataFrame:
    """Anomaly breakdown by device and type per time window.

    Shows per-field anomaly counts so operators can identify whether
    a device has a temperature-sensor fault vs a pressure-sensor drift.
    """
    return (
        df
        .groupBy(window(col("timestamp"), window_duration), col("device_id"))
        .agg(
            count("*").alias("total_events"),
            spark_sum(
                when(col("_is_temp_anomaly"), lit(1)).otherwise(lit(0))
            ).alias("temp_anomaly_count"),
            spark_sum(
                when(col("_is_humidity_anomaly"), lit(1)).otherwise(lit(0))
            ).alias("humidity_anomaly_count"),
            spark_sum(
                when(col("_is_pressure_anomaly"), lit(1)).otherwise(lit(0))
            ).alias("pressure_anomaly_count"),
            spark_sum(
                when(col("_is_anomaly"), lit(1)).otherwise(lit(0))
            ).alias("total_anomaly_count"),
        )
        .withColumn(
            "anomaly_rate",
            spark_round(col("total_anomaly_count") / col("total_events"), 4),
        )
        .select(
            col("device_id"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "total_events",
            "temp_anomaly_count", "humidity_anomaly_count",
            "pressure_anomaly_count", "total_anomaly_count",
            "anomaly_rate",
        )
    )


# ── Device Health ────────────────────────────────────────────────────────────


def compute_device_health(
    df: DataFrame, config: dict, window_duration: str = "1 hour",
) -> DataFrame:
    """Composite device health score with risk-tier classification.

    health = w_quality * avg_quality
           + w_anomaly * (1 - anomaly_rate)
           + w_battery * (avg_battery / 100)

    Risk tiers:
      healthy  >= threshold (default 0.8)
      warning  >= threshold (default 0.5)
      critical <  warning threshold
    """
    gold_cfg = config.get("gold", {})
    weights = gold_cfg.get("health_weights", {})
    quality_w = weights.get("quality", 0.4)
    anomaly_w = weights.get("anomaly_rate", 0.3)
    battery_w = weights.get("battery", 0.3)

    tiers = gold_cfg.get("risk_tiers", {})
    healthy_thresh = tiers.get("healthy", 0.8)
    warning_thresh = tiers.get("warning", 0.5)

    agg_df = (
        df
        .groupBy(window(col("timestamp"), window_duration), col("device_id"))
        .agg(
            count("*").alias("event_count"),
            avg("_quality_score").alias("avg_quality_score"),
            (
                spark_sum(when(col("_is_anomaly"), lit(1)).otherwise(lit(0)))
                / count("*")
            ).alias("anomaly_rate"),
            avg("battery_level").alias("avg_battery_level"),
        )
    )

    health_expr = (
        lit(quality_w) * col("avg_quality_score")
        + lit(anomaly_w) * (lit(1.0) - col("anomaly_rate"))
        + lit(battery_w) * (col("avg_battery_level") / lit(100.0))
    )

    return (
        agg_df
        .withColumn("health_score", spark_round(health_expr, 4))
        .withColumn(
            "risk_tier",
            when(col("health_score") >= lit(healthy_thresh), lit("healthy"))
            .when(col("health_score") >= lit(warning_thresh), lit("warning"))
            .otherwise(lit("critical")),
        )
        .select(
            col("device_id"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "event_count",
            spark_round(col("avg_quality_score"), 4).alias("avg_quality_score"),
            spark_round(col("anomaly_rate"), 4).alias("anomaly_rate"),
            spark_round(col("avg_battery_level"), 2).alias("avg_battery_level"),
            "health_score", "risk_tier",
        )
    )


# ── ML Feature Table ────────────────────────────────────────────────────────


def compute_ml_features(
    df: DataFrame, window_duration: str = "1 hour",
) -> DataFrame:
    """Wide feature vector per device-window for model consumption.

    Columns are named for direct use as ML model inputs:
      - Sensor stats: mean, stddev, min, max, range
      - Battery stats: mean, min, max
      - Anomaly features: counts, rate
      - Quality features: mean, min
      - Z-score features: mean-absolute, max-absolute
    """
    return (
        df
        .groupBy(window(col("timestamp"), window_duration), col("device_id"))
        .agg(
            count("*").alias("event_count"),
            # Temperature
            spark_round(avg("temperature"), 4).alias("temp_mean"),
            spark_round(stddev("temperature"), 4).alias("temp_stddev"),
            spark_round(spark_min("temperature"), 4).alias("temp_min"),
            spark_round(spark_max("temperature"), 4).alias("temp_max"),
            # Humidity
            spark_round(avg("humidity"), 4).alias("hum_mean"),
            spark_round(stddev("humidity"), 4).alias("hum_stddev"),
            spark_round(spark_min("humidity"), 4).alias("hum_min"),
            spark_round(spark_max("humidity"), 4).alias("hum_max"),
            # Pressure
            spark_round(avg("pressure"), 4).alias("pres_mean"),
            spark_round(stddev("pressure"), 4).alias("pres_stddev"),
            spark_round(spark_min("pressure"), 4).alias("pres_min"),
            spark_round(spark_max("pressure"), 4).alias("pres_max"),
            # Battery
            spark_round(avg("battery_level"), 4).alias("battery_mean"),
            spark_round(spark_min("battery_level"), 4).alias("battery_min"),
            spark_round(spark_max("battery_level"), 4).alias("battery_max"),
            # Anomaly counts
            spark_sum(
                when(col("_is_anomaly"), lit(1)).otherwise(lit(0))
            ).alias("anomaly_count"),
            spark_sum(
                when(col("_is_temp_anomaly"), lit(1)).otherwise(lit(0))
            ).alias("temp_anomaly_count"),
            spark_sum(
                when(col("_is_humidity_anomaly"), lit(1)).otherwise(lit(0))
            ).alias("hum_anomaly_count"),
            spark_sum(
                when(col("_is_pressure_anomaly"), lit(1)).otherwise(lit(0))
            ).alias("pres_anomaly_count"),
            # Quality
            spark_round(avg("_quality_score"), 4).alias("quality_mean"),
            spark_round(spark_min("_quality_score"), 4).alias("quality_min"),
            # Z-score summaries
            spark_round(
                avg(spark_abs(col("_temperature_zscore"))), 4
            ).alias("temp_zscore_abs_mean"),
            spark_round(
                spark_max(spark_abs(col("_temperature_zscore"))), 4
            ).alias("temp_zscore_abs_max"),
            spark_round(
                avg(spark_abs(col("_humidity_zscore"))), 4
            ).alias("hum_zscore_abs_mean"),
            spark_round(
                spark_max(spark_abs(col("_humidity_zscore"))), 4
            ).alias("hum_zscore_abs_max"),
            spark_round(
                avg(spark_abs(col("_pressure_zscore"))), 4
            ).alias("pres_zscore_abs_mean"),
            spark_round(
                spark_max(spark_abs(col("_pressure_zscore"))), 4
            ).alias("pres_zscore_abs_max"),
        )
        .withColumn(
            "temp_range", spark_round(col("temp_max") - col("temp_min"), 4),
        )
        .withColumn(
            "hum_range", spark_round(col("hum_max") - col("hum_min"), 4),
        )
        .withColumn(
            "pres_range", spark_round(col("pres_max") - col("pres_min"), 4),
        )
        .withColumn(
            "anomaly_rate",
            spark_round(col("anomaly_count") / col("event_count"), 4),
        )
        .select(
            col("device_id"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "event_count",
            "temp_mean", "temp_stddev", "temp_min", "temp_max", "temp_range",
            "hum_mean", "hum_stddev", "hum_min", "hum_max", "hum_range",
            "pres_mean", "pres_stddev", "pres_min", "pres_max", "pres_range",
            "battery_mean", "battery_min", "battery_max",
            "anomaly_count", "anomaly_rate",
            "temp_anomaly_count", "hum_anomaly_count", "pres_anomaly_count",
            "quality_mean", "quality_min",
            "temp_zscore_abs_mean", "temp_zscore_abs_max",
            "hum_zscore_abs_mean", "hum_zscore_abs_max",
            "pres_zscore_abs_mean", "pres_zscore_abs_max",
        )
    )
