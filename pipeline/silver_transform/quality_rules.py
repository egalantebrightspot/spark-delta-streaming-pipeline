"""Data quality rules for the Silver transformation layer.

Every function is a pure DataFrame -> DataFrame transform using only
built-in Spark SQL functions (no Python UDFs) so all execution stays
on the JVM.  This avoids the Windows Python-worker crash (SPARK-53759)
and ensures the transforms work identically in batch and streaming.
"""

from functools import reduce
from operator import add

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    concat_ws,
    greatest,
    least,
    lit,
    round as spark_round,
    when,
)


# ── Cleaning ─────────────────────────────────────────────────────────────────


def drop_corrupt_records(df: DataFrame) -> DataFrame:
    """Remove rows that Spark's PERMISSIVE parser couldn't parse."""
    if "_corrupt_record" in df.columns:
        return df.filter(col("_corrupt_record").isNull()).drop("_corrupt_record")
    return df


def drop_nulls(df: DataFrame, required_cols: list[str] | None = None) -> DataFrame:
    """Remove rows missing required fields."""
    if required_cols is None:
        required_cols = ["device_id", "timestamp"]
    return df.dropna(subset=required_cols)


def deduplicate(df: DataFrame, key_cols: list[str] | None = None) -> DataFrame:
    """Remove exact duplicates by natural key."""
    if key_cols is None:
        key_cols = ["device_id", "timestamp"]
    return df.dropDuplicates(key_cols)


def enforce_schema(df: DataFrame) -> DataFrame:
    """Drop internal Bronze columns that shouldn't propagate to Silver."""
    drop_cols = ["_source_file"]
    for c in drop_cols:
        if c in df.columns:
            df = df.drop(c)
    return df


# ── Normalization ────────────────────────────────────────────────────────────


def normalize_units(df: DataFrame) -> DataFrame:
    """Clamp humidity to the physically valid [0, 100] range.

    Temperature and pressure are left as-is so anomaly detection sees the
    raw sensor value; humidity is clamped because negative humidity is a
    sensor fault, not an anomaly worth tracking.
    """
    return df.withColumn(
        "humidity",
        greatest(lit(0.0), least(lit(100.0), col("humidity"))),
    )


# ── Anomaly Detection ───────────────────────────────────────────────────────


def tag_anomalies(df: DataFrame, config: dict) -> DataFrame:
    """Add per-field anomaly flags, a composite flag, and a detail string.

    The detail string lists which fields triggered so downstream consumers
    can triage without re-evaluating thresholds.
    """
    quality = config.get("quality", {})
    max_temp = quality.get("max_temperature", 150.0)
    min_temp = quality.get("min_temperature", -50.0)
    max_hum = quality.get("max_humidity", 100.0)
    min_hum = quality.get("min_humidity", 0.0)
    max_pres = quality.get("max_pressure", 1100.0)
    min_pres = quality.get("min_pressure", 900.0)

    temp_flag = (col("temperature") > max_temp) | (col("temperature") < min_temp)
    hum_flag = (col("humidity") > max_hum) | (col("humidity") < min_hum)
    pres_flag = (col("pressure") > max_pres) | (col("pressure") < min_pres)

    return (
        df
        .withColumn("_is_temp_anomaly", temp_flag)
        .withColumn("_is_humidity_anomaly", hum_flag)
        .withColumn("_is_pressure_anomaly", pres_flag)
        .withColumn("_is_anomaly", temp_flag | hum_flag | pres_flag)
        .withColumn(
            "_anomaly_details",
            when(
                temp_flag | hum_flag | pres_flag,
                concat_ws(
                    ", ",
                    when(temp_flag, lit("temperature_out_of_range")),
                    when(hum_flag, lit("humidity_out_of_range")),
                    when(pres_flag, lit("pressure_out_of_range")),
                ),
            ),
        )
    )


# ── Derived Metrics ──────────────────────────────────────────────────────────


def add_zscores(df: DataFrame, config: dict) -> DataFrame:
    """Compute z-scores against expected population statistics from config.

    Z-score = (observed - mean) / stddev.  A |z| > 3 strongly suggests an
    anomaly even if the value is inside the hard min/max thresholds.
    """
    expected = config.get("quality", {}).get("expected", {})

    for field, defaults in [
        ("temperature", {"mean": 22.0, "stddev": 5.0}),
        ("humidity", {"mean": 55.0, "stddev": 10.0}),
        ("pressure", {"mean": 1013.25, "stddev": 10.0}),
    ]:
        exp = expected.get(field, defaults)
        mean = exp.get("mean", defaults["mean"])
        std = exp.get("stddev", defaults["stddev"])
        df = df.withColumn(
            f"_{field}_zscore",
            when(
                col(field).isNotNull(),
                spark_round((col(field) - lit(mean)) / lit(std), 2),
            ),
        )

    return df


def add_quality_score(df: DataFrame, config: dict | None = None) -> DataFrame:
    """Compute a 0.0-1.0 composite data-quality score per record.

    Components (configurable via quality.scoring in config.yaml)
    ----------
    completeness : float
        Fraction of non-null optional sensor/metadata fields.
    anomaly_penalty : float
        1.0 if clean, penalty value if any anomaly flag is set.

    Final score = completeness * anomaly_penalty.
    """
    scoring = {}
    if config:
        scoring = config.get("quality", {}).get("scoring", {})

    optional_fields = scoring.get("optional_fields", [
        "temperature", "humidity", "pressure",
        "battery_level", "location", "firmware_version",
    ])
    penalty_val = scoring.get("anomaly_penalty", 0.9)

    n = float(len(optional_fields))

    non_null_checks = [
        when(col(c).isNotNull(), lit(1.0)).otherwise(lit(0.0))
        for c in optional_fields
    ]
    completeness = reduce(add, non_null_checks) / lit(n)

    anomaly_penalty = when(col("_is_anomaly"), lit(penalty_val)).otherwise(lit(1.0))

    return df.withColumn(
        "_quality_score",
        spark_round(completeness * anomaly_penalty, 4),
    )
