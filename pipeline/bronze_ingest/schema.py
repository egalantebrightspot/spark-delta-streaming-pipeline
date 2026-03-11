from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType,
)

CORRUPT_RECORD_COLUMN = "_corrupt_record"

IOT_TELEMETRY_SCHEMA = StructType([
    StructField("device_id", StringType(), nullable=False),
    StructField("timestamp", TimestampType(), nullable=False),
    StructField("temperature", DoubleType(), nullable=True),
    StructField("humidity", DoubleType(), nullable=True),
    StructField("pressure", DoubleType(), nullable=True),
    StructField("battery_level", DoubleType(), nullable=True),
    StructField("location", StringType(), nullable=True),
    StructField("firmware_version", StringType(), nullable=True),
    StructField(CORRUPT_RECORD_COLUMN, StringType(), nullable=True),
])
