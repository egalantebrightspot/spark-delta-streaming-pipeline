from pathlib import Path

import yaml
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


def load_config(config_path: str = None) -> dict:
    if config_path is None:
        config_path = Path(__file__).parent / "config.yaml"
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def get_spark_session(config: dict = None) -> SparkSession:
    if config is None:
        config = load_config()

    spark_cfg = config.get("spark", {})

    builder = (
        SparkSession.builder
        .master(spark_cfg.get("master", "local[*]"))
        .appName(spark_cfg.get("app_name", "iot-telemetry-pipeline"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config(
            "spark.sql.shuffle.partitions",
            spark_cfg.get("shuffle_partitions", 4),
        )
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel(spark_cfg.get("log_level", "WARN"))
    return spark


def ensure_path(path: str) -> Path:
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p
