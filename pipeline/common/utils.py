import os
import sys
import platform
from pathlib import Path

import yaml
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


def load_config(config_path: str = None) -> dict:
    if config_path is None:
        config_path = os.environ.get(
            "PIPELINE_CONFIG_PATH",
            str(Path(__file__).parent / "config.yaml"),
        )
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def _configure_windows_env():
    """Set HADOOP_HOME and pin PYSPARK_PYTHON on Windows."""
    if platform.system() != "Windows":
        return

    if not os.environ.get("HADOOP_HOME"):
        hadoop_home = r"C:\hadoop"
        if Path(hadoop_home, "bin", "winutils.exe").exists():
            os.environ["HADOOP_HOME"] = hadoop_home

    python_exe = sys.executable
    os.environ.setdefault("PYSPARK_PYTHON", python_exe)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", python_exe)


def _build_driver_java_options() -> str:
    parts = [
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
        "--add-opens=java.base/java.io=ALL-UNNAMED",
        "--add-opens=java.base/java.util=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
    ]
    if platform.system() == "Windows":
        hadoop_bin = Path(os.environ.get("HADOOP_HOME", r"C:\hadoop"), "bin")
        if hadoop_bin.exists():
            parts.append(f"-Djava.library.path={hadoop_bin.as_posix()}")
    return " ".join(parts)


def get_spark_session(config: dict = None) -> SparkSession:
    if config is None:
        config = load_config()

    _configure_windows_env()
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
        .config("spark.driver.extraJavaOptions", _build_driver_java_options())
    )

    if os.environ.get("DELTA_JARS_PREINSTALLED"):
        spark = builder.getOrCreate()
    else:
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel(spark_cfg.get("log_level", "WARN"))
    return spark


def ensure_path(path: str) -> Path:
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p
