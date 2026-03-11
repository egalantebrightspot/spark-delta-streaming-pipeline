"""Central utilities: Spark session factory, config loading, path helpers.

This module is the single gateway for every SparkSession in the pipeline.
Any Spark, Delta Lake, or environment configuration applied here propagates
to Bronze, Silver, Gold, and all scripts/tests.
"""

import os
import sys
import platform
from pathlib import Path

import yaml
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


def load_config(config_path: str = None) -> dict:
    """Load pipeline configuration from YAML.

    Resolution order:
      1. Explicit ``config_path`` argument (highest priority)
      2. ``PIPELINE_CONFIG_PATH`` environment variable
      3. ``pipeline/common/config.yaml`` (default)
    """
    if config_path is None:
        config_path = os.environ.get(
            "PIPELINE_CONFIG_PATH",
            str(Path(__file__).parent / "config.yaml"),
        )
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def _configure_windows_env():
    """Set HADOOP_HOME and pin PYSPARK_PYTHON on Windows.

    Windows requires ``winutils.exe`` for HDFS-compatible file operations.
    Without explicit PYSPARK_PYTHON, PySpark may launch the wrong
    interpreter when multiple Python installs exist (SPARK-53759).
    """
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
    """Build JVM options for the Spark driver.

    The ``--add-opens`` flags are required for Java 17+ where the module
    system blocks reflective access that Spark's internals depend on.
    On Windows, ``-Djava.library.path`` points to the Hadoop native DLLs.
    """
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
    """Create or retrieve a configured SparkSession.

    Applies all Spark, Delta Lake, and environment settings from the
    ``spark`` section of the pipeline config.  This is the **only**
    factory function used across the entire pipeline so that every
    job, script, and test gets an identical session configuration.
    """
    if config is None:
        config = load_config()

    _configure_windows_env()
    spark_cfg = config.get("spark", {})

    builder = (
        SparkSession.builder
        .master(spark_cfg.get("master", "local[*]"))
        .appName(spark_cfg.get("app_name", "iot-telemetry-pipeline"))

        # ── Delta Lake ──────────────────────────────────────────────
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config(
            "spark.databricks.delta.schema.autoMerge.enabled",
            str(spark_cfg.get("delta_auto_merge", True)).lower(),
        )

        # ── Timestamps ─────────────────────────────────────────────
        # Force UTC so timestamps are identical on Windows, Linux,
        # Docker, and any cloud environment regardless of OS timezone.
        .config(
            "spark.sql.session.timeZone",
            spark_cfg.get("timezone", "UTC"),
        )

        # ── Performance ────────────────────────────────────────────
        .config(
            "spark.sql.shuffle.partitions",
            spark_cfg.get("shuffle_partitions", 4),
        )
        .config(
            "spark.sql.adaptive.enabled",
            str(spark_cfg.get("adaptive_enabled", True)).lower(),
        )
        .config(
            "spark.driver.memory",
            spark_cfg.get("driver_memory", "2g"),
        )

        # ── JVM ────────────────────────────────────────────────────
        .config("spark.driver.extraJavaOptions", _build_driver_java_options())
    )

    if os.environ.get("DELTA_JARS_PREINSTALLED"):
        spark = builder.getOrCreate()
    else:
        spark = configure_spark_with_delta_pip(builder).getOrCreate()

    spark.sparkContext.setLogLevel(spark_cfg.get("log_level", "WARN"))
    return spark


def ensure_path(path: str) -> Path:
    """Create a directory tree if it doesn't exist and return the Path.

    Safe to call repeatedly (idempotent).  Raises ``ValueError`` if
    *path* is empty or None.
    """
    if not path:
        raise ValueError("ensure_path requires a non-empty path string")
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p
