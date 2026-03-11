"""Tests for configuration loading and utility helpers.

These tests do NOT require a Spark session.
"""

import pytest
from pathlib import Path


# ── load_config ──────────────────────────────────────────────────────────────


class TestLoadConfig:
    def test_loads_default_config(self):
        from pipeline.common.utils import load_config

        config = load_config()
        assert isinstance(config, dict)
        assert "paths" in config
        assert "spark" in config
        assert "quality" in config
        assert "gold" in config

    def test_default_config_has_required_paths(self):
        from pipeline.common.utils import load_config

        config = load_config()
        paths = config["paths"]
        for key in ["bronze_input", "delta_bronze", "delta_silver",
                     "delta_gold", "checkpoints"]:
            assert key in paths, f"Missing path key: {key}"

    def test_quality_thresholds_present(self):
        from pipeline.common.utils import load_config

        config = load_config()
        quality = config["quality"]
        for key in ["max_temperature", "min_temperature",
                     "max_humidity", "min_humidity",
                     "max_pressure", "min_pressure"]:
            assert key in quality
            assert isinstance(quality[key], (int, float))

    def test_quality_expected_statistics(self):
        from pipeline.common.utils import load_config

        config = load_config()
        expected = config["quality"]["expected"]
        for field in ["temperature", "humidity", "pressure"]:
            assert "mean" in expected[field]
            assert "stddev" in expected[field]
            assert expected[field]["stddev"] > 0

    def test_gold_health_weights_sum_to_one(self):
        from pipeline.common.utils import load_config

        config = load_config()
        weights = config["gold"]["health_weights"]
        total = weights["quality"] + weights["anomaly_rate"] + weights["battery"]
        assert total == pytest.approx(1.0)

    def test_risk_tier_thresholds_ordered(self):
        from pipeline.common.utils import load_config

        config = load_config()
        tiers = config["gold"]["risk_tiers"]
        assert tiers["healthy"] > tiers["warning"]

    def test_custom_config_path(self, tmp_path):
        import yaml
        from pipeline.common.utils import load_config

        custom = {"spark": {"master": "local[2]"}, "paths": {}}
        cfg_file = tmp_path / "custom.yaml"
        cfg_file.write_text(yaml.dump(custom))

        result = load_config(str(cfg_file))
        assert result["spark"]["master"] == "local[2]"

    def test_missing_config_raises_error(self):
        from pipeline.common.utils import load_config

        with pytest.raises(FileNotFoundError):
            load_config("/nonexistent/path/config.yaml")


# ── ensure_path ──────────────────────────────────────────────────────────────


class TestEnsurePath:
    def test_creates_directory(self, tmp_path):
        from pipeline.common.utils import ensure_path

        target = str(tmp_path / "new" / "nested" / "dir")
        result = ensure_path(target)
        assert result.exists()
        assert result.is_dir()

    def test_returns_path_object(self, tmp_path):
        from pipeline.common.utils import ensure_path

        result = ensure_path(str(tmp_path / "test_dir"))
        assert isinstance(result, Path)

    def test_idempotent(self, tmp_path):
        from pipeline.common.utils import ensure_path

        target = str(tmp_path / "idempotent")
        ensure_path(target)
        ensure_path(target)
        assert Path(target).exists()

    def test_existing_directory_noop(self, tmp_path):
        from pipeline.common.utils import ensure_path

        result = ensure_path(str(tmp_path))
        assert result == tmp_path


# ── Schema module ────────────────────────────────────────────────────────────


class TestSchemaModule:
    def test_corrupt_record_column_name(self):
        from pipeline.bronze_ingest.schema import CORRUPT_RECORD_COLUMN

        assert CORRUPT_RECORD_COLUMN == "_corrupt_record"

    def test_schema_is_struct_type(self):
        from pyspark.sql.types import StructType
        from pipeline.bronze_ingest.schema import IOT_TELEMETRY_SCHEMA

        assert isinstance(IOT_TELEMETRY_SCHEMA, StructType)
