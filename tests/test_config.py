"""Tests for configuration loading and utility helpers.

These tests do NOT require a Spark session.
"""

import pytest
from pathlib import Path


pytestmark = pytest.mark.unit


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

    def test_env_var_override(self, tmp_path, monkeypatch):
        import yaml
        from pipeline.common.utils import load_config

        custom = {"app": {"environment": "docker"}, "paths": {}}
        cfg_file = tmp_path / "docker.yaml"
        cfg_file.write_text(yaml.dump(custom))

        monkeypatch.setenv("PIPELINE_CONFIG_PATH", str(cfg_file))
        result = load_config()
        assert result["app"]["environment"] == "docker"

    def test_explicit_path_beats_env_var(self, tmp_path, monkeypatch):
        import yaml
        from pipeline.common.utils import load_config

        env_cfg = {"source": "env"}
        arg_cfg = {"source": "arg"}
        (tmp_path / "env.yaml").write_text(yaml.dump(env_cfg))
        (tmp_path / "arg.yaml").write_text(yaml.dump(arg_cfg))

        monkeypatch.setenv("PIPELINE_CONFIG_PATH", str(tmp_path / "env.yaml"))
        result = load_config(str(tmp_path / "arg.yaml"))
        assert result["source"] == "arg"

    def test_docker_config_is_valid(self):
        from pipeline.common.utils import load_config

        docker_cfg = str(
            Path(__file__).parent.parent / "infra" / "docker" / "config.docker.yaml"
        )
        config = load_config(docker_cfg)
        assert config["app"]["environment"] == "docker"
        assert config["paths"]["bronze_input"].startswith("/app/data")
        assert "quality" in config
        assert "gold" in config

    def test_generator_section_present(self):
        from pipeline.common.utils import load_config

        config = load_config()
        gen = config["generator"]
        assert gen["batch_size"] > 0
        assert gen["interval_seconds"] > 0
        assert gen["num_devices"] > 0
        assert 0 < gen["anomaly_probability"] < 1

    def test_quality_scoring_section(self):
        from pipeline.common.utils import load_config

        config = load_config()
        scoring = config["quality"]["scoring"]
        assert 0 < scoring["anomaly_penalty"] < 1
        assert len(scoring["optional_fields"]) == 6
        assert "temperature" in scoring["optional_fields"]

    def test_streaming_output_mode(self):
        from pipeline.common.utils import load_config

        config = load_config()
        assert config["streaming"]["output_mode"] == "append"

    def test_all_sections_present(self):
        """Every top-level section the pipeline depends on must exist."""
        from pipeline.common.utils import load_config

        config = load_config()
        for section in ["app", "paths", "spark", "streaming",
                        "generator", "quality", "gold", "monitoring"]:
            assert section in config, f"Missing config section: {section}"

    def test_docker_and_local_configs_have_same_sections(self):
        from pipeline.common.utils import load_config

        local_cfg = load_config()
        docker_cfg = load_config(str(
            Path(__file__).parent.parent / "infra" / "docker" / "config.docker.yaml"
        ))
        assert set(local_cfg.keys()) == set(docker_cfg.keys())

    def test_spark_timezone_is_utc(self):
        from pipeline.common.utils import load_config

        config = load_config()
        assert config["spark"]["timezone"] == "UTC"

    def test_spark_adaptive_enabled(self):
        from pipeline.common.utils import load_config

        config = load_config()
        assert config["spark"]["adaptive_enabled"] is True

    def test_spark_driver_memory_present(self):
        from pipeline.common.utils import load_config

        config = load_config()
        assert "driver_memory" in config["spark"]
        assert config["spark"]["driver_memory"].endswith("g")

    def test_spark_delta_auto_merge_enabled(self):
        from pipeline.common.utils import load_config

        config = load_config()
        assert config["spark"]["delta_auto_merge"] is True

    def test_docker_spark_section_matches_local_keys(self):
        from pipeline.common.utils import load_config

        local_spark = set(load_config()["spark"].keys())
        docker_spark = set(load_config(str(
            Path(__file__).parent.parent / "infra" / "docker" / "config.docker.yaml"
        ))["spark"].keys())
        assert local_spark == docker_spark


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

    def test_empty_string_raises_value_error(self):
        from pipeline.common.utils import ensure_path

        with pytest.raises(ValueError, match="non-empty"):
            ensure_path("")

    def test_none_raises_value_error(self):
        from pipeline.common.utils import ensure_path

        with pytest.raises(ValueError, match="non-empty"):
            ensure_path(None)


# ── Schema module ────────────────────────────────────────────────────────────


class TestSchemaModule:
    def test_corrupt_record_column_name(self):
        from pipeline.bronze_ingest.schema import CORRUPT_RECORD_COLUMN

        assert CORRUPT_RECORD_COLUMN == "_corrupt_record"

    def test_schema_is_struct_type(self):
        from pyspark.sql.types import StructType
        from pipeline.bronze_ingest.schema import IOT_TELEMETRY_SCHEMA

        assert isinstance(IOT_TELEMETRY_SCHEMA, StructType)
