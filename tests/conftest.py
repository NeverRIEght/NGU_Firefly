import tomllib
from pathlib import Path

import pytest

from app import file_utils
from app.config.app_config import AppConfig, ConfigManager

BASE_DIR = Path(__file__).resolve().parent.parent


@pytest.fixture
def mock_app_config(monkeypatch, tmp_path) -> AppConfig:
    test_input_dir = tmp_path / "test_input_dir"
    test_output_dir = tmp_path / "test_output_dir"
    test_input_dir.mkdir()
    test_output_dir.mkdir()

    pyproject_file = BASE_DIR / "pyproject.toml"

    if not file_utils.check_file_exists(pyproject_file):
        raise FileNotFoundError("pyproject.toml not found. Expected location: {}".format(pyproject_file))

    with pyproject_file.open("rb") as f:
        pyproject_data = tomllib.load(f)

    project = pyproject_data.get("project")
    metadata = pyproject_data.get("tool").get("firefly").get("metadata")

    test_app_config = AppConfig(
        app_name=project.get("name"),
        app_version=project.get("version"),
        compression_engine_version=metadata.get("compression_engine_version"),
        schema_version=metadata.get("schema_version"),

        input_dir=test_input_dir,
        output_dir=test_output_dir,
        randomize_threads_count=False,
        threads_count=0,
        disable_resources_monitoring=False,
        low_resources_restart_delay_seconds=20,
        encoder_process_priority="idle",
        vmaf_process_priority="idle",
        ram_monitoring_interval_seconds=2.0,
        ram_percent_hard_limit=85.0,
        ram_hard_limit_bytes=500 * 1024 * 1024,
        crf_min=12,
        crf_max=36,
        initial_crf=26,
        vmaf_min=96.0,
        vmaf_max=97.0,
        efficiency_threshold=0.28,
        encoder_preset="veryslow"
    )

    monkeypatch.setattr(ConfigManager, "get_config", lambda: test_app_config)

    return test_app_config