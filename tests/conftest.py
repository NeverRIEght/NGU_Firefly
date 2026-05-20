from pathlib import Path

import pytest

from app.config.app_config import AppConfig
from app.config.config_manager import ConfigManager
from tests.config.test_config_builder import TestConfigBuilder

BASE_DIR = Path(__file__).resolve().parent.parent


@pytest.fixture
def mock_app_config(monkeypatch, tmp_path) -> AppConfig:
    test_input_dir = tmp_path / "test_input_dir"
    test_output_dir = tmp_path / "test_output_dir"
    test_input_dir.mkdir()
    test_output_dir.mkdir()

    test_app_config = (
        TestConfigBuilder()
        .with_input_dir(test_input_dir)
        .with_output_dir(test_output_dir)
        .build()
    )

    monkeypatch.setattr(ConfigManager, "get_config", lambda: test_app_config)

    return test_app_config
