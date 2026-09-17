from pathlib import Path

import pytest

from app.config.app_config import AppConfig
from app.config.config_manager import ConfigManager
from app.db.cache.lookup_cache_registry import LookupCacheRegistry
from app.db.database_manager import DatabaseManager
from tests.config.test_config_builder import TestConfigBuilder

BASE_DIR = Path(__file__).resolve().parent.parent


@pytest.fixture
def mock_app_config(monkeypatch, tmp_path) -> AppConfig:
    test_input_dir = tmp_path / "test_input_dir"
    test_output_dir = tmp_path / "test_output_dir"
    test_database_dir = tmp_path / "test_database_dir"
    test_input_dir.mkdir(parents=True, exist_ok=True)
    test_output_dir.mkdir(parents=True, exist_ok=True)
    test_database_dir.mkdir(parents=True, exist_ok=True)

    test_app_config = (
        TestConfigBuilder()
        .with_input_dir(test_input_dir)
        .with_output_dir(test_output_dir)
        .with_database_dir(test_database_dir)
        .build()
    )

    monkeypatch.setattr(ConfigManager, "get_config", lambda: test_app_config)

    DatabaseManager._instance = None
    LookupCacheRegistry._instance = None

    yield test_app_config

    DatabaseManager._instance = None
    LookupCacheRegistry._instance = None


@pytest.fixture
def init_test_db(mock_app_config):
    db_manager = DatabaseManager.get_instance()
    db_manager.init_db()
    return db_manager
