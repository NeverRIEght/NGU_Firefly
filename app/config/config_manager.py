import threading
import tomllib
from typing import Optional

from app.config.app_config import AppConfig
from app.config.config_validator import ConfigValidator
from app.project_paths import ProjectPaths


class ConfigManager:
    _instance: Optional[AppConfig] = None
    _lock = threading.Lock()

    def __init__(self):
        raise RuntimeError("Constructor is not allowed. Use get_config() method.")

    @classmethod
    def get_config(cls) -> AppConfig:
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = ConfigManager.load_config()
        return cls._instance

    @staticmethod
    def load_pyproject_metadata() -> dict:
        project_paths = ProjectPaths.get_instance()
        pyproject_file = project_paths.pyproject_file

        with pyproject_file.open("rb") as f:
            pyproject_data = tomllib.load(f)

        project = pyproject_data.get("project", {})
        metadata = pyproject_data.get("tool", {}).get("firefly", {}).get("metadata", {})

        return {
            "app_name": project["name"],
            "app_version": project["version"],
            "compression_engine_version": metadata["compression_engine_version"],
            "schema_version": metadata["schema_version"],
        }

    @staticmethod
    def load_config() -> AppConfig:
        project_paths = ProjectPaths.get_instance()
        config_file = project_paths.app_config_file

        metadata = ConfigManager.load_pyproject_metadata()

        with config_file.open("rb") as f:
            conf_data = tomllib.load(f)

        parameters = conf_data.get("params", {})

        raw_config = AppConfig(
            **metadata,
            **parameters
        )

        validated_config = ConfigValidator.validate(raw_config)

        return validated_config
