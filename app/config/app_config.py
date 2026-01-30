import logging
import threading
import tomllib
from pathlib import Path
from typing import Optional

from pydantic import BaseModel

from app import file_utils

log = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent.parent


# Default values can be overridden in app_config.toml
class AppConfig(BaseModel):
    app_name: str
    app_version: str
    compression_engine_version: int
    schema_version: int

    input_dir: Path
    output_dir: Path

    randomize_threads_count: bool = False
    threads_count: int = 0

    disable_resources_monitoring: bool = False
    low_resources_restart_delay_seconds: float = 20
    encoder_process_priority: str = "idle"
    vmaf_process_priority: str = "idle"
    ram_monitoring_interval_seconds: float = 2.0
    ram_percent_hard_limit: float = 85.0
    ram_hard_limit_bytes: int = 500 * 1024 * 1024  # 500 MB

    crf_min: int = 12
    crf_max: int = 36
    initial_crf: int = 26
    vmaf_min: float = 96.0
    vmaf_max: float = 97.0
    efficiency_threshold: float = 0.28
    encoder_preset: str = "veryslow"


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
    def load_config() -> AppConfig:
        pyproject_file = BASE_DIR / "pyproject.toml"
        config_file = BASE_DIR / "app_config.toml"

        if not file_utils.check_file_exists(pyproject_file):
            raise FileNotFoundError("pyproject.toml not found. Expected location: {}".format(pyproject_file))

        if not file_utils.check_file_exists(config_file):
            raise FileNotFoundError("app_config.toml not found. Expected location: {}".format(config_file))

        with pyproject_file.open("rb") as f:
            pyproject_data = tomllib.load(f)

        project = pyproject_data.get("project")
        metadata = pyproject_data.get("tool").get("firefly").get("metadata")

        with config_file.open("rb") as f:
            conf_data = tomllib.load(f)

        parameters = conf_data.get("params", {})

        return AppConfig(
            app_name=project.get("name"),
            app_version=project.get("version"),
            compression_engine_version=metadata.get("compression_engine_version"),
            schema_version=metadata.get("schema_version"),
            **parameters
        )
