import logging
import threading
import tomllib
from pathlib import Path
from typing import Optional

from pydantic import BaseModel

import file_utils

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
                    ConfigManager.validate_config(cls._instance)
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

    @staticmethod
    def validate_config(config: AppConfig) -> None:
        if not file_utils.check_directory_exists(config.input_dir):
            raise ValueError(f"Input directory does not exist: {config.input_dir}")
        if not file_utils.check_directory_exists(config.output_dir):
            log.warning(f"Output directory does not exist: {config.output_dir}. Will create it.")
            config.output_dir.mkdir(parents=True, exist_ok=True)
        if config.crf_min < 0 or config.crf_max > 51 or config.crf_min >= config.crf_max:
            raise ValueError("Invalid CRF range in configuration. Expected: 0 <= crf_min < crf_max <= 51.")
        if config.initial_crf > config.crf_max or config.initial_crf < config.crf_min:
            raise ValueError("Invalid initial CRF in configuration. Expected: crf_min <= initial_crf <= crf_max.")
        if config.vmaf_min < 0.0 or config.vmaf_max > 100.0 or config.vmaf_min >= config.vmaf_max:
            raise ValueError("Invalid VMAF range in configuration. Expected: 0.0 <= vmaf_min < vmaf_max <= 100.0.")
        if config.threads_count < 0:
            raise ValueError("Threads count must be a positive integer.")
        if config.threads_count == 0:
            log.warning("Threads count is set to 0. Will use all available CPU threads.")
        if config.efficiency_threshold <= 0.0 or config.efficiency_threshold >= 0.5:
            raise ValueError(
                "Invalid efficiency threshold in configuration. Expected: 0.0 < efficiency_threshold < 0.5."
            )
        if not config.encoder_preset in [
            "ultrafast", "superfast", "veryfast", "faster", "fast",
            "medium", "slow", "slower", "veryslow", "placebo"
        ]:
            raise ValueError("Invalid encode preset in configuration.")
