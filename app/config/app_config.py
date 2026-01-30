import logging
import threading
import tomllib
from pathlib import Path
from typing import Optional

from pydantic import BaseModel

from app import file_utils
from app.extractor import environment_extractor

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
        available_threads_count = environment_extractor.extract_cpu_threads()

        if not file_utils.check_directory_exists(config.input_dir):
            raise ValueError(f"Input directory does not exist: {config.input_dir}")
        if not file_utils.check_directory_exists(config.output_dir):
            log.warning(f"Output directory does not exist: {config.output_dir}. Will create it.")
            config.output_dir.mkdir(parents=True, exist_ok=True)
        if config.threads_count < 0:
            raise ValueError("Threads count must be a positive integer.")
        if config.threads_count == 0:
            log.warning("Threads count is set to 0. Will use all available CPU threads.")
            config.threads_count = available_threads_count
        if config.threads_count > environment_extractor.extract_cpu_threads():
            log.warning("Threads count is too large for the hardware. Using maximum available threads.")
            config.threads_count = available_threads_count
        if config.low_resources_restart_delay_seconds < 0.5:
            log.warning("Low resources restart delay is lower than safe. Setting to default value of 20 seconds.")
            config.low_resources_restart_delay_seconds = 0.5
        if config.encoder_process_priority not in [
            "idle", "below_normal", "normal", "above_normal", "high", "real_time"
        ]:
            raise ValueError("Invalid encode process priority in configuration.")
        if config.vmaf_process_priority not in [
            "idle", "below_normal", "normal", "above_normal", "high", "real_time"
        ]:
            raise ValueError("Invalid VMAF process priority in configuration.")
        if config.ram_monitoring_interval_seconds < 0.5:
            log.warning("RAM monitoring interval is lower than safe. Setting to default value of 2 seconds.")
            config.ram_monitoring_interval_seconds = 0.5
        if config.ram_percent_hard_limit < 0.0 or config.ram_percent_hard_limit >= 100.0:
            raise ValueError(
                "Invalid RAM percent hard limit in configuration. Expected: 0.0 < ram_percent_hard_limit < 100.0.")
        if config.ram_percent_hard_limit == 0:
            log.warning("RAM percent hard limit is set to 0. Setting to default value of 85.")
            config.ram_percent_hard_limit = 85
        if config.ram_hard_limit_bytes < 0:
            raise ValueError("Invalid RAM hard limit bytes in configuration. Expected: ram_hard_limit_bytes >= 0.")
        if config.ram_hard_limit_bytes == 0:
            log.warning("RAM hard limit bytes is set to 0. Setting to default value of 500 MB.")
            config.ram_hard_limit_bytes = 500 * 1024 * 1024
        if config.crf_min < 0 or config.crf_max > 51 or config.crf_min >= config.crf_max:
            raise ValueError("Invalid CRF range in configuration. Expected: 0 <= crf_min < crf_max <= 51.")
        if config.initial_crf > config.crf_max or config.initial_crf < config.crf_min:
            raise ValueError("Invalid initial CRF in configuration. Expected: crf_min <= initial_crf <= crf_max.")
        if config.vmaf_min < 0.0 or config.vmaf_max > 100.0 or config.vmaf_min >= config.vmaf_max:
            raise ValueError("Invalid VMAF range in configuration. Expected: 0.0 <= vmaf_min < vmaf_max <= 100.0.")
        if config.efficiency_threshold <= 0.0 or config.efficiency_threshold >= 0.5:
            raise ValueError(
                "Invalid efficiency threshold in configuration. Expected: 0.0 < efficiency_threshold < 0.5."
            )
        if config.encoder_preset not in [
            "ultrafast", "superfast", "veryfast", "faster", "fast",
            "medium", "slow", "slower", "veryslow", "placebo"
        ]:
            raise ValueError("Invalid encode preset in configuration.")
