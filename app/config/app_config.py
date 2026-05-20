import logging
from pathlib import Path

from pydantic import BaseModel

log = logging.getLogger(__name__)


# Default values can be overridden in app_config.toml
class AppConfig(BaseModel):
    model_config = {"frozen": True}  # Immutable object

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
    encoder_process_priority: str = "normal"
    vmaf_process_priority: str = "normal"
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
    max_encodes_limit: int = 2


