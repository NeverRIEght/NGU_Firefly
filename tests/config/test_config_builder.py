import tempfile
from pathlib import Path

from app.config.app_config import AppConfig
from app.config.config_manager import ConfigManager


class TestConfigBuilder:
    def __init__(self):
        tmp_directory = Path(tempfile.mkdtemp())
        input_directory = tmp_directory / "input"
        output_directory = tmp_directory / "output"

        metadata = ConfigManager.load_pyproject_metadata()
        self._config = AppConfig(
            **metadata,
            input_dir=input_directory,
            output_dir=output_directory,
        )
        self._updates = {}

    def with_input_dir(self, path: Path) -> "TestConfigBuilder":
        self._updates["input_dir"] = path
        return self

    def with_output_dir(self, path: Path) -> "TestConfigBuilder":
        self._updates["output_dir"] = path
        return self

    def with_randomize_threads_count(self, randomize: bool) -> "TestConfigBuilder":
        self._updates["randomize_threads_count"] = randomize
        return self

    def with_threads_count(self, count: int) -> "TestConfigBuilder":
        self._updates["threads_count"] = count
        return self

    def with_disable_resources_monitoring(self, disable: bool) -> "TestConfigBuilder":
        self._updates["disable_resources_monitoring"] = disable
        return self

    def with_low_resources_restart_delay_seconds(self, seconds: float) -> "TestConfigBuilder":
        self._updates["low_resources_restart_delay_seconds"] = seconds
        return self

    def with_encoder_process_priority(self, priority: str) -> "TestConfigBuilder":
        self._updates["encoder_process_priority"] = priority
        return self

    def with_vmaf_process_priority(self, priority: str) -> "TestConfigBuilder":
        self._updates["vmaf_process_priority"] = priority
        return self

    def with_ram_monitoring_interval_seconds(self, seconds: float) -> "TestConfigBuilder":
        self._updates["ram_monitoring_interval_seconds"] = seconds
        return self

    def with_ram_percent_hard_limit(self, percent: float) -> "TestConfigBuilder":
        self._updates["ram_percent_hard_limit"] = percent
        return self

    def with_ram_hard_limit_bytes(self, bytes_limit: int) -> "TestConfigBuilder":
        self._updates["ram_hard_limit_bytes"] = bytes_limit
        return self

    def with_crf_min(self, crf_min: int) -> "TestConfigBuilder":
        self._updates["crf_min"] = crf_min
        return self

    def with_crf_max(self, crf_max: int) -> "TestConfigBuilder":
        self._updates["crf_max"] = crf_max
        return self

    def with_initial_crf(self, initial_crf: int) -> "TestConfigBuilder":
        self._updates["initial_crf"] = initial_crf
        return self

    def with_vmaf_min(self, vmaf_min: int) -> "TestConfigBuilder":
        self._updates["vmaf_min"] = vmaf_min
        return self

    def with_vmaf_max(self, vmaf_max: int) -> "TestConfigBuilder":
        self._updates["vmaf_max"] = vmaf_max
        return self

    def with_efficiency_threshold(self, threshold: float) -> "TestConfigBuilder":
        self._updates["efficiency_threshold"] = threshold
        return self

    def with_encoder_preset(self, preset: str) -> "TestConfigBuilder":
        self._updates["encoder_preset"] = preset
        return self

    def with_max_encodes_limit(self, limit: int) -> "TestConfigBuilder":
        self._updates["max_encodes_limit"] = limit
        return self

    def build(self) -> AppConfig:
        if not self._updates:
            return self._config

        return self._config.model_copy(update=self._updates)
