import logging

from app import file_utils
from app.config.app_config import AppConfig
from app.extractor import environment_extractor

log = logging.getLogger(__name__)


class ConfigValidator:
    @staticmethod
    def validate(config: AppConfig) -> None:
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
