import logging

from app import file_utils
from app.config.app_config import AppConfig
from app.system.hardware import CpuInfoProvider

log = logging.getLogger(__name__)


class ConfigValidator:
    @staticmethod
    def validate(config: AppConfig) -> None:
        available_threads_count = CpuInfoProvider.get_instance().get_thread_count()

        if not file_utils.check_directory_exists(config.input_dir):
            raise ValueError(f"Input directory does not exist: {config.input_dir}")

        if not file_utils.check_directory_exists(config.output_dir):
            log.warning(f"Output directory does not exist: {config.output_dir}. Will create it.")
            config.output_dir.mkdir(parents=True, exist_ok=True)

        if config.database_dir is not None:
            if str(config.database_dir).strip() in ("", "."):
                raise ValueError(
                    "database_dir in configuration cannot be empty. "
                    "Either comment it out in app_config.toml to use the default system drive, "
                    "or specify a valid directory path."
                )
            if not file_utils.check_directory_exists(config.database_dir):
                log.warning(f"Database directory does not exist: {config.database_dir}. Will create it.")
                try:
                    config.database_dir.mkdir(parents=True, exist_ok=True)
                except Exception as e:
                    raise ValueError(
                        f"Custom database directory does not exist and cannot be created: {config.database_dir}."
                        f" Error: {e}"
                    )

        if config.threads_count <= 0:
            raise ValueError(
                f"threads_count in configuration cannot be 0 or negative. "
                f"Your system has {available_threads_count} available threads. Please specify a positive integer."
            )
        if config.threads_count > available_threads_count:
            raise ValueError(
                f"threads_count in configuration cannot be higher than amount of available threads. "
                f"Your system has {available_threads_count} available threads. Please specify a positive integer."
            )

        if config.low_resources_restart_delay_seconds < 0.5:
            raise ValueError(
                f"low_resources_restart_delay_seconds ({config.low_resources_restart_delay_seconds}) in configuration "
                f"is lower than the minimal safe value of 0.5 seconds. Please set it to at least 0.5 in app_config.toml."
            )

        valid_priorities = {"idle", "below_normal", "normal", "above_normal", "high", "real_time"}
        if config.encoder_process_priority not in valid_priorities:
            raise ValueError("Invalid encode process priority in configuration.")
        if config.vmaf_process_priority not in valid_priorities:
            raise ValueError("Invalid VMAF process priority in configuration.")

        if config.ram_monitoring_interval_seconds < 0.5:
            raise ValueError(
                f"ram_monitoring_interval_seconds ({config.ram_monitoring_interval_seconds}) in configuration "
                f"is lower than the minimal safe value of 0.5 seconds. Please set it to at least 0.5 in app_config.toml."
            )

        if config.ram_percent_hard_limit <= 0.0 or config.ram_percent_hard_limit >= 100.0:
            raise ValueError(
                f"Invalid RAM percent hard limit ({config.ram_percent_hard_limit}) in configuration. "
                f"Expected: 0.0 < ram_percent_hard_limit < 100.0. Please set it to a value between 0.0 and 100.0 in app_config.toml."
            )

        if config.ram_hard_limit_bytes <= 0:
            raise ValueError(
                f"Invalid RAM hard limit bytes ({config.ram_hard_limit_bytes}) in configuration. "
                f"Please set it to a positive value in app_config.toml."
            )

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

        valid_presets = {
            "ultrafast", "superfast", "veryfast", "faster", "fast",
            "medium", "slow", "slower", "veryslow", "placebo"
        }
        if config.encoder_preset not in valid_presets:
            raise ValueError("Invalid encode preset in configuration.")
