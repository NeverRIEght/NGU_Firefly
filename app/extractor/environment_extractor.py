import random
import re
import subprocess

from app.system.hardware import CpuInfoProvider


def get_available_cpu_threads() -> int:
    from app.config.config_manager import ConfigManager
    app_config = ConfigManager.get_config()

    actual_threads = CpuInfoProvider.get_instance().get_thread_count()

    if not app_config.randomize_threads_count:
        if app_config.threads_count == 0:
            return actual_threads
        else:
            return app_config.threads_count

    possible_options = [1, 2, 4, 8, 12, 16]
    valid_options = [opt for opt in possible_options if opt <= actual_threads]

    return random.choice(valid_options)


def extract_ffmpeg_version() -> str:
    try:
        result = subprocess.run(
            ['ffmpeg', '-version'],
            capture_output=True,
            text=True,
            check=True
        )

        first_line = result.stdout.split('\n')[0]

        match = re.search(r'version\s+([^\s]+)', first_line)

        if match:
            return match.group(1)
        return "Unknown version format"

    except (subprocess.CalledProcessError, FileNotFoundError):
        return "ffmpeg not found or error occurred"
