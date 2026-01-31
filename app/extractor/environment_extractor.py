import random
import re
import subprocess

from cpuinfo import get_cpu_info

from app.config.app_config import ConfigManager
from app.model.json.environment import Environment


def extract() -> Environment:
    app_config = ConfigManager.get_config()

    return Environment(
            script_version=app_config.app_version,
        ffmpeg_version=_extract_ffmpeg_version(),
        encoder_version="unknown",  # TODO: extract encoder version
        cpu_name=_extract_cpu_name(),
        cpu_threads=extract_cpu_threads()
    )

def get_available_cpu_threads() -> int:
    app_config = ConfigManager.get_config()
    if not app_config.randomize_threads_count:
        if app_config.threads_count == 0:
            return extract_cpu_threads()
        else:
            return app_config.threads_count

    actual_threads = extract_cpu_threads()

    possible_options = [1, 2, 4, 8, 12, 16]
    valid_options = [opt for opt in possible_options if opt <= actual_threads]

    return random.choice(valid_options)

def _extract_ffmpeg_version() -> str:
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


def _extract_encoder_version() -> str:
    pass


def _extract_cpu_name() -> str:
    cpu_info = get_cpu_info()
    cpu_model = cpu_info['brand_raw']

    if cpu_model:
        return cpu_model
    else:
        return "unknown"


def extract_cpu_threads() -> int:
    cpu_info = get_cpu_info()
    cpu_threads = cpu_info['count']

    if cpu_threads:
        return cpu_threads
    else:
        return -1
