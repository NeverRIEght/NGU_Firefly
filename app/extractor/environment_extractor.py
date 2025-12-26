import re
import subprocess

from app.app_config import ConfigManager
from app.model.environment import Environment


def extract() -> Environment:
    app_config = ConfigManager.get_config()

    return Environment(
        script_version=app_config.version,
        ffmpeg_version=_extract_ffmpeg_version(),
        encoder_version="unknown",  # TODO: extract encoder version
        cpu_name="unknown",  # TODO: extract CPU name
        cpu_threads=-1  # TODO: extract CPU data
    )


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
    pass


def _extract_cpu_threads() -> int:
    pass
