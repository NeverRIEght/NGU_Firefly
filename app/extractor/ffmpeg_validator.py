import logging
import shutil
import subprocess
from typing import List

from app.extractor import FfmpegValidationError

log = logging.getLogger(__name__)


class FfmpegValidator:
    _REQUIRED_BINARIES: List[str] = ["ffmpeg", "ffprobe"]
    _REQUIRED_ENCODERS: List[str] = ["libx265"]
    _REQUIRED_FILTERS: List[str] = ["libvmaf", "scale2ref", "format"]

    @classmethod
    def validate_environment(cls) -> None:
        """
        Validates that FFmpeg and FFprobe are installed and have all
        necessary modules (encoders, filters) compiled in.
        Throws FfmpegValidationError if any check fails.
        """
        cls._verify_binaries_present()
        cls._verify_encoders_present()
        cls._verify_filters_present()
        log.debug("FFmpeg environment verification passed.")

    @classmethod
    def _verify_binaries_present(cls) -> None:
        missing_binaries = []
        for binary in cls._REQUIRED_BINARIES:
            if shutil.which(binary) is None:
                missing_binaries.append(binary)

        if missing_binaries:
            raise FfmpegValidationError(
                f"Missing required executables in PATH: {', '.join(missing_binaries)}. "
                "Please install FFmpeg and FFprobe and ensure they are in your system PATH."
            )

    @classmethod
    def _verify_encoders_present(cls) -> None:
        available_encoders = cls._query_ffmpeg(["ffmpeg", "-hide_banner", "-encoders"])
        missing_encoders = []

        for encoder in cls._REQUIRED_ENCODERS:
            if not any(encoder in line for line in available_encoders):
                missing_encoders.append(encoder)

        if missing_encoders:
            raise FfmpegValidationError(
                f"FFmpeg is missing required encoders: {', '.join(missing_encoders)}. "
                "Ensure your FFmpeg build was configured with '--enable-libx265' and '--enable-gpl'."
            )

    @classmethod
    def _verify_filters_present(cls) -> None:
        available_filters = cls._query_ffmpeg(["ffmpeg", "-hide_banner", "-filters"])
        missing_filters = []

        for filter_name in cls._REQUIRED_FILTERS:
            if not any(cls._filter_matches(line, filter_name) for line in available_filters):
                missing_filters.append(filter_name)

        if missing_filters:
            raise FfmpegValidationError(
                f"FFmpeg is missing required filters: {', '.join(missing_filters)}. "
                "Ensure your FFmpeg build was configured with '--enable-libvmaf'."
            )

    @classmethod
    def _filter_matches(cls, line: str, filter_name: str) -> bool:
        tokens = line.split()
        return len(tokens) >= 2 and tokens[1] == filter_name

    @classmethod
    def _query_ffmpeg(cls, command: List[str]) -> List[str]:
        try:
            result = subprocess.run(
                command,
                capture_output=True,
                text=True,
                check=True
            )
            return result.stdout.splitlines()
        except (subprocess.CalledProcessError, FileNotFoundError) as e:
            raise FfmpegValidationError(f"Failed to query FFmpeg capabilities via {command}: {e}") from e