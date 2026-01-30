import logging
from pathlib import Path

from locking import LockManager, LockMode
from model.json.video_attributes import VideoAttributes

log = logging.getLogger(__name__)

import json
import subprocess


def extract(path_to_file: Path) -> VideoAttributes:
    with LockManager.acquire_file_operation_lock(path_to_file, LockMode.EXCLUSIVE):
        if not path_to_file.is_file():
            log.error(f"File not found: {path_to_file}")
            raise FileNotFoundError(f"File not found: {path_to_file}")

        cmd = [
            'ffprobe',
            '-v', 'error',
            '-select_streams', 'v',
            '-show_entries',
            'stream=width,height,codec_name,r_frame_rate,avg_frame_rate,tags,bit_rate,profile,'
            + 'pix_fmt,chroma_location,color_primaries,color_transfer,color_space,level'
            + ':format=size,duration,bit_rate,nb_frames',
            '-of', 'json',
            str(path_to_file),
        ]

        log.debug(f"Executing ffprobe for {path_to_file}")

        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            ffprobe_output = json.loads(result.stdout)

        except subprocess.CalledProcessError as e:
            log.error(f"ffprobe execution failed: {e.stderr}")
            raise RuntimeError(f"Could not run ffprobe on {path_to_file}") from e
        except FileNotFoundError:
            raise RuntimeError("ffprobe is not found. Please ensure it is installed and in your PATH.")
        except json.JSONDecodeError:
            raise RuntimeError("ffprobe returned unparseable JSON.")

    stream_data = ffprobe_output.get('streams', [{}])[0]
    format_data = ffprobe_output.get('format', {})

    video_attributes = VideoAttributes(
        codec=_extract_codec_name(path_to_file, stream_data),
        width_px=_extract_width(path_to_file, stream_data),
        height_px=_extract_height(path_to_file, stream_data),
        duration_seconds=_get_video_duration(path_to_file),
        fps=_extract_fps(path_to_file, stream_data),
        average_bitrate_kilobits_per_second=_extract_bitrate_kbps(path_to_file, stream_data, format_data)
    )

    return video_attributes


def _get_video_duration(video_path: Path) -> float | None:
    with LockManager.acquire_file_operation_lock(video_path, LockMode.EXCLUSIVE):
        log.debug(f"Getting video duration for: {video_path}")
        try:
            command = [
                'ffprobe',
                '-v', 'error',
                '-select_streams', 'v:0',
                '-show_entries', 'format=duration:stream=duration',
                '-of', 'json',
                str(video_path)
            ]
            result = subprocess.run(command, capture_output=True, text=True, check=True)
            data = json.loads(result.stdout)

            duration_str = data.get('streams', [{}])[0].get('duration') or data.get('format', {}).get('duration')

            if not duration_str or duration_str == 'N/A':
                log.warning(f"Duration is N/A for {video_path}")
                return None

            duration = float(duration_str)
            if duration < 0:
                return None

            return duration
        except (subprocess.CalledProcessError, KeyError, ValueError, IndexError) as e:
            log.error(f"Error getting duration: {e}")
            return None


def _extract_codec_name(file_path: Path, stream_data) -> str:
    extracted_codec = stream_data.get('codec_name', '')
    if extracted_codec is None:
        log.warning(f"Codec name could not be determined for {file_path}, defaulting to None")

    return extracted_codec


def _extract_width(file_path: Path, stream_data) -> int:
    width = stream_data.get('width', 0)
    if width is None:
        log.warning(f"Width could not be determined for {file_path}, defaulting to 0")
        width = 0

    return int(width)


def _extract_height(file_path: Path, stream_data) -> int:
    height = stream_data.get('height', 0)
    if height is None:
        log.warning(f"Height could not be determined for {file_path}, defaulting to 0")
        height = 0

    return int(height)


def _extract_fps(file_path: Path, stream_data) -> float:
    fps_fraction = stream_data.get('avg_frame_rate', '0/1')
    try:
        num, den = map(int, fps_fraction.split('/'))
        fps = num / den if den != 0 else 0.0
    except ValueError:
        log.error(f"FPS could not be determined for {file_path}, defaulting to 0.0")
        fps = 0.0
    return fps


def _extract_bitrate_kbps(file_path: Path, stream_data, format_data):
    bitrate_str = stream_data.get('bit_rate') or format_data.get('bit_rate', 0)
    try:
        bitrate_kbps = int(bitrate_str) / 1000
    except ValueError:
        log.warning(f"Bitrate could not be determined for {file_path}, defaulting to 0.0")
        bitrate_kbps = 0.0
    return bitrate_kbps
