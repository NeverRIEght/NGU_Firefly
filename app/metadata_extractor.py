import json
import logging
import subprocess
from pathlib import Path

from app.model import EncoderJobContext

logs_dir = Path("../logs")
logs_dir.mkdir(exist_ok=True)

log = logging.getLogger()
log.setLevel(logging.INFO)

if log.hasHandlers():
    log.handlers.clear()

logs_formatter = logging.Formatter('[%(asctime)s][%(levelname)s]: %(message)s')

all_logs_handler = logging.FileHandler(logs_dir / "full.log", mode='a', encoding='utf-8')
all_logs_handler.setLevel(logging.INFO)
all_logs_formatter = logs_formatter
all_logs_handler.setFormatter(all_logs_formatter)
log.addHandler(all_logs_handler)

error_logs_handler = logging.FileHandler(logs_dir / "errors.log", mode='a', encoding='utf-8')
error_logs_handler.setLevel(logging.ERROR)
error_logs_formatter = logs_formatter
error_logs_handler.setFormatter(error_logs_formatter)
log.addHandler(error_logs_handler)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_formatter = logs_formatter
console_handler.setFormatter(console_formatter)
log.addHandler(console_handler)


def extract(job_context: EncoderJobContext) -> EncoderJobContext:
    if job_context is None:
        raise ValueError("jobContext cannot be None")

    if not job_context.source_file_path.is_file():
        raise FileNotFoundError(f"Source file not found: {job_context.source_file_path}")

    cmd = [
        'ffprobe',
        '-v', 'error',
        '-select_streams', 'v',
        '-show_entries',
        'stream=width,height,codec_name,r_frame_rate,avg_frame_rate,tags,bit_rate:format=size,duration,bit_rate,nb_frames',
        '-of', 'json',
        str(job_context.source_file_path),
    ]

    log.info(f"Executing ffprobe for {job_context.source_file_path}")

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        ffprobe_output = json.loads(result.stdout)

    except subprocess.CalledProcessError as e:
        log.error(f"ffprobe execution failed: {e.stderr}")
        raise RuntimeError(f"Could not run ffprobe on {job_context.source_file_path}") from e
    except FileNotFoundError:
        raise RuntimeError("ffprobe is not found. Please ensure it is installed and in your PATH.")
    except json.JSONDecodeError:
        raise RuntimeError("ffprobe returned unparseable JSON.")

    stream_data = ffprobe_output.get('streams', [{}])[0]
    format_data = ffprobe_output.get('format', {})
    tags = stream_data.get('tags', {})

    fps = _extract_fps(stream_data)
    if fps == 0.0:
        log.warning(f"FPS could not be determined for {job_context.source_file_path}, defaulting to 0.0")
    job_context.report_data.source_video.fps = fps

    file_size_mb = _extract_file_size_megabytes(format_data)
    job_context.report_data.source_video.file_size_megabytes = file_size_mb

    duration_seconds = _extract_video_duration_seconds(format_data)
    if duration_seconds == 0.0:
        log.warning(f"Duration could not be determined for {job_context.source_file_path}, defaulting to 0.0")
    job_context.report_data.source_video.duration_seconds = duration_seconds

    bitrate_kbps = _extract_bitrate_kbps(stream_data, format_data)
    if bitrate_kbps == 0.0:
        log.warning(f"Bitrate could not be determined for {job_context.source_file_path}, defaulting to 0.0")
    job_context.report_data.source_video.average_bitrate_kilobits_per_second = bitrate_kbps

    codec_name = _extract_codec_name(stream_data)
    if not codec_name:
        log.warning(
            f"Codec name could not be determined for {job_context.source_file_path}, defaulting to empty string")
    job_context.report_data.source_video.codec = codec_name

    width, height = _extract_resolution(stream_data)
    if width == 0 or height == 0:
        log.warning(f"Resolution could not be determined for {job_context.source_file_path}, defaulting to 0x0")
    job_context.report_data.source_video.resolution_width = width
    job_context.report_data.source_video.resolution_height = height

    pixel_aspect_ratio = _extract_pixel_aspect_ratio(stream_data, tags)
    if not pixel_aspect_ratio:
        log.warning(
            f"Pixel aspect ratio could not be determined for {job_context.source_file_path}, defaulting to empty string"
        )
    job_context.report_data.source_video.ffmpeg_metadata.pixel_aspect_ratio = pixel_aspect_ratio

    profile = _extract_profile(stream_data)
    if not profile:
        log.warning(f"Profile could not be determined for {job_context.source_file_path}, defaulting to empty string")
    job_context.report_data.source_video.ffmpeg_metadata.profile = profile

    return job_context


def _extract_fps(stream_data) -> float:
    fps_fraction = stream_data.get('avg_frame_rate', '0/1')
    try:
        num, den = map(int, fps_fraction.split('/'))
        fps = num / den if den != 0 else 0.0
    except ValueError:
        fps = 0.0
    return fps


def _extract_file_size_megabytes(format_data) -> float:
    size_bytes = int(format_data.get('size', 0))
    size_megabytes = size_bytes / (1024 * 1024)
    return size_megabytes


def _extract_video_duration_seconds(format_data) -> float:
    duration_str = format_data.get('duration', '0.0')
    try:
        duration_seconds = float(duration_str)
    except ValueError:
        duration_seconds = 0.0
    return duration_seconds


def _extract_bitrate_kbps(stream_data, format_data):
    bitrate_str = stream_data.get('bit_rate') or format_data.get('bit_rate', 0)
    try:
        bitrate_kbps = int(bitrate_str) / 1000
    except ValueError:
        bitrate_kbps = 0.0
    return bitrate_kbps


def _extract_codec_name(stream_data) -> str:
    return stream_data.get('codec_name', '')


def _extract_resolution(stream_data) -> tuple[int, int]:
    width = int(stream_data.get('width', 0))
    height = int(stream_data.get('height', 0))
    return width, height


def _extract_pixel_aspect_ratio(stream_data, tags) -> str:
    return stream_data.get('display_aspect_ratio', tags.get('display_aspect_ratio'))


def _extract_profile(stream_data) -> str:
    return stream_data.get('profile')
