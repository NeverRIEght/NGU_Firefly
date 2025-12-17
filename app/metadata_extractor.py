import logging

log = logging.getLogger(__name__)

import json
import math
import subprocess

from hashing_service import calculate_sha256_hash
from model import EncoderJobContext, Resolution, SourceVideo, FfmpegMetadata, EncodingStageNamesEnum


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
        'stream=width,height,codec_name,r_frame_rate,avg_frame_rate,tags,bit_rate,profile,'
        + 'pix_fmt,chroma_location,color_primaries,color_transfer,color_space,level'
        + ':format=size,duration,bit_rate,nb_frames',
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

    duration_seconds = _extract_video_duration_seconds(job_context, format_data)
    fps_value = _extract_fps(job_context, stream_data)

    source_video = SourceVideo(
        file_name=job_context.source_file_path.stem,
        file_size_megabytes=_extract_file_size_megabytes(format_data),
        resolution=_extract_resolution(stream_data),
        video_duration_seconds=duration_seconds,
        codec=_extract_codec_name(job_context, stream_data),
        average_bitrate_kilobits_per_second=_extract_bitrate_kbps(job_context, stream_data, format_data),
        fps=fps_value,
        actual_frame_count=math.floor(fps_value * duration_seconds),
        sha256_hash=calculate_sha256_hash(job_context.source_file_path)
    )

    metadata = FfmpegMetadata(
        pixel_aspect_ratio=_extract_pixel_aspect_ratio(job_context, stream_data, tags),
        profile=_extract_profile(job_context, stream_data),
        pixel_format=_extract_pixel_format(job_context, stream_data),
        chroma_sample_location=_extract_chroma_sample_location(job_context, stream_data),
        color_primaries=_extract_color_primaries(job_context, stream_data),
        color_trc=_extract_color_trc(job_context, stream_data),
        colorspace=_extract_colorspace(job_context, stream_data),
        level=_extract_level(job_context, stream_data)
    )

    job_context.report_data.source_video = source_video
    job_context.report_data.source_video.ffmpeg_metadata = metadata

    job_context.report_data.encoding_stage.stage_name = EncodingStageNamesEnum.METADATA_EXTRACTED
    job_context.report_data.encoding_stage.stage_number_from_1 = 2

    return job_context


def _extract_fps(job_context: EncoderJobContext, stream_data) -> float:
    fps_fraction = stream_data.get('avg_frame_rate', '0/1')
    try:
        num, den = map(int, fps_fraction.split('/'))
        fps = num / den if den != 0 else 0.0
    except ValueError:
        log.error(f"FPS could not be determined for {job_context.source_file_path}, defaulting to 0.0")
        fps = 0.0
    return fps


def _extract_file_size_megabytes(format_data) -> float:
    size_bytes = int(format_data.get('size', 0))
    size_megabytes = size_bytes / (1024 * 1024)
    return size_megabytes


def _extract_video_duration_seconds(job_context: EncoderJobContext, format_data) -> float:
    duration_str = format_data.get('duration', '0.0')
    try:
        duration_seconds = float(duration_str)
    except ValueError:
        log.warning(f"Duration could not be determined for {job_context.source_file_path}, defaulting to 0.0")
        duration_seconds = 0.0
    return duration_seconds


def _extract_bitrate_kbps(job_context: EncoderJobContext, stream_data, format_data):
    bitrate_str = stream_data.get('bit_rate') or format_data.get('bit_rate', 0)
    try:
        bitrate_kbps = int(bitrate_str) / 1000
    except ValueError:
        log.warning(f"Bitrate could not be determined for {job_context.source_file_path}, defaulting to 0.0")
        bitrate_kbps = 0.0
    return bitrate_kbps


def _extract_codec_name(job_context: EncoderJobContext, stream_data) -> str:
    extracted_codec = stream_data.get('codec_name', '')
    if extracted_codec is None:
        log.warning(f"Codec name could not be determined for {job_context.source_file_path}, defaulting to None")

    return extracted_codec


def _extract_resolution(stream_data) -> Resolution:
    width = stream_data.get('width', 0)
    height = stream_data.get('height', 0)

    if width is None or height is None:
        log.warning("Width or height is missing, defaulting to 0")
        width = int(width) or 0
        height = int(height) or 0

    extracted_resolution = Resolution(
        width_px=width,
        height_px=height
    )

    return extracted_resolution


def _extract_pixel_aspect_ratio(job_context: EncoderJobContext, stream_data, tags) -> str:
    par = stream_data.get('display_aspect_ratio') or tags.get('display_aspect_ratio')
    if par is None:
        f"Pixel aspect ratio could not be determined for {job_context.source_file_path}, defaulting to 1:1"
        return "1:1"

    return par


def _extract_profile(job_context: EncoderJobContext, stream_data) -> str | None:
    extracted_profile = stream_data.get('profile')
    if extracted_profile is None:
        log.warning(f"Profile could not be determined for {job_context.source_file_path}, defaulting to None")

    return extracted_profile


def _extract_pixel_format(job_context: EncoderJobContext, stream_data) -> str | None:
    extracted_pixel_format = stream_data.get('pix_fmt')
    if extracted_pixel_format is None:
        log.warning(f"Pixel format could not be determined for {job_context.source_file_path}, defaulting to None")

    return extracted_pixel_format


def _extract_chroma_sample_location(job_context: EncoderJobContext, stream_data) -> str | None:
    extracted_chroma = stream_data.get('chroma_location')
    if extracted_chroma is None:
        log.warning(
            f"Chroma sample location could not be determined for {job_context.source_file_path}, defaulting to None")

    return extracted_chroma


def _extract_color_primaries(job_context: EncoderJobContext, stream_data) -> str | None:
    extracted_primaries = stream_data.get('color_primaries')
    if extracted_primaries is None:
        log.warning(f"Color primaries could not be determined for {job_context.source_file_path}, defaulting to None")
        return None

    return extracted_primaries


def _extract_color_trc(job_context: EncoderJobContext, stream_data) -> str | None:
    extracted_trc = stream_data.get('color_transfer')
    if extracted_trc is None:
        log.warning(
            f"Color TRC (Transfer Characteristics) could not be determined for {job_context.source_file_path},"
            f"defaulting to None")
        return None

    return extracted_trc


def _extract_colorspace(job_context: EncoderJobContext, stream_data) -> str | None:
    extracted_colorspace = stream_data.get('color_space')
    if extracted_colorspace is None:
        log.warning(
            f"Color space could not be determined for {job_context.source_file_path}, defaulting to None")
        return None

    return extracted_colorspace


def _extract_level(job_context: EncoderJobContext, stream_data) -> str | None:
    extracted_level = stream_data.get('level')
    if extracted_level is None:
        log.warning(f"Codec level could not be determined for {job_context.source_file_path}, defaulting to None")

    return extracted_level
