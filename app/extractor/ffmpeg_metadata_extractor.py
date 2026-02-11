import json
import logging
from typing import Set

from app.locking import LockManager, LockMode

log = logging.getLogger(__name__)

from pathlib import Path
import subprocess

from app.model.json.ffmpeg_metadata import FfmpegMetadata
from app.model.json.ffmpeg_metadata import HdrType


def extract(path_to_file: Path) -> FfmpegMetadata:
    with LockManager.acquire_file_operation_lock(path_to_file, LockMode.SHARED):
        if not path_to_file.is_file():
            log.error(f"File not found: {path_to_file}")
            raise FileNotFoundError(f"File not found: {path_to_file}")

        cmd = [
            'ffprobe',
            '-v', 'error',
            '-select_streams', 'v',
            '-show_entries',
            'stream=width,height,codec_name,r_frame_rate,avg_frame_rate,tags,bit_rate,profile,'
            + 'pix_fmt,chroma_location,color_primaries,color_transfer,color_space,level,side_data_list'
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

    streams = ffprobe_output.get('streams', [])
    video_streams = [s for s in streams if s.get('codec_type') == 'video']
    stream_data = video_streams[0] if video_streams else {}
    tags = stream_data.get('tags', {})

    metadata = FfmpegMetadata(
        pixel_aspect_ratio=_extract_pixel_aspect_ratio(path_to_file, stream_data, tags),
        profile=_extract_profile(path_to_file, stream_data),
        pixel_format=_extract_pixel_format(path_to_file, stream_data),
        chroma_sample_location=_extract_chroma_sample_location(path_to_file, stream_data),
        color_primaries=_extract_color_primaries(path_to_file, stream_data),
        color_trc=_extract_color_trc(path_to_file, stream_data),
        colorspace=_extract_colorspace(path_to_file, stream_data),
        level=_extract_level(path_to_file, stream_data),
        hdr_types=_detect_hdr_types(stream_data, tags),
    )

    return metadata


def _extract_pixel_aspect_ratio(file_path: Path, stream_data, tags) -> str:
    par = stream_data.get('display_aspect_ratio') or tags.get('display_aspect_ratio')
    if par is None:
        log.warning(f"Pixel aspect ratio could not be determined for {file_path}, defaulting to 1:1")
        return "1:1"

    return par


def _extract_profile(file_path: Path, stream_data) -> str | None:
    extracted_profile = stream_data.get('profile')
    if extracted_profile is None:
        log.warning(f"Profile could not be determined for {file_path}, defaulting to None")

    return extracted_profile


def _extract_pixel_format(file_path: Path, stream_data) -> str | None:
    extracted_pixel_format = stream_data.get('pix_fmt')
    if extracted_pixel_format is None:
        log.warning(f"Pixel format could not be determined for {file_path}, defaulting to None")

    return extracted_pixel_format


def _extract_chroma_sample_location(file_path: Path, stream_data) -> str | None:
    extracted_chroma = stream_data.get('chroma_location')
    if extracted_chroma is None:
        log.warning(f"Chroma sample location could not be determined for {file_path}, defaulting to None")

    return extracted_chroma


def _extract_color_primaries(file_path: Path, stream_data) -> str | None:
    extracted_primaries = stream_data.get('color_primaries')
    if extracted_primaries is None:
        log.warning(f"Color primaries could not be determined for {file_path}, defaulting to None")
        return None

    return extracted_primaries


def _extract_color_trc(file_path: Path, stream_data) -> str | None:
    extracted_trc = stream_data.get('color_transfer')
    if extracted_trc is None:
        log.warning(f"Color TRC (Transfer Characteristics) could not be determined for {file_path} defaulting to None")
        return None

    return extracted_trc


def _extract_colorspace(file_path: Path, stream_data) -> str | None:
    extracted_colorspace = stream_data.get('color_space')
    if extracted_colorspace is None:
        log.warning(
            f"Color space could not be determined for {file_path}, defaulting to None")
        return None

    return extracted_colorspace


def _extract_level(file_path: Path, stream_data) -> str | None:
    extracted_level = stream_data.get('level')
    if extracted_level is None:
        log.warning(f"Codec level could not be determined for {file_path}, defaulting to None")

    return extracted_level


def _detect_hdr_types(stream_data: dict, tags) -> Set[HdrType]:
    detected: Set[HdrType] = set()
    tags = tags or {}

    transfer = stream_data.get('color_transfer', '').lower()
    side_data_list = stream_data.get('side_data_list', [])

    is_pq = (transfer == 'smpte2084')
    is_hlg = (transfer == 'arib-std-b67')

    if is_hlg:
        detected.add(HdrType.HLG)

    if is_pq and not is_hlg:
        detected.add(HdrType.PQ)

    has_static_metadata = False

    if 'dv_profile' in tags:
        detected.add(HdrType.DOLBY_VISION)

    for entry in side_data_list:
        dt = entry.get('side_data_type', '')

        if 'DOVI' in dt or 'Dolby Vision' in dt:
            detected.add(HdrType.DOLBY_VISION)

        if 'HDR Dynamic Metadata' in dt and '2094-40' in dt:
            detected.add(HdrType.HDR10_PLUS)

        if dt in ('Mastering display metadata', 'Content light level settings'):
            has_static_metadata = True

    if is_pq and has_static_metadata:
        detected.add(HdrType.HDR10)

    return detected
