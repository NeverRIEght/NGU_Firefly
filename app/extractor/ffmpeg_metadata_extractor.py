import json
import logging

log = logging.getLogger(__name__)

from pathlib import Path
import subprocess

from app.model.ffmpeg_metadata import FfmpegMetadata


def extract(path_to_file: Path) -> FfmpegMetadata:
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
        is_hdr=_detect_hdr(path_to_file, stream_data),
    )

    return metadata


def _extract_pixel_aspect_ratio(file_path: Path, stream_data, tags) -> str:
    par = stream_data.get('display_aspect_ratio') or tags.get('display_aspect_ratio')
    if par is None:
        f"Pixel aspect ratio could not be determined for {file_path}, defaulting to 1:1"
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


def _detect_hdr(file_path: Path, stream_data) -> bool:
    # Check Color Transfer Function
    # smpte2084 is HDR10/Dolby Vision, arib-std-b67 is HLG
    transfer = stream_data.get('color_transfer', '').lower()
    hdr_transfers = {'smpte2084', 'arib-std-b67'}

    if transfer in hdr_transfers:
        log.debug(f"HDR detected via color transfer: {transfer} in {file_path}")
        return True

    # Check Color Primaries
    # bt2020 is the standard wide gamut for HDR
    primaries = stream_data.get('color_primaries', '').lower()
    if primaries == 'bt2020':
        log.debug(f"HDR detected via color primaries: {primaries} in {file_path}")
        return True

    # Check Side Data (fallback for some containers)
    # Using a flexible check for side_data_type names
    side_data_list = stream_data.get('side_data_list', [])
    for entry in side_data_list:
        data_type = entry.get('side_data_type', '')
        if any(hdr_keyword in data_type for hdr_keyword in ['Mastering display', 'Content light level', 'HDR10']):
            log.debug(f"HDR detected via side data: {data_type} in {file_path}")
            return True

    return False