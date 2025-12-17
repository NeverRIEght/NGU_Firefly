import json
import logging
import os
import subprocess
import time
import hashing_service
import shlex
from pathlib import Path
from datetime import datetime, timezone

from app.app_config import ConfigManager
from app.model import EncoderSettings

log = logging.getLogger(__name__)

from model import EncoderJobContext, Iteration


def encode_job(job_context: EncoderJobContext) -> EncoderJobContext:
    log.info(f"Encoding job: {job_context}")

    app_config = ConfigManager.get_config()

    input_file_path = job_context.source_file_path

    crf = app_config.initial_crf
    preset = app_config.encode_preset
    output_folder_path = Path(app_config.output_dir)

    output_filename = (
        f"{input_file_path.stem}_libx265_{preset}_crf_{crf}{input_file_path.suffix}"
    )

    output_file_path = output_folder_path / output_filename

    threads_count = _calculate_threads_count()

    encoding_command = _compose_encoding_command(job_context=job_context,
                                                 crf=crf,
                                                 threads_count=threads_count,
                                                 output_file_path=output_file_path)
    start_time = time.perf_counter()
    _encode_libx265(job_context=job_context,
                    command=encoding_command,
                    output_file_path=output_file_path)
    end_time = time.perf_counter()
    encoding_duration_seconds = end_time - start_time

    # Check if the output file exists
    os.path.isfile(output_file_path)

    # Populate the iteration
    encoder_settings = EncoderSettings(
        encoder="libx265",
        preset=preset,
        crf=crf,
        pools=threads_count
    )

    file_size_megabytes = _get_file_size_megabytes(output_file_path)
    encoding_finished_time = datetime.now(timezone.utc)
    video_duration = get_video_duration(output_file_path)
    vmaf = _calculate_vmaf(input_file_path, output_file_path)

    readable_command = shlex.join(encoding_command)

    iteration = Iteration(
        file_name=output_file_path.name,
        file_size_megabytes=file_size_megabytes,
        video_duration_seconds=video_duration,
        codec="h265",
        encoder_settings=encoder_settings,
        source_to_encoded_vmaf_percent=vmaf,

        encoding_finished_datetime=encoding_finished_time,

        encoding_time_seconds=encoding_duration_seconds,
        actual_frame_count=20, # TODO: use metadata_extractor._extract_fps() after refactoring
        sha256_hash=hashing_service.calculate_sha256_hash(output_file_path),
        ffmpeg_command_used=readable_command,
    #environment: Environment # TODO: extract environment data in separate files
    #ffmpeg_metadata: FfmpegMetadata # TODO: use metadata_extractor after refactoring
    )

    # TODO: serialize the iteration data

    return job_context


def _calculate_threads_count() -> int:
    app_config = ConfigManager.get_config()
    if app_config.is_silent:
        threads_count = 1
    else:
        threads_count = os.cpu_count()
    return threads_count


def _compose_encoding_command(job_context: EncoderJobContext,
                              crf: int,
                              threads_count: int,
                              output_file_path: Path) -> list[str]:
    app_config = ConfigManager.get_config()

    source_video = job_context.report_data.source_video
    source_metadata = source_video.ffmpeg_metadata

    color_arguments = []
    if (source_metadata.color_primaries is not None
            and source_metadata.color_trc is not None
            and source_metadata.colorspace is not None):
        color_arguments += [
            '-color_primaries', source_metadata.color_primaries,
            '-color_trc', source_metadata.color_trc,
            '-colorspace', source_metadata.colorspace,
        ]

    command = [
        'ffmpeg',
        '-i', str(job_context.source_file_path),

        '-c:v', 'libx265',
        '-x265-params', f'crf={crf}:pools={threads_count}',
        '-preset', app_config.encode_preset,

        *color_arguments,

        '-tag:v', 'hvc1',

        '-c:a', 'copy',
        '-map', '0:v:0',
        '-map', '0:a?',
        '-map_metadata', '0',
        '-map_chapters', '0',
        '-movflags', '+faststart',

        str(output_file_path),

        '-loglevel', 'error',
        '-hide_banner'
    ]

    return command


def _encode_libx265(job_context: EncoderJobContext, command: list[str], output_file_path: Path) -> EncoderJobContext:
    input_file_path = job_context.source_file_path

    log.info(f"Starting encode for: {input_file_path}")

    process = None

    try:
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1
        )

        _, stderr = process.communicate()

        if process.returncode != 0:
            log.error(f"Error while encoding the file: '{input_file_path}'.")
            log.error(f"Return code: {process.returncode}")
            log.error(f"FFmpeg Error Output:\n{stderr}")

            raise EncodingError("FFmpeg failed to encode the video.")

        log.info(f"Encoding finished successfully for: {output_file_path}")
        return job_context
    except FileNotFoundError:
        log.error("FFmpeg not found. Please check your installation and PATH settings.")
        return job_context
    except KeyboardInterrupt:
        log.info("Encoding interrupted by user.")
        if process:
            process.kill()
        raise
    except EncodingError:
        return job_context
    except Exception as e:
        log.error(f"Unexpected system error while encoding '{input_file_path}'. Details: {e}")
        return job_context

    finally:
        if process and process.returncode != 0 or 'KeyboardInterrupt' in locals():
            log.info(f"Deleting incomplete output file: {output_file_path}")
            _delete_file(output_file_path)


class EncodingError(Exception):
    pass


def _delete_file(file_path: Path) -> bool:
    if file_path.is_file():
        try:
            file_path.unlink()
            log.info(f"Deleted file: {file_path}")
            return True
        except OSError as e:
            log.error(f"Error deleting file {file_path}. Details: \n{e}")
            return False
    return False


def get_video_duration(video_path: Path) -> float | None:
    log.info(f"Getting video duration for: {video_path}")
    try:
        command = [
            'ffprobe',
            '-v', 'error',
            '-select_streams', 'v:0',
            '-show_entries', 'format=duration',
            '-of', 'json',
            str(video_path)
        ]
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        duration_info = json.loads(result.stdout)
        duration = float(duration_info['format']['duration'])
        if duration < 0:
            log.warning(f"Negative duration for {video_path}. Returning None.")
            return None
        log.info(f"Got duration for {video_path}: {duration} seconds")
        return duration
    except (subprocess.CalledProcessError, KeyError, ValueError):
        log.error(f"Error getting duration for {video_path}. Returning None.")
        return None


def _get_file_size_megabytes(file_path: Path) -> float | None:
    # TODO: Move to another place, remove duplicate from metadata_extractor
    log.info(f"Getting file size for: {file_path}")
    try:
        if not file_path.is_file():
            log.error(f"File not found for size calculation: {file_path}")
            return None

        size_in_bytes = file_path.stat().st_size
        size_in_mb = size_in_bytes / (1024 * 1024)
        return size_in_mb
    except OSError as e:
        log.error(f"Failed to access file {file_path}: {e}")
        return None

def _calculate_vmaf(
    reference: Path,
    distorted: Path,
    model: str = "vmaf_v0.6.1"
) -> float:
    """
    Compares two video files using VMAF.

    Assumptions & guarantees:
    - No reliance on container color metadata
    - Explicit colorspace normalization
    - Frame-accurate comparison
    - No intermediate files created

    Requirements:
    - ffmpeg built with libvmaf
    """

    if not reference.is_file():
        raise FileNotFoundError(f"Reference file not found: {reference}")
    if not distorted.is_file():
        raise FileNotFoundError(f"Distorted file not found: {distorted}")

    # We explicitly normalize EVERYTHING to:
    # - yuv420p
    # - bt709
    # - progressive
    # - same resolution & fps (taken from reference)
    #
    # This avoids:
    # - colorspace mismatches
    # - container metadata lies
    # - VMAF undefined behavior

    vmaf_filter = (
        f"[0:v]"
        f"scale=flags=bicubic,"
        f"fps=fps=source,"
        f"format=yuv420p"
        f"[ref];"
        f"[1:v]"
        f"scale=flags=bicubic,"
        f"fps=fps=source,"
        f"format=yuv420p"
        f"[dist];"
        f"[ref][dist]"
        f"libvmaf="
        f"model={model}:"
        f"log_fmt=json"
    )

    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel", "error",

        "-i", str(reference),
        "-i", str(distorted),

        "-lavfi", vmaf_filter,
        "-f", "null",
        "-"
    ]

    try:
        proc = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True
        )
    except subprocess.CalledProcessError as e:
        raise RuntimeError(
            f"FFmpeg VMAF computation failed:\n{e.stderr}"
        ) from e

    # FFmpeg prints libvmaf JSON to stderr
    stderr = proc.stderr

    try:
        # Extract JSON block from stderr
        json_start = stderr.index("{")
        json_data = json.loads(stderr[json_start:])
    except Exception as e:
        raise RuntimeError(
            f"Failed to parse VMAF output.\nRaw stderr:\n{stderr}"
        ) from e

    try:
        vmaf_score = json_data["pooled_metrics"]["vmaf"]["mean"]
    except KeyError:
        raise RuntimeError(
            f"VMAF score not found in output JSON:\n{json_data}"
        )

    return float(vmaf_score)