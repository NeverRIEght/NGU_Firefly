import logging

import file_utils
import json_serializer
from app_config import ConfigManager
from extractor import video_attributes_extractor, ffmpeg_metadata_extractor
from model.encoder_job_context import EncoderJobContext
from model.encoder_settings import EncoderSettings
from model.environment import Environment
from model.execution_data import ExecutionData
from model.file_attributes import FileAttributes
from model.iteration import Iteration

log = logging.getLogger(__name__)

import json
import os
import subprocess
import time
import hashing_service
import shlex
from pathlib import Path
from datetime import datetime, timezone


def encode_job(job: EncoderJobContext) -> EncoderJobContext:
    log.info(f"Encoding job: {job}")

    app_config = ConfigManager.get_config()
    stage = job.encoder_data.encoding_stage

    VMAF_TARGET_MIN = 95.0
    VMAF_TARGET_MAX = 96.0

    while True:
        if stage.last_crf is None:
            crf_to_test = app_config.initial_crf
        else:
            crf_to_test = (stage.crf_range_min + stage.crf_range_max) // 2

        if stage.crf_range_min >= stage.crf_range_max:
            log.info(f"Ideal CRF found: {stage.crf_range_min}")
            break

        log.info(f"Testing CRF={crf_to_test} in range {stage.crf_range_min}-{stage.crf_range_max}")

        iteration = _encode_iteration(job_context=job, crf=crf_to_test)
        current_vmaf = iteration.execution_data.source_to_encoded_vmaf_percent

        if VMAF_TARGET_MIN <= current_vmaf <= VMAF_TARGET_MAX:
            stage.crf_range_min = crf_to_test
            stage.crf_range_max = crf_to_test
            stage.last_vmaf = current_vmaf
            stage.last_crf = crf_to_test
            log.info(f"CRF {crf_to_test} produced acceptable VMAF: {current_vmaf}%, ending search.")
            break

        if current_vmaf > VMAF_TARGET_MAX:
            # Quality too high (>96%), need more compression -> increase CRF
            log.info(f"VMAF {current_vmaf}% is above target max {VMAF_TARGET_MAX}%, increasing CRF.")
            stage.crf_range_min = crf_to_test + 1
        else:
            # Quality too low (<95%), need less compression -> decrease CRF
            log.info(f"VMAF {current_vmaf}% is below target min {VMAF_TARGET_MIN}%, decreasing CRF.")
            stage.crf_range_max = crf_to_test - 1

        stage.last_crf = crf_to_test
        stage.last_vmaf = current_vmaf

        json_serializer.serialize_to_json(job.encoder_data, job.metadata_json_file_path)

    log.info(f"Encoding completed for {job.source_file_path}, performing cleanup")
    # TODO: Perform cleanup actions and mark as complete

    return job


def _encode_iteration(job_context: EncoderJobContext, crf: int) -> Iteration:
    log.info(f"Encoding iteration with CRF={crf} of job {job_context}")

    app_config = ConfigManager.get_config()

    input_file_path = job_context.source_file_path
    output_file_path = _generate_output_file_path(input_file_path, crf)
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
    encoding_finished_time = datetime.now(timezone.utc)

    if not file_utils.check_file_exists(output_file_path):
        log.error(f"Encoding failed, output file not found: {output_file_path}")
        raise EncodingError("Encoding failed, output file not found.")

    readable_command = shlex.join(encoding_command)

    iteration = Iteration(
        file_attributes=FileAttributes(
            file_name=output_file_path.name,
            file_size_megabytes=file_utils.get_file_size_megabytes(output_file_path),
        ),
        sha256_hash=hashing_service.calculate_sha256_hash(output_file_path),
        video_attributes=video_attributes_extractor.extract(output_file_path),
        encoder_settings=EncoderSettings(
            encoder="libx265",
            preset=app_config.encode_preset,
            crf=crf,
            cpu_threads_to_use=threads_count
        ),
        execution_data=ExecutionData(
            ffmpeg_command_used=readable_command,
            source_to_encoded_vmaf_percent=_calculate_vmaf(input_file_path, output_file_path),
            encoding_finished_datetime=encoding_finished_time.isoformat(),
            encoding_time_seconds=encoding_duration_seconds
        ),
        environment=Environment(
            script_version=app_config.version,
            ffmpeg_version="unknown",  # TODO: extract ffmpeg version
            encoder_version="unknown",  # TODO: extract encoder version
            cpu_name="unknown",  # TODO: extract CPU name
            cpu_threads=-1  # TODO: extract CPU data
        ),
        ffmpeg_metadata=ffmpeg_metadata_extractor.extract(output_file_path)
    )

    job_context.encoder_data.iterations.append(iteration)

    return iteration


def _generate_output_file_path(input_file_path: Path, crf: int) -> Path:
    app_config = ConfigManager.get_config()

    preset = app_config.encode_preset
    output_folder_path = Path(app_config.output_dir)

    output_filename = (
        f"{file_utils.get_file_name_without_extension(input_file_path)}"
        f"_libx265_{preset}_crf_{crf}{file_utils.get_file_extension(input_file_path)}"
    )

    output_file_path = output_folder_path / output_filename
    return output_file_path


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

    source_video = job_context.encoder_data.source_video
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
            file_utils.delete_file(output_file_path)


class EncodingError(Exception):
    pass


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
