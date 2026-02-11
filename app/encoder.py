import logging
import re

from filelock import Timeout as TimeoutException

from app import file_utils, json_serializer
from app.config.app_config import ConfigManager
from app.extractor import video_attributes_extractor, ffmpeg_metadata_extractor, environment_extractor
from app.locking import LockManager, LockMode
from app.model.encoder_job_context import EncoderJob
from app.model.json.encoder_settings import EncoderSettings
from app.model.json.encoding_stage import EncodingStageNamesEnum, EncodingStage
from app.model.json.execution_data import ExecutionData
from app.model.json.file_attributes import FileAttributes
from app.model.json.iteration import Iteration
from app.model.json.video_embedded_metadata import VideoEmbeddedMetadata
from app.os_resources import os_resources_utils
from app.os_resources.exceptions import LowResourcesException
from app.os_resources.os_resources_utils import offload_if_memory_low
from app.vmaf_comparator import calculate_vmaf

log = logging.getLogger(__name__)

import subprocess
import time
from app import hashing_service
import shlex
from pathlib import Path
from datetime import datetime, timezone
import numpy as np


def encode_job(job: EncoderJob):
    app_config = ConfigManager.get_config()

    try:
        with (LockManager.acquire_job_lock(Path(job.source_file_path), Path(app_config.output_dir))):
            log.info("Starting encoding job.")
            log.info("|-Source file: %s", job.source_file_path)

            vmaf_target_min = app_config.vmaf_min
            vmaf_target_max = app_config.vmaf_max

            while True:
                stage = job.job_data.encoding_stage

                if stage.crf_range_min > stage.crf_range_max:
                    log.warning(f"CRF bounds are broken. Ending search.")
                    log.warning(f"|-Stage bounds: %s-%s", stage.crf_range_min, stage.crf_range_max)
                    log.warning(f"|-Last tested CRF: %s", stage.last_crf)
                    job.job_data.encoding_stage = EncodingStage(
                        stage_number_from_1=-3,
                        stage_name=EncodingStageNamesEnum.UNREACHABLE_VMAF,
                        crf_range_min=stage.crf_range_min,
                        crf_range_max=stage.crf_range_max,
                        last_vmaf=stage.last_vmaf,
                        last_crf=stage.last_crf
                    )
                    json_serializer.serialize_to_json(job.job_data, job.metadata_json_file_path)
                    break

                crf_to_test = _predict_next_crf(job)

                if not _is_crf_prediction_valid(job, crf_to_test):
                    job.job_data.encoding_stage = EncodingStage(
                        stage_number_from_1=-3,
                        stage_name=EncodingStageNamesEnum.UNREACHABLE_VMAF,
                        crf_range_min=stage.crf_range_min,
                        crf_range_max=stage.crf_range_max,
                        last_vmaf=stage.last_vmaf,
                        last_crf=stage.last_crf
                    )
                    json_serializer.serialize_to_json(job.job_data, job.metadata_json_file_path)
                    break

                log.info("Starting iteration.")
                log.info("|-Source file: %s", job.source_file_path)
                log.info("|-CRF search range: %d-%d", stage.crf_range_min, stage.crf_range_max)
                log.info("|-CRF to test: %d", crf_to_test)

                iteration = _encode_iteration(job_context=job, crf=crf_to_test)
                current_vmaf = iteration.execution_data.source_to_encoded_vmaf_percent

                iteration.execution_data.iteration_time_seconds = (iteration.execution_data.encoding_time_seconds +
                                                                   iteration.execution_data.calculating_vmaf_time_seconds)

                if vmaf_target_min <= current_vmaf <= vmaf_target_max:
                    log.info("CRF search successful. Ending search.")
                    log.info(f"|-Best CRF: {crf_to_test}")
                    log.info(f"|-VMAF: {current_vmaf}%")

                    job.job_data.encoding_stage = EncodingStage(
                        stage_number_from_1=4,
                        stage_name=EncodingStageNamesEnum.CRF_FOUND,
                        crf_range_min=crf_to_test,
                        crf_range_max=crf_to_test,
                        last_vmaf=current_vmaf,
                        last_crf=crf_to_test
                    )
                    json_serializer.serialize_to_json(job.job_data, job.metadata_json_file_path)
                    break

                if not _is_encoding_efficient(job, current_vmaf, crf_to_test):
                    best_iteration = min(
                        job.job_data.iterations,
                        key=lambda i: abs(i.execution_data.source_to_encoded_vmaf_percent - app_config.vmaf_min)
                    )

                    job.job_data.encoding_stage = EncodingStage(
                        stage_number_from_1=-2,
                        stage_name=EncodingStageNamesEnum.STOPPED_VMAF_DELTA,
                        crf_range_min=stage.crf_range_min,
                        crf_range_max=stage.crf_range_max,
                        last_vmaf=best_iteration.execution_data.source_to_encoded_vmaf_percent,
                        last_crf=best_iteration.encoder_settings.crf
                    )
                    json_serializer.serialize_to_json(job.job_data, job.metadata_json_file_path)
                    break

                if current_vmaf > vmaf_target_max:
                    # Quality too high, need more compression -> increase CRF
                    log.info(f"VMAF {current_vmaf}% is above target max {vmaf_target_max}%, increasing CRF.")
                    stage.crf_range_min = crf_to_test + 1
                else:
                    # Quality too low, need less compression -> decrease CRF
                    log.info(f"VMAF {current_vmaf}% is below target min {vmaf_target_min}%, decreasing CRF.")
                    stage.crf_range_max = crf_to_test - 1

                job.job_data.encoding_stage = EncodingStage(
                    stage_number_from_1=3,
                    stage_name=EncodingStageNamesEnum.SEARCHING_CRF,
                    crf_range_min=stage.crf_range_min,
                    crf_range_max=stage.crf_range_max,
                    last_vmaf=current_vmaf,
                    last_crf=crf_to_test
                )
                json_serializer.serialize_to_json(job.job_data, job.metadata_json_file_path)

            log.info(f"Encoder: completed {job.source_file_path}")
    except TimeoutException as e:
        log.error(f"Video is already being processed: {e}")


def _is_encoding_efficient(job: EncoderJob, current_vmaf: float, crf_to_test: int) -> bool:
    app_config = ConfigManager.get_config()
    efficiency_threshold = app_config.efficiency_threshold
    stage = job.job_data.encoding_stage

    if stage.last_vmaf is not None and stage.last_crf is not None:
        vmaf_delta = abs(current_vmaf - stage.last_vmaf)
        crf_delta = abs(crf_to_test - stage.last_crf)

        if crf_delta > 0:
            vmaf_per_crf = vmaf_delta / crf_delta

            if vmaf_per_crf < efficiency_threshold:
                log.warning("Low encoding efficiency. Skipping file.")
                log.warning("|-VMAF delta: %.4f", vmaf_delta)
                log.warning("|-CRF delta: %.4f", crf_delta)
                log.warning("|-VMAF/CRF: %.4f", vmaf_per_crf)
                log.warning("|-Efficiency threshold: %.4f", efficiency_threshold)
                log.warning("|-Last VMAF: %.4f", stage.last_vmaf)
                log.warning("|-Last CRF: %d", stage.last_crf)
                return False
    return True


def _is_crf_prediction_valid(job: EncoderJob, predicted_crf: int) -> bool:
    stage = job.job_data.encoding_stage
    if (predicted_crf < job.job_data.encoding_stage.crf_range_min
            or predicted_crf > job.job_data.encoding_stage.crf_range_max):
        log.warning(f"Predicted CRF is out of bounds. Ending search.")
        log.warning(f"|-Stage bounds: %s-%s", stage.crf_range_min, stage.crf_range_max)
        log.warning(f"|-Predicted CRF: %s", predicted_crf)
        return False

    return True


def _encode_iteration(job_context: EncoderJob, crf: int) -> Iteration:
    log.info("Encoding iteration...")
    log.info("|-Source file: %s", job_context.source_file_path)
    log.info("|-CRF: %d", crf)

    app_config = ConfigManager.get_config()

    threads_count = environment_extractor.get_available_cpu_threads()
    input_file_path = job_context.source_file_path
    output_file_path = _generate_output_file_path(input_file_path, crf)
    log.info("|-Output file: %s", output_file_path)
    log.info("|-Using threads: %d", threads_count)

    file_utils.delete_file_with_lock(output_file_path)

    encoding_command = _compose_encoding_command(job_context=job_context,
                                                 crf=crf,
                                                 threads_count=threads_count,
                                                 output_file_path=output_file_path)

    encoding_duration_seconds = 0.0

    while True:
        attempt_start = time.perf_counter()
        file_utils.delete_file_with_lock(output_file_path)

        try:
            _encode_libx265(job_context=job_context,
                            command=encoding_command,
                            output_file_path=output_file_path)
            attempt_end = time.perf_counter()
            encoding_duration_seconds += (attempt_end - attempt_start)
            break  # encoding succeeded, exit the loop

        except LowResourcesException:
            attempt_end = time.perf_counter()
            encoding_duration_seconds += (attempt_end - attempt_start)
            log.warning("Encoding stopped due to low resources. Sleeping for %d seconds...",
                        app_config.low_resources_restart_delay_seconds)
            time.sleep(app_config.low_resources_restart_delay_seconds)
            log.info("Retrying to encode iteration...")

    encoding_finished_time = datetime.now(timezone.utc)

    if not file_utils.check_file_exists(output_file_path):
        log.error(f"Encoding failed, output file not found: {output_file_path}")
        raise EncodingError("Encoding failed, output file not found.")

    readable_command = shlex.join(encoding_command)

    source_video_attributes = job_context.job_data.source_video.video_attributes

    cpu_threads_for_vmaf = environment_extractor.get_available_cpu_threads()

    log.info("Encoding finished.")
    log.info("Calculating VMAF...")
    log.info("|-Source file: %s", job_context.source_file_path)
    log.info("|-Encoded file: %s", output_file_path)

    vmaf_calculation_duration_seconds = 0.0

    while True:
        attempt_start = time.perf_counter()
        try:
            vmaf_value = calculate_vmaf(input_file_path,
                                        output_file_path,
                                        source_video_attributes,
                                        cpu_threads_for_vmaf)
            attempt_end = time.perf_counter()
            vmaf_calculation_duration_seconds += (attempt_end - attempt_start)
            break  # calculation succeeded, exit the loop

        except LowResourcesException:
            attempt_end = time.perf_counter()
            vmaf_calculation_duration_seconds += (attempt_end - attempt_start)
            log.warning("VMAF calculation stopped due to low resources. Sleeping for %d seconds...",
                        app_config.low_resources_restart_delay_seconds)
            time.sleep(app_config.low_resources_restart_delay_seconds)
            log.info("Retrying to calculate VMAF...")

    iteration = Iteration(
        file_attributes=FileAttributes(
            file_name=output_file_path.name,
                file_size_bytes=file_utils.get_file_size_bytes(output_file_path),
        ),
        sha256_hash=hashing_service.calculate_sha256_hash(output_file_path),
        video_attributes=video_attributes_extractor.extract(output_file_path),
        encoder_settings=EncoderSettings(
            encoder="libx265",
            preset=app_config.encoder_preset,
            crf=crf,
            cpu_threads_to_use=threads_count
        ),
        execution_data=ExecutionData(
            ffmpeg_command_used=readable_command,
            source_to_encoded_vmaf_percent=vmaf_value,
            encoding_finished_datetime=encoding_finished_time.isoformat(),
            encoding_time_seconds=encoding_duration_seconds,
            calculating_vmaf_time_seconds=vmaf_calculation_duration_seconds,
            vmaf_cpu_threads_used=cpu_threads_for_vmaf
        ),
        environment=environment_extractor.extract(),
        ffmpeg_metadata=ffmpeg_metadata_extractor.extract(output_file_path)
    )

    job_context.job_data.iterations.append(iteration)
    _write_embedded_metadata(output_file_path, VideoEmbeddedMetadata.from_job(job=job_context, iteration=iteration))

    log.info("Iteration encoded.")
    log.info("|-Source file: %s", job_context.source_file_path)
    log.info("|-CRF: %d", crf)

    return iteration


def _predict_next_crf(job: EncoderJob) -> int:
    app_config = ConfigManager.get_config()
    stage = job.job_data.encoding_stage
    iterations = job.job_data.iterations
    target_vmaf = (app_config.vmaf_min + app_config.vmaf_max) / 2

    if stage.last_crf is None:
        return app_config.initial_crf

    if len(iterations) >= 2:
        try:
            x = np.array([i.encoder_settings.crf for i in iterations])
            y = np.array([i.execution_data.source_to_encoded_vmaf_percent for i in iterations])

            k, b = np.polyfit(x, y, 1)
            predicted = (target_vmaf - b) / k

            res = round(float(predicted))
            return max(stage.crf_range_min, min(stage.crf_range_max, res))
        except Exception as e:
            log.warning(f"Prediction failed ({e}), falling back to binary search.")

    return (stage.crf_range_min + stage.crf_range_max) // 2


def _generate_output_file_path(input_file_path: Path, crf: int) -> Path:
    app_config = ConfigManager.get_config()

    preset = app_config.encoder_preset
    output_folder_path = Path(app_config.output_dir)

    output_filename = (
        f"{file_utils.get_file_name_without_extension(input_file_path)}"
        f"_libx265_{preset}_crf_{crf}{file_utils.get_file_extension(input_file_path)}"
    )

    output_file_path = output_folder_path / output_filename
    return output_file_path


def _compose_encoding_command(job_context: EncoderJob,
                              crf: int,
                              threads_count: int,
                              output_file_path: Path) -> list[str]:
    app_config = ConfigManager.get_config()

    source_video = job_context.job_data.source_video
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
    else:
        log.warning("Source video is missing color metadata, encoding without explicit color settings.")

    x265_params = [
        f'crf={crf}',
        f'pools={threads_count}',
        'ssim-rd=1',  # better results for VMAF evaluation
        'aq-mode=3',  # better compression for complex scenes
    ]

    command = [
        'ffmpeg',
        '-i', str(job_context.source_file_path),

        '-c:v', 'libx265',
        '-x265-params', ':'.join(x265_params),
        '-preset', app_config.encoder_preset,

        '-fps_mode', 'passthrough',

        *color_arguments,

        '-tag:v', 'hvc1',

        '-c:a', 'copy',
        '-map', '0:v:0',
        '-map', '0:a?',
        '-map_metadata', '0',
        '-map_chapters', '0',
        '-movflags', '+faststart',

        str(output_file_path),

        '-progress', 'pipe:2',
        '-loglevel', 'info',
        '-hide_banner'
    ]

    return command


def _format_duration(seconds: float) -> str:
    seconds = int(seconds)
    if seconds < 0:
        return "0s"

    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60

    parts = []
    if h > 0:
        parts.append(f"{h}h")
    if m > 0:
        parts.append(f"{m}m")
    if s > 0 or not parts:
        parts.append(f"{s}s")

    return " ".join(parts)


def _encode_libx265(job_context: EncoderJob, command: list[str], output_file_path: Path) -> EncoderJob:
    app_config = ConfigManager.get_config()
    input_file_path = job_context.source_file_path

    total_duration = job_context.job_data.source_video.video_attributes.duration_seconds

    log.debug(f"Starting encode for: {input_file_path}")

    process = None
    start_real_time = time.perf_counter()
    try:
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            universal_newlines=True,
            bufsize=1
        )

        if not app_config.disable_resources_monitoring:
            os_resources_utils.set_process_priority(process, app_config.encoder_process_priority)

        time_re = re.compile(r"out_time_ms=(\d+)")

        last_ram_check_time = 0

        while True:
            line = process.stderr.readline()
            if not line and process.poll() is not None:
                break

            if line:
                if not app_config.disable_resources_monitoring:
                    current_time = time.perf_counter()
                    if current_time - last_ram_check_time >= app_config.ram_monitoring_interval_seconds:
                        offload_if_memory_low(process)
                        last_ram_check_time = current_time

                match = time_re.search(line)
                if match:
                    # Video time processed so far (in seconds)
                    current_video_time = int(match.group(1)) / 1000000
                    elapsed_real_time = time.perf_counter() - start_real_time

                    if total_duration > 0 and current_video_time > 0:
                        percent = min(100, (current_video_time / total_duration) * 100)

                        # Predict remaining time (ETA)
                        # Speed = current_video_time / elapsed_real_time
                        # Remaining_video = total_duration - current_video_time
                        # ETA = Remaining_video / Speed
                        eta_seconds = elapsed_real_time * (total_duration - current_video_time) / current_video_time

                        elapsed_str = _format_duration(elapsed_real_time)
                        eta_str = _format_duration(eta_seconds)

                        status_line = (
                            f"\rEncoding progress: {percent:.2f}% | "
                            f"Elapsed time: {elapsed_str} | "
                            f"Remaining time: ~{eta_str} | "
                            f"Video duration (encoded/total): {current_video_time:.1f}/{total_duration:.1f}s"
                        )
                        print(status_line, end="", flush=True)

        print()

        if process.returncode != 0:
            log.error(f"Error while encoding the file: '{input_file_path}'.")
            raise EncodingError("FFmpeg failed to encode the video.")

        return job_context
    except LowResourcesException:
        raise LowResourcesException("Encoding stopped due to low system resources.")
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
            file_utils.delete_file_with_lock(output_file_path)


class EncodingError(Exception):
    pass


def _write_embedded_metadata(output_file_path: Path, metadata: VideoEmbeddedMetadata):
    with LockManager.acquire_file_operation_lock(output_file_path, LockMode.EXCLUSIVE):
        temp_file = output_file_path.with_suffix(".tmp" + output_file_path.suffix)
        json_str = metadata.model_dump_json()

        cmd = [
            'ffmpeg',
            '-i', str(output_file_path),
            '-metadata', f'comment=encoder_metadata:{json_str}',
            '-c', 'copy',
            '-map_metadata', '0',
            '-movflags', '+faststart',
            str(temp_file),
            '-loglevel', 'error',
            '-y'
        ]

        backup_file = None
        try:
            subprocess.run(cmd, check=True, capture_output=True)

            backup_file = output_file_path.with_suffix(".old")

            file_utils.delete_file(backup_file)

            output_file_path.rename(backup_file)
            temp_file.rename(output_file_path)
            file_utils.delete_file(backup_file)

            log.info(f"Wrote metadata for {output_file_path}")
        except KeyboardInterrupt as e:
            log.warning("Metadata writing interrupted! Cleaning up temp files.")
            _cleanup_metadata(temp_file, backup_file, output_file_path)
            raise
        except Exception as e:
            log.error(f"Error writing embedded metadata to {output_file_path}: {e}")
            _cleanup_metadata(temp_file, backup_file, output_file_path)


def _cleanup_metadata(temp_file: Path, backup_file: Path | None, original_file: Path):
    if temp_file:
        file_utils.delete_file(temp_file)
    if backup_file and file_utils.check_file_exists(backup_file):
        if not file_utils.check_file_exists(original_file):
            backup_file.rename(original_file)
        else:
            file_utils.delete_file(backup_file)
