import logging

from app import file_utils
from app import json_serializer
from app.app_config import ConfigManager
from app.extractor import video_attributes_extractor, ffmpeg_metadata_extractor, environment_extractor
from app.model.encoder_job_context import EncoderJobContext
from app.model.encoder_settings import EncoderSettings
from app.model.encoding_stage import EncodingStageNamesEnum, EncodingStage
from app.model.environment import Environment
from app.model.execution_data import ExecutionData
from app.model.file_attributes import FileAttributes
from app.model.iteration import Iteration
from app.vmaf_comparator import calculate_vmaf

log = logging.getLogger(__name__)

import os
import subprocess
import time
from app import hashing_service
import shlex
from pathlib import Path
from datetime import datetime, timezone
import numpy as np


def encode_job(job: EncoderJobContext) -> EncoderJobContext:
    log.info(f"Encoding job: {job}")

    app_config = ConfigManager.get_config()

    vmaf_target_min = app_config.vmaf_min
    vmaf_target_max = app_config.vmaf_max
    efficiency_threshold = app_config.efficiency_threshold

    while True:
        stage = job.encoder_data.encoding_stage
        iteration_start_time = time.perf_counter()

        if stage.crf_range_min > stage.crf_range_max:
            log.warning(f"CRF bounds are broken. Ending search.")
            log.warning(f"|-Stage bounds: %s-%s", stage.crf_range_min, stage.crf_range_max)
            log.warning(f"|-Last tested CRF: %s", stage.last_crf)
            job.encoder_data.encoding_stage = EncodingStage(
                stage_number_from_1=-3,
                stage_name=EncodingStageNamesEnum.UNREACHABLE_VMAF,
                crf_range_min=stage.crf_range_min,
                crf_range_max=stage.crf_range_max,
                last_vmaf=stage.last_vmaf,
                last_crf=stage.last_crf
            )
            json_serializer.serialize_to_json(job.encoder_data, job.metadata_json_file_path)
            break

        crf_to_test = _predict_next_crf(job, (vmaf_target_min + vmaf_target_max) / 2)

        if (crf_to_test < job.encoder_data.encoding_stage.crf_range_min
                or crf_to_test > job.encoder_data.encoding_stage.crf_range_max):
            log.warning(f"Predicted CRF is out of bounds. Ending search.")
            log.warning(f"|-Stage bounds: %s-%s", stage.crf_range_min, stage.crf_range_max)
            log.warning(f"|-Predicted CRF: %s", crf_to_test)
            job.encoder_data.encoding_stage = EncodingStage(
                stage_number_from_1=-3,
                stage_name=EncodingStageNamesEnum.UNREACHABLE_VMAF,
                crf_range_min=stage.crf_range_min,
                crf_range_max=stage.crf_range_max,
                last_vmaf=stage.last_vmaf,
                last_crf=stage.last_crf
            )
            json_serializer.serialize_to_json(job.encoder_data, job.metadata_json_file_path)
            break

        log.info(f"Testing CRF={crf_to_test} in range {stage.crf_range_min}-{stage.crf_range_max}")

        iteration = _encode_iteration(job_context=job, crf=crf_to_test)
        current_vmaf = iteration.execution_data.source_to_encoded_vmaf_percent

        iteration_end_time = time.perf_counter()
        iteration_duration_seconds = iteration_end_time - iteration_start_time
        iteration.execution_data.iteration_time_seconds = iteration_duration_seconds

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

                    best_iteration = min(
                        job.encoder_data.iterations,
                        key=lambda i: abs(i.execution_data.source_to_encoded_vmaf_percent - vmaf_target_min)
                    )

                    job.encoder_data.encoding_stage = EncodingStage(
                        stage_number_from_1=-2,
                        stage_name=EncodingStageNamesEnum.STOPPED_VMAF_DELTA,
                        crf_range_min=best_iteration.encoder_settings.crf,
                        crf_range_max=best_iteration.encoder_settings.crf,
                        last_vmaf=best_iteration.execution_data.source_to_encoded_vmaf_percent,
                        last_crf=best_iteration.encoder_settings.crf
                    )
                    json_serializer.serialize_to_json(job.encoder_data, job.metadata_json_file_path)
                    break

        if vmaf_target_min <= current_vmaf <= vmaf_target_max:
            log.info(f"CRF {crf_to_test} produced acceptable VMAF: {current_vmaf}%, ending search.")

            job.encoder_data.encoding_stage = EncodingStage(
                stage_number_from_1=4,
                stage_name=EncodingStageNamesEnum.CRF_FOUND,
                crf_range_min=crf_to_test,
                crf_range_max=crf_to_test,
                last_vmaf=current_vmaf,
                last_crf=crf_to_test
            )
            json_serializer.serialize_to_json(job.encoder_data, job.metadata_json_file_path)
            break

        if current_vmaf > vmaf_target_max:
            # Quality too high, need more compression -> increase CRF
            log.info(f"VMAF {current_vmaf}% is above target max {vmaf_target_max}%, increasing CRF.")
            stage.crf_range_min = crf_to_test + 1
        else:
            # Quality too low, need less compression -> decrease CRF
            log.info(f"VMAF {current_vmaf}% is below target min {vmaf_target_min}%, decreasing CRF.")
            stage.crf_range_max = crf_to_test - 1

        job.encoder_data.encoding_stage = EncodingStage(
            stage_number_from_1=3,
            stage_name=EncodingStageNamesEnum.SEARCHING_CRF,
            crf_range_min=stage.crf_range_min,
            crf_range_max=stage.crf_range_max,
            last_vmaf=current_vmaf,
            last_crf=crf_to_test
        )
        json_serializer.serialize_to_json(job.encoder_data, job.metadata_json_file_path)

    log.info(f"Encoder: completed {job.source_file_path}")
    return job


def _encode_iteration(job_context: EncoderJobContext, crf: int) -> Iteration:
    log.info("Encoding iteration...")
    log.info("|-Source file: %s", job_context.source_file_path)
    log.info("|-CRF: %d", crf)

    app_config = ConfigManager.get_config()

    threads_count = _calculate_threads_count()
    input_file_path = job_context.source_file_path
    output_file_path = _generate_output_file_path(input_file_path, crf)
    log.info("|-Output file: %s", output_file_path)

    if file_utils.check_file_exists(output_file_path):
        file_utils.delete_file(output_file_path)

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

    source_video_attributes = job_context.encoder_data.source_video.video_attributes

    vmaf_calculation_start_time = time.perf_counter()
    vmaf_value = calculate_vmaf(input_file_path, output_file_path, source_video_attributes)
    vmaf_calculation_end_time = time.perf_counter()
    vmaf_calculation_duration_seconds = vmaf_calculation_end_time - vmaf_calculation_start_time

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
            source_to_encoded_vmaf_percent=vmaf_value,
            encoding_finished_datetime=encoding_finished_time.isoformat(),
            encoding_time_seconds=encoding_duration_seconds,
            calculating_vmaf_time_seconds=vmaf_calculation_duration_seconds,
        ),
        environment=environment_extractor.extract(),
        ffmpeg_metadata=ffmpeg_metadata_extractor.extract(output_file_path)
    )

    job_context.encoder_data.iterations.append(iteration)

    log.info("Iteration encoded.")
    log.info("|-Source file: %s", job_context.source_file_path)
    log.info("|-CRF: %d", crf)

    return iteration


def _predict_next_crf(job: EncoderJobContext, target_vmaf: float) -> int:
    app_config = ConfigManager.get_config()
    stage = job.encoder_data.encoding_stage
    iterations = job.encoder_data.iterations

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
    else:
        log.warning("Source video is missing color metadata, encoding without explicit color settings.")

    command = [
        'ffmpeg',
        '-i', str(job_context.source_file_path),

        '-c:v', 'libx265',
        '-x265-params', f'crf={crf}:pools={threads_count}',
        '-preset', app_config.encode_preset,

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

        '-loglevel', 'error',
        '-hide_banner'
    ]

    return command


def _encode_libx265(job_context: EncoderJobContext, command: list[str], output_file_path: Path) -> EncoderJobContext:
    input_file_path = job_context.source_file_path

    log.debug(f"Starting encode for: {input_file_path}")

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

        log.debug(f"Encoding finished successfully for: {output_file_path}")
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
