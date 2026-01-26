import logging
import time
from datetime import datetime, timezone
from pathlib import Path

from filelock import Timeout as TimeoutException

import job_validator
from app import encoder
from app import file_utils
from app import hashing_service
from app import job_composer
from app import json_serializer
from app.config.app_config import ConfigManager
from app.extractor import video_attributes_extractor, ffmpeg_metadata_extractor
from app.locking import LockManager
from app.model.encoder_job_context import EncoderJobContext
from app.model.encoding_stage import EncodingStageNamesEnum
from app.model.file_attributes import FileAttributes
from app.model.source_video import SourceVideo

logs_dir = Path("../logs")
logs_dir.mkdir(exist_ok=True)

log = logging.getLogger()
log.setLevel(logging.DEBUG)

if log.hasHandlers():
    log.handlers.clear()

logs_formatter = logging.Formatter('[%(asctime)s][%(levelname)s]: %(message)s')

all_logs_handler = logging.FileHandler(logs_dir / "full.log", mode='a', encoding='utf-8')
all_logs_handler.setLevel(logging.DEBUG)
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


def main():
    app_config = ConfigManager.get_config()

    log.info("%s v.%s", app_config.app_name, app_config.app_version)
    log.info("Current datetime: %s", datetime.now(timezone.utc))
    log.info("Starting session...")

    try:
        with LockManager.acquire_application_lock(Path(app_config.output_dir)):
            jobs_list = job_composer.compose_jobs()
            processed_jobs_count = 0

            for job in jobs_list:
                is_job_valid = job_validator.validate(job)
                if not is_job_valid:
                    log.error("Job is not valid. Skipping...")
                    continue
                job_start_time = time.perf_counter()
                was_job_already_processed: bool = False

                if (job.encoder_data.encoding_stage.stage_name == EncodingStageNamesEnum.CRF_FOUND
                        or job.encoder_data.encoding_stage.stage_name == EncodingStageNamesEnum.COMPLETED):
                    was_job_already_processed = True

                is_error: bool = job.encoder_data.encoding_stage.stage_number_from_1 < 0
                if is_error:
                    log.info("Job finished with an error.")
                    log.info("|-Source video: %s", job.source_file_path)
                    log.info("|-Error name: %s", job.encoder_data.encoding_stage.stage_name)
                    log.info("|-Error code: %s", job.encoder_data.encoding_stage.stage_number_from_1)

                    safe_error_codes = {EncodingStageNamesEnum.STOPPED_VMAF_DELTA,
                                        EncodingStageNamesEnum.UNREACHABLE_VMAF}

                    if job.encoder_data.encoding_stage.stage_name in safe_error_codes:
                        log.info("|-Error is safe. Performing cleanup...")
                        _remove_all_non_final_iteration_files(job)
                        _use_initial_file_as_output(job)

                    processed_jobs_count += 1
                    log.info("Job finished.")
                    log.info("|-Source video: %s", job.source_file_path)
                    log.info("|-Total time processing: %.2f seconds", time.perf_counter() - job_start_time)
                    log.info("|-Processed jobs: %d/%d", processed_jobs_count, len(jobs_list))
                    continue

                if job.encoder_data.encoding_stage.stage_name == EncodingStageNamesEnum.PREPARED:
                    file_attributes = FileAttributes(
                        file_name=file_utils.get_file_name_with_extension(job.source_file_path),
                        file_size_megabytes=file_utils.get_file_size_megabytes(job.source_file_path),
                    )
                    job.encoder_data.source_video = SourceVideo(
                        file_attributes=file_attributes,
                        sha256_hash=hashing_service.calculate_sha256_hash(job.source_file_path),
                        video_attributes=video_attributes_extractor.extract(job.source_file_path),
                        ffmpeg_metadata=ffmpeg_metadata_extractor.extract(job.source_file_path),
                    )
                    job.encoder_data.encoding_stage.stage_number_from_1 = 2
                    job.encoder_data.encoding_stage.stage_name = EncodingStageNamesEnum.METADATA_EXTRACTED
                    json_serializer.serialize_to_json(job.encoder_data, job.metadata_json_file_path)

                if job.encoder_data.encoding_stage.stage_name == EncodingStageNamesEnum.METADATA_EXTRACTED:
                    source_file_name = job.encoder_data.source_video.file_attributes.file_name
                    hdr_types = job.encoder_data.source_video.ffmpeg_metadata.hdr_types
                    if hdr_types and len(hdr_types) > 0:
                        log.info("HDR detected: %s. Skipping, HDR is not supported.", source_file_name)
                        job.encoder_data.encoding_stage.stage_number_from_1 = -4
                        job.encoder_data.encoding_stage.stage_name = EncodingStageNamesEnum.SKIPPED_IS_HDR_VIDEO
                        json_serializer.serialize_to_json(job.encoder_data, job.metadata_json_file_path)
                        _use_initial_file_as_output(job)
                        processed_jobs_count += 1
                        log.info("Job finished.")
                        log.info("|-Source video: %s", job.source_file_path)
                        log.info("|-Total time processing: %.2f seconds", time.perf_counter() - job_start_time)
                        log.info("|-Processed jobs: %d/%d", processed_jobs_count, len(jobs_list))
                        continue

                    encoder.encode_job(job)

                if job.encoder_data.encoding_stage.stage_name == EncodingStageNamesEnum.SEARCHING_CRF:
                    encoder.encode_job(job)

                if (job.encoder_data.encoding_stage.stage_name == EncodingStageNamesEnum.CRF_FOUND
                        or job.encoder_data.encoding_stage.stage_name == EncodingStageNamesEnum.COMPLETED):
                    log.info("Job encoded. Performing cleanup...")

                    deleted_files_count = _remove_all_non_final_iteration_files(job)
                    if deleted_files_count >= len(job.encoder_data.iterations):
                        log.warning(
                            "|-None of the iteration files were of acceptable quality. Will use the original file as output.")
                        _use_initial_file_as_output(job)

                job_end_time = time.perf_counter()
                job_duration_seconds = job_end_time - job_start_time

                if not was_job_already_processed:
                    job.encoder_data.encoding_stage.job_total_time_seconds = job_duration_seconds
                    json_serializer.serialize_to_json(job.encoder_data, job.metadata_json_file_path)

                current_stage_num = job.encoder_data.encoding_stage.stage_number_from_1
                if current_stage_num >= 0:
                    if job.encoder_data.encoding_stage.stage_name != EncodingStageNamesEnum.COMPLETED:
                        job.encoder_data.encoding_stage.stage_number_from_1 = 5
                        job.encoder_data.encoding_stage.stage_name = EncodingStageNamesEnum.COMPLETED
                    json_serializer.serialize_to_json(job.encoder_data, job.metadata_json_file_path)

                processed_jobs_count += 1

                log.info("Job finished.")
                log.info("|-Source video: %s", job.source_file_path)
                log.info("|-Total time processing: %.2f seconds", job_duration_seconds)
                log.info("|-Processed jobs: %d/%d", processed_jobs_count, len(jobs_list))
    except KeyboardInterrupt:
        log.warning("Interrupted by user.")
    except TimeoutException as e:
        log.error(f"Another application instance is already running. "
                  f"Please, make sure to use different output folders for multiple instances. Error info: {e}")


def _remove_all_non_final_iteration_files(job: EncoderJobContext) -> int:
    app_config = ConfigManager.get_config()

    deleted_files_count = 0
    for iteration in job.encoder_data.iterations:
        if iteration.execution_data.source_to_encoded_vmaf_percent < app_config.vmaf_min:
            output_file_path = Path(app_config.output_dir) / iteration.file_attributes.file_name
            if output_file_path.exists():
                log.info("|-Deleting non-final iteration file: %s", output_file_path)
                file_utils.delete_file(output_file_path)
                deleted_files_count += 1

    return deleted_files_count


def _use_initial_file_as_output(job: EncoderJobContext):
    app_config = ConfigManager.get_config()
    input_file_name = file_utils.get_file_name_with_extension(job.source_file_path)
    output_file_path = Path(app_config.output_dir) / input_file_name
    file_utils.copy_file(job.source_file_path, output_file_path)
    log.info("|-Will use the original file as output.")


if __name__ == "__main__":
    main()
