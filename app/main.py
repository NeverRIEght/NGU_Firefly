import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import List

from filelock import Timeout as TimeoutException

from app import job_validator, encoder, file_utils, job_composer, json_serializer
from app.config.app_config import ConfigManager
from app.config.config_validator import ConfigValidator
from app.extractor import video_attributes_extractor, ffmpeg_metadata_extractor
from app.locking import LockManager
from app.model.encoder_job_context import EncoderJob
from app.model.json.encoding_stage import EncodingStageNamesEnum
from app.prioritization import JobPrioritizer

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
    ConfigValidator.validate(app_config)

    log.info("%s v.%s", app_config.app_name, app_config.app_version)
    log.info("Current datetime: %s", datetime.now(timezone.utc))
    log.info("Starting session...")

    try:
        with LockManager.acquire_application_lock(Path(app_config.output_dir)):
            jobs_list = job_composer.compose_jobs()

            valid_jobs = _validate_jobs(jobs_list)
            _extract_metadata(valid_jobs)

            filtered_jobs = _filter_jobs(valid_jobs)
            _prioritize_jobs(filtered_jobs)

            _execute_jobs(filtered_jobs)
    except KeyboardInterrupt:
        log.warning("Interrupted by user.")
    except TimeoutException as e:
        log.error(f"Another application instance is already running. "
                  f"Please, make sure to use different output folders for multiple instances. Error info: {e}")


def _validate_jobs(jobs_list: List[EncoderJob]) -> List[EncoderJob]:
    valid_jobs = []
    for job in jobs_list:
        is_job_valid = job_validator.validate(job)
        if is_job_valid:
            valid_jobs.append(job)
        else:
            log.error("Job is not valid: %s. Skipping...", job.source_file_path)

    return valid_jobs


def _extract_metadata(jobs_list: List[EncoderJob]):
    for job in jobs_list:
        if job.job_data.encoding_stage.stage_name == EncodingStageNamesEnum.PREPARED:
            try:
                log.debug(f"Extracting metadata for: {job.source_file_path.name}")
                job.job_data.source_video.video_attributes = video_attributes_extractor.extract(
                        job.source_file_path)
                job.job_data.source_video.ffmpeg_metadata = ffmpeg_metadata_extractor.extract(
                        job.source_file_path)

                job.job_data.encoding_stage.stage_number_from_1 = 2
                job.job_data.encoding_stage.stage_name = EncodingStageNamesEnum.METADATA_EXTRACTED
                json_serializer.serialize_to_json(job.job_data, job.metadata_json_file_path)
            except Exception as e:
                log.error(f"Failed to extract metadata for {job.source_file_path}: {e}")
                job.job_data.encoding_stage.stage_number_from_1 = -1
                job.job_data.encoding_stage.stage_name = EncodingStageNamesEnum.FAILED
                try:
                    json_serializer.serialize_to_json(job.job_data, job.metadata_json_file_path)
                except Exception:
                    pass


def _filter_jobs(jobs_list: List[EncoderJob]) -> List[EncoderJob]:
    filtered_jobs = []
    for job in jobs_list:
        if job.job_data.encoding_stage.stage_name == EncodingStageNamesEnum.METADATA_EXTRACTED:
            source_file_name = job.job_data.source_video.file_attributes.file_name
            hdr_types = job.job_data.source_video.ffmpeg_metadata.hdr_types

            if not hdr_types or len(hdr_types) == 0:
                filtered_jobs.append(job)
            elif hdr_types and len(hdr_types) > 0:
                log.info("HDR detected: %s. Skipping, HDR is not supported.", source_file_name)
                job.job_data.encoding_stage.stage_number_from_1 = -4
                job.job_data.encoding_stage.stage_name = EncodingStageNamesEnum.SKIPPED_IS_HDR_VIDEO
                json_serializer.serialize_to_json(job.job_data, job.metadata_json_file_path)
                _use_initial_file_as_output(job)
        else:
            filtered_jobs.append(job)
            
    return filtered_jobs


def _prioritize_jobs(jobs_list: List[EncoderJob]):
    prioritizer = JobPrioritizer.get_instance()
    prioritizer.prioritize(jobs_list)

    # Sort jobs by priority (descending)
    jobs_list.sort(key=lambda x: x.priority, reverse=True)


def _execute_jobs(jobs_list: List[EncoderJob]):
    processed_jobs_count = 0
    total_jobs = len(jobs_list)

    for job in jobs_list:
        job_start_time = time.perf_counter()
        was_job_already_processed: bool = False

        if (job.job_data.encoding_stage.stage_name == EncodingStageNamesEnum.CRF_FOUND
                or job.job_data.encoding_stage.stage_name == EncodingStageNamesEnum.COMPLETED):
            was_job_already_processed = True

        is_error: bool = job.job_data.encoding_stage.stage_number_from_1 < 0
        if is_error:
            _handle_job_error(job, jobs_list, processed_jobs_count, job_start_time)
            processed_jobs_count += 1
            continue

        if job.job_data.encoding_stage.stage_name in {EncodingStageNamesEnum.METADATA_EXTRACTED,
                                                      EncodingStageNamesEnum.SEARCHING_CRF}:
            encoder.encode_job(job)

        job_end_time = time.perf_counter()
        job_duration_seconds = job_end_time - job_start_time

        if not was_job_already_processed:
            job.job_data.encoding_stage.job_total_time_seconds = job_duration_seconds
            json_serializer.serialize_to_json(job.job_data, job.metadata_json_file_path)

        current_stage_num = job.job_data.encoding_stage.stage_number_from_1
        if current_stage_num >= 0:
            if job.job_data.encoding_stage.stage_name != EncodingStageNamesEnum.COMPLETED:
                job.job_data.encoding_stage.stage_number_from_1 = 5
                job.job_data.encoding_stage.stage_name = EncodingStageNamesEnum.COMPLETED
            json_serializer.serialize_to_json(job.job_data, job.metadata_json_file_path)

        # Perform cleanup for newly completed jobs
        if (job.job_data.encoding_stage.stage_name == EncodingStageNamesEnum.CRF_FOUND
                or job.job_data.encoding_stage.stage_name == EncodingStageNamesEnum.COMPLETED):
             _perform_job_cleanup(job)

        processed_jobs_count += 1
        _log_job_finished(job, processed_jobs_count, total_jobs, job_start_time)


def _handle_job_error(job: EncoderJob, jobs_list: List[EncoderJob], processed_count: int, start_time: float):
    log.info("Job finished with an error.")
    log.info("|-Source video: %s", job.source_file_path)
    log.info("|-Error name: %s", job.job_data.encoding_stage.stage_name)
    log.info("|-Error code: %s", job.job_data.encoding_stage.stage_number_from_1)

    safe_error_codes = {EncodingStageNamesEnum.STOPPED_VMAF_DELTA,
                        EncodingStageNamesEnum.UNREACHABLE_VMAF}

    if job.job_data.encoding_stage.stage_name in safe_error_codes:
        log.info("|-Error is safe.")
        _perform_job_cleanup(job)

    _log_job_finished(job, processed_count + 1, len(jobs_list), start_time)


def _log_job_finished(job: EncoderJob, processed_count: int, total_count: int, start_time: float):
    duration = time.perf_counter() - start_time
    log.info("Job finished.")
    log.info("|-Source video: %s", job.source_file_path)
    log.info("|-Total time processing: %.2f seconds", duration)
    log.info("|-Processed jobs: %d/%d", processed_count, total_count)


def _perform_job_cleanup(job: EncoderJob):
    log.info("|-Performing cleanup...")
    deleted_files_count = _remove_all_non_final_iteration_files(job)
    if deleted_files_count >= len(job.job_data.iterations):
        log.warning(
                "|-None of the iteration files were of acceptable quality. Will use the original file as output.")
        _use_initial_file_as_output(job)
    json_serializer.serialize_to_json(job.job_data, job.metadata_json_file_path)


def _remove_all_non_final_iteration_files(job: EncoderJob) -> int:
    app_config = ConfigManager.get_config()

    deleted_files_count = 0
    for iteration in job.job_data.iterations:
        vmaf_percent = iteration.execution_data.source_to_encoded_vmaf_percent
        if vmaf_percent < app_config.vmaf_min or vmaf_percent > app_config.vmaf_max:
            output_file_path = Path(app_config.output_dir) / iteration.file_attributes.file_name
            if output_file_path.exists():
                log.info("|-Deleting non-final iteration file: %s", output_file_path)
                file_utils.delete_file(output_file_path)
                deleted_files_count += 1

    return deleted_files_count


def _use_initial_file_as_output(job: EncoderJob):
    app_config = ConfigManager.get_config()
    input_file_name = file_utils.get_file_name_with_extension(job.source_file_path)
    output_file_path = Path(app_config.output_dir) / input_file_name
    file_utils.copy_file(job.source_file_path, output_file_path)
    log.info("|-Will use the original file as output.")


if __name__ == "__main__":
    main()
