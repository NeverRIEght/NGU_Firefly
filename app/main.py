import logging
import time
from pathlib import Path

from app import encoder
from app import file_utils
from app import hashing_service
from app import job_composer
from app import json_serializer
from app.app_config import ConfigManager
from app.extractor import video_attributes_extractor, ffmpeg_metadata_extractor
from app.model.encoding_stage import EncodingStageNamesEnum
from app.model.file_attributes import FileAttributes
from app.model.source_video import SourceVideo
from datetime import datetime, timezone

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


def main():
    app_config = ConfigManager.get_config()

    log.info("Video Encoder v.%s", app_config.version)
    log.info("Current datetime: %s", datetime.now(timezone.utc))
    log.info("Starting session...")

    jobs_list = job_composer.compose_jobs()
    processed_jobs_count = 0
    for job in jobs_list:
        job_start_time = time.perf_counter()
        was_job_already_processed: bool = False

        if (job.encoder_data.encoding_stage.stage_name == EncodingStageNamesEnum.CRF_FOUND
                or job.encoder_data.encoding_stage.stage_name == EncodingStageNamesEnum.COMPLETED):
            processed_jobs_count += 1
            was_job_already_processed = True

        is_error: bool = job.encoder_data.encoding_stage.stage_number_from_1 < 0
        if is_error:
            log.info("Job finished with an error.")
            log.info("|-Source video: %s", job.source_file_path)
            log.info("|-Error name: %s", job.encoder_data.encoding_stage.stage_name)
            log.info("|-Error code: %s", job.encoder_data.encoding_stage.stage_number_from_1)
            processed_jobs_count += 1
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
            encoder.encode_job(job)

        if job.encoder_data.encoding_stage.stage_name == EncodingStageNamesEnum.SEARCHING_CRF:
            encoder.encode_job(job)

        if (job.encoder_data.encoding_stage.stage_name == EncodingStageNamesEnum.CRF_FOUND
                or job.encoder_data.encoding_stage.stage_name == EncodingStageNamesEnum.COMPLETED):
            log.info("Job encoded. Performing cleanup...")

            deleted_files_count = 0
            for iteration in job.encoder_data.iterations:
                if iteration.execution_data.source_to_encoded_vmaf_percent < app_config.vmaf_min:
                    output_file_path = Path(app_config.output_dir) / iteration.file_attributes.file_name
                    if output_file_path.exists():
                        log.info("|-Deleting non-final iteration file: %s", output_file_path)
                        file_utils.delete_file(output_file_path)
                        deleted_files_count += 1
            if deleted_files_count >= len(job.encoder_data.iterations):
                log.warning("|-None of the iteration files were of acceptable quality. Will use the original file.")
                input_file_name = file_utils.get_file_name_with_extension(job.source_file_path)
                output_file_path = Path(app_config.output_dir) / input_file_name
                file_utils.copy_file(job.source_file_path, output_file_path)

        job_end_time = time.perf_counter()
        job_duration_seconds = job_end_time - job_start_time

        if not was_job_already_processed:
            job.encoder_data.encoding_stage.job_total_time_seconds = job_duration_seconds
            json_serializer.serialize_to_json(job.encoder_data, job.metadata_json_file_path)

        processed_jobs_count += 1
        log.info("Job finished.")
        log.info("|-Source video: %s", job.source_file_path)
        log.info("|-Total time processing: %.2f seconds", job_duration_seconds)
        log.info("|-Processed jobs: %d/%d", processed_jobs_count, len(jobs_list))


if __name__ == "__main__":
    main()
