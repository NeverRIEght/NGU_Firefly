import logging

from app import file_utils
from app import hashing_service
from app import job_composer
from app import encoder
from pathlib import Path

from app import json_serializer
from app.extractor import video_attributes_extractor, ffmpeg_metadata_extractor
from app.model.encoding_stage import EncodingStageNamesEnum
from app.model.file_attributes import FileAttributes
from app.model.source_video import SourceVideo

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
    jobs_list = job_composer.compose_jobs()
    for job in jobs_list:
        current_stage = job.encoder_data.encoding_stage.stage_name

        # if COMPLETED - skip
        if current_stage == EncodingStageNamesEnum.COMPLETED:
            log.info("Job already encoded, skipping.")

        # if PREPARED - extract metadata
        if current_stage == EncodingStageNamesEnum.PREPARED:
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
            current_stage = job.encoder_data.encoding_stage.stage_name

        # if METADATA_EXTRACTED - start binary search with initial values from .env
        if current_stage is EncodingStageNamesEnum.METADATA_EXTRACTED:
            encoder.encode_job(job)

        if current_stage == EncodingStageNamesEnum.SEARCHING_CRF:
            encoder.encode_job(job)

        # if SEARCHING_CRF - start binary search with the values from the json data
        # if CRF_FOUND - perform one final encoding with the "crf_range_min" from the json data. Also, perform a check if the "crf_range_min" is the same as the "crf_range_max" = search completed


if __name__ == "__main__":
    main()
