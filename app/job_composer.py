import logging

import file_utils
from app.model.encoder_data_json import EncoderDataJson
from app.model.encoder_job_context import EncoderJobContext
from app.model.encoding_stage import EncodingStage, EncodingStageNamesEnum
from app.model.file_attributes import FileAttributes
from app.model.source_video import SourceVideo

log = logging.getLogger(__name__)

from pathlib import Path
from app.app_config import ConfigManager
from app import json_serializer
from app.json_serializer import load_from_json
from app.file_utils import get_file_name_with_extension, get_file_name_without_extension, delete_file


def compose_jobs() -> list[EncoderJobContext]:
    app_config = ConfigManager.get_config()
    log.info("Composing encoding jobs...")
    log.info("|-Input directory: %s", app_config.input_dir)
    log.info("|-Output directory: %s", app_config.output_dir)

    jobs = []

    input_dir = Path(app_config.input_dir)
    output_dir = Path(app_config.output_dir)
    if not output_dir.exists():
        log.info("Output directory does not exist. Creating: %s", output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

    jsons_loaded = 0

    for file in output_dir.iterdir():
        if file.is_file() and file.name.endswith("_encoderdata.json"):
            metadata_file_path = file
            try:
                log.debug("Loading existing json metadata file: %s", metadata_file_path)
                json_data = load_from_json(metadata_file_path)

                source_video_path = input_dir / json_data.source_video.file_attributes.file_name
                if not source_video_path.exists():
                    log.error("Failed to validate metadata json file.")
                    log.error("|-Reason: source video file not found.")
                    log.error("|-Metadata file: %s", metadata_file_path)
                    log.error("|-Expected source video path: %s", source_video_path)
                    continue

                job = EncoderJobContext(
                    source_file_path=source_video_path,
                    metadata_json_file_path=metadata_file_path,
                    encoder_data=json_data
                )

                jobs.append(job)
                jsons_loaded += 1
                log.info(f"Metadata for {source_video_path} loaded from JSON")

                log.info("Composing encoding jobs...")
                log.info("|-Loading jobs from existing metadata files... %d", jsons_loaded)
            except Exception as e:
                log.warning(f"Invalid json metadata file found: {metadata_file_path}. Exception: {e}. Deleting file.")
                delete_file(metadata_file_path)

    log.info("Composing encoding jobs...")
    log.info("|-Loaded jobs from existing metadata files: %d", jsons_loaded)

    jobs_created = 0

    for item in input_dir.iterdir():
        if item.is_file() and item.suffix.lower() == ".mp4":
            source_video_path = item

            log.debug(f"Creating job for file: {source_video_path}")

            is_job_added = False
            for existing_job in jobs:
                if existing_job.source_file_path == source_video_path:
                    log.debug(f"Existing job found for file: {source_video_path}")
                    is_job_added = True
                    break
                else:
                    for iteration in existing_job.encoder_data.iterations:
                        if iteration.file_attributes.file_name == get_file_name_with_extension(source_video_path):
                            log.debug(f"File appears to be an iteration of an existing job: {source_video_path}")
                            is_job_added = True
                            break

            if is_job_added:
                log.info(f"Job exists for file: {source_video_path}, skipping.")
                continue

            json_file_path = _find_metadata_json_file(source_video_path)
            if json_file_path is not None:
                log.error("Non-loaded file metadata json found. Skipping file. Re-launch the application to load it.")
            else:
                json_name = _get_json_name_for_video_file(source_video_path)
                log.debug(f"Creating new json file for {source_video_path}: {json_name}")
                new_json_path = Path(app_config.output_dir) / json_name

                job_context = _initialize_encoder_job(source_video_path, new_json_path)
                json_serializer.serialize_to_json(job_context.encoder_data, new_json_path)

                jobs.append(job_context)
                log.info(f"Created new job for {source_video_path}")
                jobs_created += 1
                log.info("Composing encoding jobs...")
                log.info("|-Loaded jobs from existing metadata files: %d", jsons_loaded)
                log.info("|-Creating new jobs from source files... %d", jobs_created)

    log.info(f"Finished composing jobs: {len(jobs)}")
    log.info("|-Loaded jobs from existing metadata files: %d", jsons_loaded)
    log.info("|-Created new jobs from source files: %d", jobs_created)
    return jobs


def _find_metadata_json_file(video_file_path: Path) -> Path | None:
    app_config = ConfigManager.get_config()
    json_file_name = _get_json_name_for_video_file(video_file_path)
    expected_json_path = Path(app_config.output_dir) / json_file_name

    if expected_json_path.exists():
        return expected_json_path
    else:
        return None


def _find_source_video_file(json_file_path: Path) -> Path | None:
    app_config = ConfigManager.get_config()
    json_stem = get_file_name_without_extension(json_file_path)
    if json_stem.endswith("_encoderdata"):
        video_file_name = json_stem[:-len("_encoderdata")] + ".mp4"

        for file in Path(app_config.input_dir).iterdir():
            if file.is_file() and get_file_name_with_extension(file) == video_file_name:
                return file

    return None


def _get_json_name_for_video_file(video_file_path: Path) -> str:
    return f"{video_file_path.stem}_encoderdata.json"


def _initialize_encoder_job(source_file_path: Path, json_file_path: Path) -> EncoderJobContext:
    app_config = ConfigManager.get_config()
    stage = EncodingStage(
        stage_number_from_1=1,
        stage_name=EncodingStageNamesEnum.PREPARED,
        crf_range_min=app_config.crf_min,
        crf_range_max=app_config.crf_max,
    )

    file_attributes = FileAttributes(
        file_name=get_file_name_with_extension(source_file_path),
        file_size_megabytes=file_utils.get_file_size_megabytes(source_file_path)
    )

    source_video = SourceVideo(
        file_attributes=file_attributes,
    )

    encoder_data = EncoderDataJson(
        source_video=source_video,
        encoding_stage=stage
    )

    job_context = EncoderJobContext(
        source_file_path=source_file_path,
        metadata_json_file_path=json_file_path,
        encoder_data=encoder_data
    )

    return job_context
