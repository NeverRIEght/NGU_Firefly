import logging

from app.model.encoder_data_json import EncoderDataJson
from app.model.encoder_job_context import EncoderJobContext
from app.model.encoding_stage import EncodingStage, EncodingStageNamesEnum

log = logging.getLogger(__name__)

from pathlib import Path
from app.app_config import ConfigManager
from app import json_serializer
from app.json_serializer import load_from_json
from app.file_utils import get_file_name_with_extension, get_file_name_without_extension


def compose_jobs() -> list[EncoderJobContext]:
    app_config = ConfigManager.get_config()
    log.info(f"Starting to compose jobs for input directory: {app_config.input_dir}")

    jobs = list()

    input_dir = Path(app_config.input_dir)
    output_dir = Path(app_config.output_dir)
    if not output_dir.exists():
        log.info(f"Output directory does not exist. Creating: {output_dir}")
        output_dir.mkdir(parents=True, exist_ok=True)

    for file in output_dir.iterdir():
        if file.is_file() and file.name.endswith("_encoderdata.json"):
            try:
                log.info(f"Validating existing json metadata file: {file}")
                json_data = load_from_json(file)

                source_video_name = json_data.source_video.file_attributes.file_name

                # search input directory for the corresponding video file
                source_video_path = input_dir / source_video_name
                if not source_video_path.exists():
                    log.error(f"Source video file {source_video_name} not found in"
                              f"input directory for metadata file {file}. Deleting metadata file.")
                    file.unlink()
                    continue

                json_file_path = file

                job = EncoderJobContext(
                    source_file_path=source_video_path,
                    metadata_json_file_path=json_file_path,
                    encoder_data=json_data
                )

                jobs.append(job)
                log.info(f"Metadata for {source_video_path} loaded from JSON")
            except Exception as e:
                log.error(f"Invalid json metadata file found: {file}. Exception: {e}. Deleting file.")
                file.unlink()

    for item in input_dir.iterdir():
        if item.is_file() and item.suffix.lower() == ".mp4":
            source_video_path = item

            log.info(f"Composing job for file: {source_video_path}")

            for existing_job in jobs:
                if existing_job.source_file_path == source_video_path:
                    log.info(f"Job already exists for file: {source_video_path}, skipping.")
                    break
                else:
                    for iteration in existing_job.encoder_data.iterations:
                        if iteration.file_attributes.file_name == get_file_name_with_extension(source_video_path):
                            log.info(f"File: {source_video_path} is an iteration of an existing job, skipping.")
                            break

            json_file_path = _find_metadata_json_file(source_video_path)
            if json_file_path is not None:
                log.error("Unloaded file metadata json found. Skipping file.")
            else:
                json_name = _get_json_name_for_video_file(source_video_path)
                log.info(f"Creating new json file for {source_video_path}: {json_name}")
                new_json_path = Path(app_config.output_dir) / json_name

                job_context = _initialize_encoder_job(source_video_path, new_json_path)
                json_serializer.serialize_to_json(job_context.encoder_data, new_json_path)

                jobs.append(job_context)
                log.info(f"Created new job for {source_video_path}")

    log.info(f"Finished composing jobs. Total jobs composed: {len(jobs)}")
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

    encoder_data = EncoderDataJson(
        encoding_stage=stage
    )

    job_context = EncoderJobContext(
        source_file_path=source_file_path,
        metadata_json_file_path=json_file_path,
        encoder_data=encoder_data
    )

    return job_context
