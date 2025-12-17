import logging

log = logging.getLogger(__name__)

from pathlib import Path
from app_config import ConfigManager
from model import EncoderJobContext, EncoderDataJson, EncodingStage, EncodingStageNamesEnum
import metadata_extractor
import json_serializer
from json_serializer import load_from_json


def compose_jobs() -> list[EncoderJobContext]:
    app_config = ConfigManager.get_config()
    log.info(f"Starting to compose jobs for input directory: {app_config.input_dir}")

    jobs = []

    input_dir = Path(app_config.input_dir)

    for item in input_dir.iterdir():
        if item.is_file() and item.suffix.lower() == ".mp4":
            current_file_path = item

            log.info(f"Composing job for file: {current_file_path}")

            json_path = _find_associated_metadata_json_file(current_file_path)
            if json_path is None:
                json_name = _get_json_name_for_video_file(current_file_path)
                log.info(f"Json metadata file not found for {current_file_path}, creating new json file: {json_name}")
                new_json_path = current_file_path.parent / json_name

                job_context = _initialize_encoder_job(current_file_path, new_json_path)
                json_serializer.serialize_to_json(job_context.report_data, new_json_path)

                metadata_extractor.extract(job_context)

                json_serializer.serialize_to_json(job_context.report_data, new_json_path)

                jobs.append(job_context)
            else:
                try:
                    json_data = load_from_json(json_path)

                    job = EncoderJobContext(
                        source_file_path=current_file_path,
                        metadata_json_file_path=json_path,
                        report_data=json_data
                    )

                    jobs.append(job)
                except Exception as e:
                    log.error(f"Failed to load job from JSON for file {current_file_path}. Exception: {e}")

    log.info(f"Finished composing jobs. Total jobs composed: {len(jobs)}")
    return jobs


def _find_associated_metadata_json_file(video_file_path: Path) -> Path | None:
    json_file_name = _get_json_name_for_video_file(video_file_path)
    expected_json_path = video_file_path.parent / json_file_name

    if expected_json_path.exists():
        return expected_json_path
    else:
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

    report = EncoderDataJson(
        encoding_stage=stage
    )

    job_context = EncoderJobContext(
        source_file_path=source_file_path,
        metadata_json_file_path=json_file_path,
        report_data=report
    )

    return job_context
