import logging
import sys
from pathlib import Path

from app import json_serializer, hashing_service, file_utils
from app.config.app_config import ConfigManager
from app.file_utils import delete_file
from app.json_serializer import load_from_json
from app.model.encoder_job_context import EncoderJob
from app.model.json.encoder_data import JobData
from app.model.json.encoding_stage import EncodingStage, EncodingStageNamesEnum
from app.model.json.file_attributes import FileAttributes
from app.model.json.source_video import SourceVideo

log = logging.getLogger(__name__)

JOB_FILE_SUFFIX = "_encoderdata.json"


def update_progress(current, total, prefix=""):
    if total <= 0:
        return

    percent = (current / total) * 100

    status_line = f"\r{prefix} |{percent:.1f}% ({current}/{total})\033[K"

    sys.stdout.write(status_line)
    sys.stdout.flush()


def compose_jobs() -> list[EncoderJob]:
    app_config = ConfigManager.get_config()
    log.info("Composing encoding jobs...")
    log.info("|-Input directory: %s", app_config.input_dir)
    log.info("|-Output directory: %s", app_config.output_dir)

    firefly_data_directory = app_config.output_dir / "firefly" / "data"
    firefly_data_directory.mkdir(parents=True, exist_ok=True)

    firefly_jobs_directory = firefly_data_directory / "jobs"
    firefly_jobs_directory.mkdir(parents=True, exist_ok=True)

    log.info("|-Jobs directory: %s", firefly_jobs_directory)

    existing_jobs = _load_existing_jobs(firefly_jobs_directory)
    new_jobs = _create_jobs_from_source_files(existing_jobs)

    loaded_jobs_count = len(existing_jobs)
    created_jobs_count = len(new_jobs)

    jobs = existing_jobs + new_jobs

    log.info("Finished composing jobs: %d", len(jobs))
    log.info("|-Loaded jobs from existing metadata files: %d", loaded_jobs_count)
    log.info("|-Created new jobs from source files: %d", created_jobs_count)
    return jobs


def _load_existing_jobs(from_directory: Path) -> list[EncoderJob]:
    app_config = ConfigManager.get_config()

    def _handle_invalid_existing_job(path: Path, reason: str, exc: Exception = None):
        log.error("Failed to load job metadata file: %s", path)
        if exc:
            log.error("|-Reason: %s. Exception: %s", reason, exc)
        else:
            log.error("|-Reason: %s", reason)
        log.error("|-Action: deleting invalid job metadata file.")
        delete_file(path)

    jobs = []

    for file in from_directory.iterdir():
        if file.is_file() and file.name.endswith(JOB_FILE_SUFFIX):
            job_file_path = file
            try:
                log.debug("Loading existing job metadata from file: %s", job_file_path)

                try:
                    job_data = load_from_json(job_file_path)
                except FileNotFoundError as e:
                    _handle_invalid_existing_job(path=job_file_path, reason="file not found", exc=e)
                    continue
                except ValueError as e:
                    _handle_invalid_existing_job(path=job_file_path, reason="invalid json format", exc=e)
                    continue

                _is_valid = _validate_job_data(job_data, job_file_path)
                if not _is_valid:
                    _handle_invalid_existing_job(job_file_path, "validation failed")
                    continue

                source_video_path = app_config.input_dir / job_data.source_video.file_attributes.file_name

                job = EncoderJob(
                        source_file_path=source_video_path,
                        metadata_json_file_path=job_file_path,
                        job_data=job_data
                )

                jobs.append(job)
                log.debug("Existing job loaded for file: %s", source_video_path)
            except Exception as e:
                log.warning(f"Invalid job metadata file found: {job_file_path}. Exception: {e}. Deleting file.")
                delete_file(job_file_path)

    return jobs


def _create_jobs_from_source_files(existing_jobs: list[EncoderJob]) -> list[EncoderJob]:
    app_config = ConfigManager.get_config()

    jobs_map: dict[str, EncoderJob] = {}
    new_jobs = []

    for job in existing_jobs:
        jobs_map[job.job_data.source_video.sha256_hash] = job
        for iteration in job.job_data.iterations:
            jobs_map[iteration.sha256_hash] = job

    for item in app_config.input_dir.iterdir():
        if item.is_file() and item.suffix.lower() == ".mp4":
            source_video_path = item

            log.debug(f"Creating job for video: {source_video_path}")

            source_video_hash = hashing_service.calculate_sha256_hash(source_video_path)
            if source_video_hash in jobs_map:
                log.debug(f"Existing job found for video by hash: {source_video_path}")
                log.debug(f"Job exists for file: {source_video_path}, skipping.")
                continue

            json_name = f"{source_video_path.stem}{JOB_FILE_SUFFIX}.json"
            log.debug(f"Creating new job metadata file for {source_video_path}: {json_name}")
            new_json_path = Path(app_config.output_dir) / json_name

            job_context = _initialize_encoder_job(source_video_path, new_json_path)
            json_serializer.serialize_to_json(job_context.job_data, new_json_path)

            new_jobs.append(job_context)
            log.debug(f"Created new job for {source_video_path}")

    return new_jobs


def _validate_job_data(job_data: JobData, job_file_path: Path) -> bool:
    app_config = ConfigManager.get_config()
    source_video_path = app_config.input_dir / job_data.source_video.file_attributes.file_name
    if not source_video_path.exists():
        log.error("Failed to validate job metadata file.")
        log.error("|-Reason: source video file not found.")
        log.error("|-Job metadata file: %s", job_file_path)
        log.error("|-Expected source video path: %s", source_video_path)
        return False

    if hashing_service.calculate_sha256_hash(source_video_path) != job_data.source_video.sha256_hash:
        log.error("Failed to validate job metadata file.")
        log.error("|-Reason: source video file hash mismatch.")
        log.error("|-Job metadata file: %s", job_file_path)
        log.error("|-Source video path: %s", source_video_path)
        log.error("|-Action: deleting invalid job metadata file.")
        delete_file(job_file_path)
        return False

    return True


def _initialize_encoder_job(source_file_path: Path, json_file_path: Path) -> EncoderJob:
    app_config = ConfigManager.get_config()

    job_context = EncoderJob(
            source_file_path=source_file_path,
            metadata_json_file_path=json_file_path,
            job_data=JobData(
                    schema_version=app_config.schema_version,
                    source_video=SourceVideo(
                            file_attributes=FileAttributes(
                                    file_name=file_utils.get_file_name_with_extension(source_file_path),
                                    file_size_megabytes=file_utils.get_file_size_megabytes(source_file_path)
                            ),
                            sha256_hash=hashing_service.calculate_sha256_hash(source_file_path)
                    ),
                    encoding_stage=EncodingStage(
                            stage_number_from_1=1,
                            stage_name=EncodingStageNamesEnum.PREPARED,
                            crf_range_min=app_config.crf_min,
                            crf_range_max=app_config.crf_max,
                    )
            )
    )

    return job_context
