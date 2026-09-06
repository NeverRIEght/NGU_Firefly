import logging
from pathlib import Path

from app.config.config_manager import ConfigManager
from app.db.repository.job_repository import JobRepository
from app.file_utils import delete_file_with_lock
from app.json_serializer import load_from_json
from app.migrations.legacy_json_job_mapper import LegacyJsonJobMapper

log = logging.getLogger(__name__)


class LegacyJsonImporter:
    _LEGACY_SUFFIXES = (".job.json", "_encoderdata.json")

    def __init__(self):
        app_config = ConfigManager.get_config()
        self._input_dir = app_config.input_dir
        self._output_dir = app_config.output_dir
        self._job_repository = JobRepository()

    def import_all(self) -> int:
        jobs_dir = self._output_dir / "firefly" / "data" / "jobs"
        if not jobs_dir.is_dir():
            log.debug("Legacy jobs directory does not exist: %s", jobs_dir)
            return 0

        imported_count = 0
        for item in sorted(jobs_dir.iterdir()):
            if item.is_file() and item.name.endswith(self._LEGACY_SUFFIXES):
                if self._import_file(item):
                    imported_count += 1

        log.info("Legacy JSON import completed. Migrated %d job(s).", imported_count)
        return imported_count

    def _import_file(self, job_file_path: Path) -> bool:
        log.debug("Processing legacy job file: %s", job_file_path)
        try:
            job_data = load_from_json(job_file_path)
            job_dto = LegacyJsonJobMapper.to_dto(
                job_data, self._input_dir, self._output_dir, job_file_path
            )

            source_path = (
                job_dto.source_video.file.absolute_path
                if job_dto.source_video and job_dto.source_video.file
                else None
            )

            if source_path is not None:
                existing_job = self._job_repository.get_by_source_path(source_path)
                if existing_job is not None:
                    log.debug(
                        "Legacy job for '%s' already exists in database (id=%s). Deleting legacy JSON file.",
                        source_path,
                        existing_job.id,
                    )
                    delete_file_with_lock(job_file_path)
                    return False

            self._job_repository.save(job_dto)
            log.debug("Successfully imported legacy job from '%s' into database.", job_file_path)
            delete_file_with_lock(job_file_path)
            return True

        except Exception as e:
            log.error(
                "Failed to import legacy job '%s'. File will be preserved on disk. Details: %s",
                job_file_path,
                e,
                exc_info=True,
            )
            return False
