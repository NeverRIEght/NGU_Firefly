from typing import Any, Dict

from app.migrations.dict_utils import get_required_or_key_error, get_optional_or_default
from app.migrations.job_data_migrator import JobDataMigrator
from app.migrations.migration_exception import MigrationException


class V1ToV3Migrator(JobDataMigrator):
    @property
    def source_version(self) -> int:
        return 1

    @property
    def target_version(self) -> int:
        return 3

    def migrate(self, source_model: Dict[str, Any]) -> Dict[str, Any]:
        try:
            source_video = get_required_or_key_error("source_video", source_model)
            self._migrate_file_attributes(source_video.get("file_attributes"))

            iterations = get_required_or_key_error("iterations", source_model)
            for iteration in iterations:
                file_attributes = get_required_or_key_error("file_attributes", iteration)
                self._migrate_file_attributes(file_attributes)

            source_model["schema_version"] = self.target_version
            return source_model
        except KeyError as e:
            raise MigrationException(f"Model invalid: {e}",
                                     source_version=self.source_version,
                                     target_version=self.target_version)

    @staticmethod
    def _migrate_file_attributes(file_attributes: Dict[str, Any]):
        size_mb: float = get_optional_or_default("file_size_megabytes", 0, file_attributes)
        size_bytes: int = int(size_mb * 1024 * 1024)
        file_attributes["file_size_bytes"] = size_bytes
