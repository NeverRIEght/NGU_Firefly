import logging
import threading
from typing import Any, Dict, List, Optional

from app.config.app_config import ConfigManager
from app.migrations.job_data_migrator import JobDataMigrator
from app.migrations.migration_exception import MigrationException
from app.migrations.versions.v1_to_v3_migrator import V1ToV3Migrator

log = logging.getLogger(__name__)


class MigrationManager:
    _instance: Optional[MigrationManager] = None
    _lock = threading.Lock()

    def __init__(self, target_version: int):
        self._target_version = target_version

        # Migrators list. Order is important.
        self._steps: List[JobDataMigrator] = [
            V1ToV3Migrator(),
        ]

    def apply(self, data: Dict[str, Any]) -> Dict[str, Any]:
        current_v = data.get("schema_version", 1)

        log.debug("Performing model migration...")
        log.debug("|-current_version=%d", current_v)
        log.debug("|-target_version=%d", self._target_version)

        while current_v < self._target_version:
            migrator = self._find_migrator(current_v)
            log.debug(f"Migrating model version: {current_v} -> {self._target_version}")
            data = migrator.migrate(data)
            current_v = data["schema_version"]

        log.debug("Model migration completed.")
        log.debug("|-current_version=%d", current_v)

        return data

    def _find_migrator(self, version: int) -> JobDataMigrator:
        app_config = ConfigManager.get_config()
        for step in self._steps:
            if step.source_version == version:
                return step
        raise MigrationException("No migrator found",
                                 source_version=version,
                                 target_version=app_config.schema_version)

    @classmethod
    def get_instance(cls) -> MigrationManager:
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    app_config = ConfigManager.get_config()
                    current_version = app_config.schema_version
                    cls._instance = MigrationManager(current_version)
        return cls._instance
