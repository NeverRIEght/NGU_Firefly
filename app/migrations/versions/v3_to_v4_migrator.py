from typing import Any, Dict

from app.migrations.dict_utils import get_required_or_key_error, get_optional_or_default, get_optional_or_key_error
from app.migrations.abstract_migrator import AbstractMigrator
from app.migrations.migration_exception import MigrationException


class V1ToV3Migrator(AbstractMigrator):
    @property
    def source_version(self) -> int:
        return 3

    @property
    def target_version(self) -> int:
        return 4

    def migrate(self, source_model: Dict[str, Any]) -> Dict[str, Any]:
        try:
            iterations = get_required_or_key_error("iterations", source_model)
            for iteration in iterations:
                environment = get_optional_or_key_error("environment", iteration)

                environment["compression_engine_version"] = 1

                if "encoder_version" not in environment:
                    raise KeyError("Encoder version not found in environment")
                else:
                    del environment["encoder_version"]

            source_model["schema_version"] = self.target_version
            return source_model
        except KeyError as e:
            raise MigrationException(f"Model invalid: {e}",
                                     source_version=self.source_version,
                                     target_version=self.target_version)
