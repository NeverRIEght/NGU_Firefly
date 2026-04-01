from abc import ABC, abstractmethod
from typing import Any, Dict


class AbstractMigrator(ABC):
    @property
    @abstractmethod
    def source_version(self) -> int:
        pass

    @property
    @abstractmethod
    def target_version(self) -> int:
        pass

    @abstractmethod
    def migrate(self, source_model: Dict[str, Any]) -> Dict[str, Any]:
        pass
