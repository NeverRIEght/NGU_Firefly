import threading
from pathlib import Path
from typing import Optional


class ProjectPaths:
    _instance: Optional["ProjectPaths"] = None
    _lock = threading.Lock()

    def __init__(self):
        if hasattr(self, "_base_dir"):
            return

        self._base_dir = self._find_base_dir()
        self._pyproject_file = self._base_dir / "pyproject.toml"
        self._app_config_file = self._base_dir / "app_config.toml"

    @classmethod
    def _find_base_dir(cls) -> Path:
        current_path = Path(__file__).resolve()
        for parent in [current_path] + list(current_path.parents):
            if (parent / "pyproject.toml").exists():
                return parent
        raise FileNotFoundError(
            "Could not find project root (pyproject.toml missing in parent directories)."
        )

    def _validate_files(self) -> None:
        for file_path in (self._pyproject_file, self._app_config_file):
            if not file_path.exists():
                raise FileNotFoundError(
                    f"Required project file missing: {file_path}"
                )

    @classmethod
    def get_instance(cls) -> "ProjectPaths":
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = ProjectPaths()
        return cls._instance

    @property
    def base_dir(self) -> Path:
        return self._base_dir

    @property
    def pyproject_file(self) -> Path:
        return self._pyproject_file

    @property
    def app_config_file(self) -> Path:
        return self._app_config_file
