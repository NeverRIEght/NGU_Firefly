import os
import sys
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
        self._vmaf_models_dir = self._base_dir / "vmaf_models"

        self._pyproject_file = self._base_dir / "pyproject.toml"
        self._app_config_file = self._base_dir / "app_config.toml"
        self._alembic_ini_file = self._base_dir / "alembic.ini"
        self._db_schema_file = self._base_dir / "app" / "db" / "schema.sql"
        self._default_database_path = self._resolve_default_database_path()

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
        for file_path in (self._pyproject_file, self._app_config_file, self._db_schema_file):
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

    @staticmethod
    def _resolve_default_database_path() -> Path:
        db_file_name = "firefly.db"

        if sys.platform == "win32":
            local_app_data = os.environ.get("LOCALAPPDATA")
            base = Path(local_app_data) if local_app_data else Path.home() / "AppData" / "Local"
            return base / "firefly" / db_file_name
        elif sys.platform == "darwin":
            return Path.home() / "Library" / "Application Support" / "firefly" / db_file_name
        else:
            xdg_data = os.environ.get("XDG_DATA_HOME")
            base = Path(xdg_data) if xdg_data else Path.home() / ".local" / "share"
            return base / "firefly" / db_file_name

    @property
    def alembic_ini_file(self) -> Path:
        return self._alembic_ini_file

    @property
    def base_dir(self) -> Path:
        return self._base_dir

    @property
    def vmaf_models_dir(self) -> Path:
        return self._vmaf_models_dir

    @property
    def default_database_path(self) -> Path:
        return self._default_database_path

    @property
    def pyproject_file(self) -> Path:
        return self._pyproject_file

    @property
    def app_config_file(self) -> Path:
        return self._app_config_file

    @property
    def db_schema_file(self) -> Path:
        return self._db_schema_file
