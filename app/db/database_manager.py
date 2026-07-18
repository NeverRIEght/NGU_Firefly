import sqlite3
import threading
from pathlib import Path
from app.config.config_manager import ConfigManager
from app.project_paths import ProjectPaths


class DatabaseManager:
    _instance = None

    def __init__(self):
        app_config = ConfigManager.get_config()
        data_dir = app_config.output_dir / "firefly" / "data"
        self._db_path = str(Path(data_dir) / "firefly.db")
        self._local = threading.local()

    @classmethod
    def get_instance(cls) -> "DatabaseManager":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    @property
    def connection(self) -> sqlite3.Connection:
        if not hasattr(self._local, "conn"):
            conn = sqlite3.connect(self._db_path)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA foreign_keys=ON")

            project_paths = ProjectPaths.get_instance()
            schema_path = project_paths.db_schema_file
            if not Path(self._db_path).exists() or Path(self._db_path).stat().st_size == 0:
                with open(schema_path) as f:
                    conn.executescript(f.read())

            self._local.conn = conn
        return self._local.conn
