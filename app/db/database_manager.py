from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, event
from sqlalchemy.orm import Session, sessionmaker

from app.config.config_manager import ConfigManager
from app.db.cache import LookupCacheRegistry
from app.project_paths import ProjectPaths


class DatabaseManager:
    _instance = None

    def __init__(self):
        app_config = ConfigManager.get_config()
        paths = ProjectPaths.get_instance()
        if app_config.database_dir:
            self._db_path = app_config.database_dir / "firefly.db"
        else:
            self._db_path = paths.default_database_path
        self._db_path.parent.mkdir(parents=True, exist_ok=True)

        self._engine = create_engine(
            f"sqlite:///{self._db_path}",
            connect_args={"check_same_thread": False}
        )

        event.listen(self._engine, "connect", self._set_sqlite_pragma)

        self._session_factory = sessionmaker(
            bind=self._engine,
            expire_on_commit=False
        )

    @staticmethod
    def _set_sqlite_pragma(dbapi_connection, connection_record) -> None:
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    @classmethod
    def get_instance(cls) -> "DatabaseManager":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def init_db(self):
        """Initializes and migrates the db schema to latest revision."""
        paths = ProjectPaths.get_instance()
        alembic_cfg = Config(paths.alembic_ini_file)
        alembic_cfg.attributes["connection"] = self._engine
        command.upgrade(alembic_cfg, "head")

        with self.get_session() as session:
            LookupCacheRegistry.init_all(session)

    def get_session(self) -> Session:
        """Creates a separate session for the thread/operation."""
        return self._session_factory()