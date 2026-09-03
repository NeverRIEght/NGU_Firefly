from pathlib import Path

from sqlalchemy import create_engine, event
from sqlalchemy.orm import Session, sessionmaker

from app.config.config_manager import ConfigManager
from app.db.cache import LookupCacheRegistry
from app.model.entity.entities import Base


class DatabaseManager:
    _instance = None

    def __init__(self):
        app_config = ConfigManager.get_config()
        data_dir = app_config.output_dir / "firefly" / "data"
        self._db_path = Path(data_dir) / "firefly.db"
        self._db_path.parent.mkdir(parents=True, exist_ok=True)

        self._engine = create_engine(
            f"sqlite:///{self._db_path}",
            connect_args={"check_same_thread": False}
        )

        @event.listens_for(self._engine, "connect")
        def set_sqlite_pragma(dbapi_connection, connection_record):
            cursor = dbapi_connection.cursor()
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.close()

        self._session_factory = sessionmaker(
            bind=self._engine,
            expire_on_commit=False
        )

    @classmethod
    def get_instance(cls) -> "DatabaseManager":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def init_db(self):
        """Called once to init the db schema"""
        Base.metadata.create_all(bind=self._engine)
        with self.get_session() as session:
            LookupCacheRegistry.init_all(session)

    def get_session(self) -> Session:
        """Creates a separate session for the thread/operation."""
        return self._session_factory()