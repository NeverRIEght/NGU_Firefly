from pathlib import Path

from sqlalchemy import Select, select
from sqlalchemy.orm import joinedload

from app.db.database_manager import DatabaseManager
from app.db.repository.abstract_repository import AbstractRepository
from app.model.dto import Video
from app.model.entity.entities import (
    EmbeddedMetadataEntity,
    FileEntity,
    VideoEntity,
)
from app.model.mapper.mappers import VideoMapper


class VideoRepository(AbstractRepository[Video, VideoEntity]):
    def __init__(self):
        super().__init__(VideoEntity, VideoMapper)

    def _apply_options(self, stmt: Select) -> Select:
        return stmt.options(
            joinedload(VideoEntity.file),
            joinedload(VideoEntity.display),
            joinedload(VideoEntity.playback),
            joinedload(VideoEntity.encoding),
            joinedload(VideoEntity.color),
            joinedload(VideoEntity.embedded_metadata).joinedload(
                EmbeddedMetadataEntity.environment
            ),
        )

    def get_by_id(self, entity_id: int) -> Video | None:
        with DatabaseManager.get_instance().get_session() as session:
            stmt = select(VideoEntity).where(VideoEntity.id == entity_id)
            stmt = self._apply_options(stmt)
            entity: VideoEntity | None = session.scalars(stmt).one_or_none()
            if entity is None:
                return None
            return VideoMapper.to_dto(entity)

    def get_by_file_path(self, path: Path | str) -> Video | None:
        with DatabaseManager.get_instance().get_session() as session:
            stmt = (
                select(VideoEntity)
                .join(VideoEntity.file)
                .where(FileEntity.absolute_path == str(path))
            )
            stmt = self._apply_options(stmt)
            entity: VideoEntity | None = session.scalars(stmt).one_or_none()
            if entity is None:
                return None
            return VideoMapper.to_dto(entity)
