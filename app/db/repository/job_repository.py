from pathlib import Path

from sqlalchemy import Select, select
from sqlalchemy.orm import joinedload, selectinload

from app.db.database_manager import DatabaseManager
from app.db.repository.abstract_repository import AbstractRepository
from app.model.dto import Job
from app.model.entity.entities import (
    FileEntity,
    IterationEntity,
    JobEntity,
    SegmentEntity,
    VideoEntity,
)
from app.model.mapper.mappers import JobMapper


class JobRepository(AbstractRepository[Job, JobEntity]):
    def __init__(self):
        super().__init__(JobEntity, JobMapper)

    def _apply_options(self, stmt: Select, include_segments: bool) -> Select:
        stmt = stmt.options(
            joinedload(JobEntity.source_video).joinedload(VideoEntity.file),
            joinedload(JobEntity.source_video).joinedload(VideoEntity.display),
            joinedload(JobEntity.source_video).joinedload(VideoEntity.playback),
            joinedload(JobEntity.source_video).joinedload(VideoEntity.encoding),
            joinedload(JobEntity.source_video).joinedload(VideoEntity.color),
            joinedload(JobEntity.source_video).joinedload(VideoEntity.embedded_metadata),
            joinedload(JobEntity.output_video).joinedload(VideoEntity.file),
            joinedload(JobEntity.output_video).joinedload(VideoEntity.display),
            joinedload(JobEntity.output_video).joinedload(VideoEntity.playback),
            joinedload(JobEntity.output_video).joinedload(VideoEntity.encoding),
            joinedload(JobEntity.output_video).joinedload(VideoEntity.color),
            joinedload(JobEntity.output_video).joinedload(VideoEntity.embedded_metadata),
        )
        if include_segments:
            stmt = stmt.options(
                selectinload(JobEntity.segments)
                .selectinload(SegmentEntity.iterations)
                .selectinload(IterationEntity.evaluations)
            )
        return stmt

    def get_by_id(self, entity_id: int, include_segments: bool = True) -> Job | None:
        with DatabaseManager.get_instance().get_session() as session:
            stmt = select(JobEntity).where(JobEntity.id == entity_id)
            stmt = self._apply_options(stmt, include_segments)
            entity: JobEntity | None = session.scalars(stmt).one_or_none()
            if entity is None:
                return None
            return JobMapper.to_dto(entity)

    def get_by_source_path(self, path: Path | str, include_segments: bool = True) -> Job | None:
        with DatabaseManager.get_instance().get_session() as session:
            stmt = (
                select(JobEntity)
                .join(JobEntity.source_video)
                .join(VideoEntity.file)
                .where(FileEntity.absolute_path == str(path))
            )
            stmt = self._apply_options(stmt, include_segments)
            entity: JobEntity | None = session.scalars(stmt).one_or_none()
            if entity is None:
                return None
            return JobMapper.to_dto(entity)
