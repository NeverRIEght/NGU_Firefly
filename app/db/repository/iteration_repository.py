from sqlalchemy import select
from sqlalchemy.orm import joinedload, selectinload

from app.db.database_manager import DatabaseManager
from app.db.repository.abstract_repository import AbstractRepository
from app.model.dto import Iteration
from app.model.entity.entities import (
    EvaluationEntity,
    ExecutionDataEntity,
    IterationEntity,
    VideoEntity,
)
from app.model.mapper.mappers import IterationMapper


class IterationRepository(AbstractRepository[Iteration, IterationEntity]):
    def __init__(self):
        super().__init__(IterationEntity, IterationMapper)

    def get_by_id(self, entity_id: int) -> Iteration | None:
        with DatabaseManager.get_instance().get_session() as session:
            stmt = (
                select(IterationEntity)
                .where(IterationEntity.id == entity_id)
                .options(
                    joinedload(IterationEntity.video).joinedload(VideoEntity.file),
                    joinedload(IterationEntity.video).joinedload(VideoEntity.display),
                    joinedload(IterationEntity.video).joinedload(VideoEntity.playback),
                    joinedload(IterationEntity.video).joinedload(VideoEntity.encoding),
                    joinedload(IterationEntity.video).joinedload(VideoEntity.color),
                    joinedload(IterationEntity.video).joinedload(
                        VideoEntity.embedded_metadata
                    ),
                    joinedload(IterationEntity.environment),
                    joinedload(IterationEntity.execution_data).joinedload(
                        ExecutionDataEntity.encoding_cpu
                    ),
                    joinedload(IterationEntity.execution_data).joinedload(
                        ExecutionDataEntity.evaluation_cpu
                    ),
                    selectinload(IterationEntity.evaluations).joinedload(
                        EvaluationEntity.metric
                    ),
                )
            )
            entity = session.scalars(stmt).one_or_none()
            if entity is None:
                return None
            return IterationMapper.to_dto(entity)
