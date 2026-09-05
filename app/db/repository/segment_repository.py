from sqlalchemy import select
from sqlalchemy.orm import selectinload

from app.db.database_manager import DatabaseManager
from app.db.repository.abstract_repository import AbstractRepository
from app.model.dto import Segment
from app.model.entity.entities import IterationEntity, SegmentEntity
from app.model.mapper.mappers import SegmentMapper


class SegmentRepository(AbstractRepository[Segment, SegmentEntity]):
    def __init__(self):
        super().__init__(SegmentEntity, SegmentMapper)

    def get_by_id(self, entity_id: int) -> Segment | None:
        with DatabaseManager.get_instance().get_session() as session:
            stmt = (
                select(SegmentEntity)
                .where(SegmentEntity.id == entity_id)
                .options(
                    selectinload(SegmentEntity.iterations).selectinload(
                        IterationEntity.evaluations
                    )
                )
            )
            entity = session.scalars(stmt).one_or_none()
            if entity is None:
                return None
            return SegmentMapper.to_dto(entity)
