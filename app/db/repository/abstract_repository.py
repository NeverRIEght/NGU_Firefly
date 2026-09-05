from abc import ABC
from typing import Generic, TypeVar

from sqlalchemy import select

from app.db.database_manager import DatabaseManager
from app.model.mapper.abstract_mapper import AbstractMapper

DTO_T = TypeVar("DTO_T")
ENTITY_T = TypeVar("ENTITY_T")


class AbstractRepository(ABC, Generic[DTO_T, ENTITY_T]):
    def __init__(
        self,
        entity_cls: type[ENTITY_T],
        mapper_cls: type[AbstractMapper[DTO_T, ENTITY_T]],
    ):
        self._entity_cls = entity_cls
        self._mapper_cls = mapper_cls

    def get_by_id(self, entity_id: int) -> DTO_T | None:
        with DatabaseManager.get_instance().get_session() as session:
            entity = session.get(self._entity_cls, entity_id)
            if entity is None:
                return None
            return self._mapper_cls.to_dto(entity)

    def get_required(self, entity_id: int) -> DTO_T:
        dto = self.get_by_id(entity_id)
        if dto is None:
            raise ValueError(
                f"{self._entity_cls.__name__} with id {entity_id} not found"
            )
        return dto

    def save(self, dto: DTO_T) -> DTO_T:
        with DatabaseManager.get_instance().get_session() as session:
            entity = self._mapper_cls.to_entity(dto)
            if getattr(dto, "id", None) is None:
                session.add(entity)
                session.commit()
                return self._mapper_cls.to_dto(entity)
            merged_entity = session.merge(entity)
            session.commit()
            return self._mapper_cls.to_dto(merged_entity)

    def save_all(self, dtos: list[DTO_T]) -> list[DTO_T]:
        if not dtos:
            return []
        with DatabaseManager.get_instance().get_session() as session:
            saved_entities: list[ENTITY_T] = []
            for dto in dtos:
                entity = self._mapper_cls.to_entity(dto)
                if getattr(dto, "id", None) is None:
                    session.add(entity)
                    saved_entities.append(entity)
                else:
                    merged = session.merge(entity)
                    saved_entities.append(merged)
            session.commit()
            return [self._mapper_cls.to_dto(entity) for entity in saved_entities]

    def delete_by_id(self, entity_id: int) -> bool:
        with DatabaseManager.get_instance().get_session() as session:
            entity = session.get(self._entity_cls, entity_id)
            if entity is None:
                return False
            session.delete(entity)
            session.commit()
            return True

    def exists_by_id(self, entity_id: int) -> bool:
        with DatabaseManager.get_instance().get_session() as session:
            stmt = select(self._entity_cls.id).where(self._entity_cls.id == entity_id)
            return session.scalar(stmt) is not None
