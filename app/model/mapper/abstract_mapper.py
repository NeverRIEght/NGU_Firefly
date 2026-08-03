from abc import ABC, abstractmethod
from typing import Generic, List, TypeVar

DTO_T = TypeVar("DTO_T")
ENTITY_T = TypeVar("ENTITY_T")


class AbstractMapper(ABC, Generic[DTO_T, ENTITY_T]):
    @staticmethod
    def _get_required[T](value: T | None, field_name: str) -> T:
        if value is None:
            raise ValueError(f"Cannot map '{field_name}' field, None value is not allowed")
        return value

    @staticmethod
    @abstractmethod
    def to_entity(dto: DTO_T) -> ENTITY_T:
        pass

    @staticmethod
    @abstractmethod
    def to_dto(entity: ENTITY_T) -> DTO_T:
        pass

    @classmethod
    def to_entities(cls, dto_list: List[DTO_T]) -> List[ENTITY_T]:
        return [cls.to_entity(dto) for dto in dto_list]

    @classmethod
    def to_dtos(cls, entity_list: List[ENTITY_T]) -> List[DTO_T]:
        return [cls.to_dto(entity) for entity in entity_list]