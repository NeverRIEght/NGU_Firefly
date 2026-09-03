import threading
from enum import Enum
from typing import Generic, Type, TypeVar

from sqlalchemy.orm import Session

E = TypeVar("E", bound=Enum)
T = TypeVar("T")


class EnumLookupCache(Generic[E, T]):
    def __init__(self, enum_cls: Type[E], entity_cls: Type[T]):
        self._enum_cls = enum_cls
        self._entity_cls = entity_cls
        self._lock = threading.Lock()
        self._enum_to_id: dict[E, int] = {}
        self._id_to_enum: dict[int, E] = {}
        self._name_to_id: dict[str, int] = {}
        self._id_to_entity: dict[int, T] = {}
        self._is_initialized: bool = False

    def init(self, session: Session) -> None:
        with self._lock:
            existing_entities = session.query(self._entity_cls).all()
            existing_by_name = {e.name: e for e in existing_entities}

            to_insert = []
            for enum_item in self._enum_cls:
                name_val = enum_item.value
                if name_val not in existing_by_name:
                    new_entity = self._entity_cls(name=name_val)
                    session.add(new_entity)
                    to_insert.append(new_entity)

            if to_insert:
                session.commit()
                existing_entities = session.query(self._entity_cls).all()

            self._enum_to_id.clear()
            self._id_to_enum.clear()
            self._name_to_id.clear()
            self._id_to_entity.clear()

            for entity in existing_entities:
                try:
                    enum_val = self._enum_cls(entity.name)
                    self._enum_to_id[enum_val] = entity.id
                    self._id_to_enum[entity.id] = enum_val
                except ValueError:
                    pass
                self._name_to_id[entity.name] = entity.id
                self._id_to_entity[entity.id] = entity

            self._is_initialized = True

    def _ensure_initialized(self) -> None:
        if not self._is_initialized:
            with self._lock:
                if not self._is_initialized:
                    from app.db.database_manager import DatabaseManager
                    session = DatabaseManager.get_instance().get_session()
                    try:
                        self.init(session)
                    finally:
                        session.close()

    def get_id(self, enum_val: E) -> int:
        self._ensure_initialized()
        if enum_val not in self._enum_to_id:
            raise KeyError(f"Enum value '{enum_val}' not found in lookup cache for {self._enum_cls.__name__}")
        return self._enum_to_id[enum_val]

    def get_id_by_name(self, name: str) -> int:
        self._ensure_initialized()
        if name not in self._name_to_id:
            raise KeyError(f"Name '{name}' not found in lookup cache for {self._enum_cls.__name__}")
        return self._name_to_id[name]

    def get_enum(self, entity_id: int) -> E:
        self._ensure_initialized()
        if entity_id not in self._id_to_enum:
            raise KeyError(f"ID '{entity_id}' not found in lookup cache for {self._enum_cls.__name__}")
        return self._id_to_enum[entity_id]

    def get_stage(self, entity_id: int) -> E:
        return self.get_enum(entity_id)

    def get_entity(self, enum_val: E) -> T:
        entity_id = self.get_id(enum_val)
        return self._id_to_entity[entity_id]
