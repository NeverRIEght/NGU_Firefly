from pathlib import Path

from app.model.dto.file import File
from app.model.entity.entities import FileEntity
from app.model.mapper.abstract_mapper import AbstractMapper


class FileMapper(AbstractMapper[File, FileEntity]):
    @staticmethod
    def to_entity(dto: File) -> FileEntity:
        return FileEntity(
            id=dto.id,
            file_name=FileMapper._get_required(dto.file_name, "file_name"),
            absolute_path=str(dto.absolute_path),
            file_size_bytes=FileMapper._get_required(dto.file_size_bytes, "file_size_bytes"),
            sha256_hash=dto.sha256_hash
        )

    @staticmethod
    def to_dto(entity: FileEntity) -> File:
        return File(
            id=entity.id,
            file_name=entity.file_name,
            absolute_path=Path(entity.absolute_path) if entity.absolute_path else None,
            file_size_bytes=entity.file_size_bytes,
            sha256_hash=entity.sha256_hash
        )
