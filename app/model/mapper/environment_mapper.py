from app.model.dto import Environment
from app.model.entity.entities import EnvironmentEntity
from app.model.mapper.abstract_mapper import AbstractMapper


class EnvironmentMapper(AbstractMapper[Environment, EnvironmentEntity]):

    @staticmethod
    def to_entity(dto: Environment) -> EnvironmentEntity:
        return EnvironmentEntity(
            id=dto.id,
            firefly_version=EnvironmentMapper._get_required(dto.firefly_version, "firefly_version"),
            ffmpeg_version=EnvironmentMapper._get_required(dto.ffmpeg_version, "ffmpeg_version"),
            compression_engine_version=EnvironmentMapper._get_required(
                dto.compression_engine_version,
                "compression_engine_version"
            )
        )

    @staticmethod
    def to_dto(entity: EnvironmentEntity) -> Environment:
        return Environment(
            id=entity.id,
            firefly_version=entity.firefly_version,
            ffmpeg_version=entity.ffmpeg_version,
            compression_engine_version=entity.compression_engine_version
        )
