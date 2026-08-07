from app.model.dto import EmbeddedMetadata
from app.model.entity.entities import EmbeddedMetadataEntity
from app.model.mapper.abstract_mapper import AbstractMapper
from app.model.mapper.environment_mapper import EnvironmentMapper


class EmbeddedMetadataMapper(AbstractMapper[EmbeddedMetadata, EmbeddedMetadataEntity]):
    @staticmethod
    def to_entity(dto: EmbeddedMetadata) -> EmbeddedMetadataEntity:
        environment = EnvironmentMapper.to_entity(
            EmbeddedMetadataMapper._get_required(dto.environment, "environment")
        )

        return EmbeddedMetadataEntity(
            id=dto.id,
            encodes_count=EmbeddedMetadataMapper._get_required(
                dto.encodes_count,
                "encodes_count"
            ),
            last_encode_datetime_utc=EmbeddedMetadataMapper._get_required(
                dto.last_encode_datetime,
                "last_encode_datetime"
            ),
            source_video_sha256_hash=EmbeddedMetadataMapper._get_required(
                dto.source_video_sha256_hash,
                "source_video_sha256_hash"
            ),
            environment=environment
        )

    @staticmethod
    def to_dto(entity: EmbeddedMetadataEntity) -> EmbeddedMetadata:
        return EmbeddedMetadata(
            id=entity.id,
            encodes_count=entity.encodes_count,
            last_encode_datetime=entity.last_encode_datetime_utc,
            source_video_sha256_hash=entity.source_video_sha256_hash,
            environment=EnvironmentMapper.to_dto(entity.environment) if entity.environment else None
        )
