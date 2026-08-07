from app.model.dto import Encoding
from app.model.entity.entities import EncodingEntity
from app.model.mapper.abstract_mapper import AbstractMapper


class EncodingMapper(AbstractMapper[Encoding, EncodingEntity]):
    @staticmethod
    def to_entity(dto: Encoding) -> EncodingEntity:
        return EncodingEntity(
            id=dto.id,
            codec=EncodingMapper._get_required(dto.codec, "codec"),
            preset=dto.preset,
            encoder=dto.encoder
        )

    @staticmethod
    def to_dto(entity: EncodingEntity) -> Encoding:
        return Encoding(
            id=entity.id,
            codec=entity.codec,
            preset=entity.preset,
            encoder=entity.encoder
        )
