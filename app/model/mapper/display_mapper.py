from app.model.dto import Display
from app.model.entity.entities import DisplayEntity
from app.model.mapper.abstract_mapper import AbstractMapper


class DisplayMapper(AbstractMapper[Display, DisplayEntity]):
    @staticmethod
    def to_entity(dto: Display) -> DisplayEntity:
        return DisplayEntity(
            id=dto.id,
            width_px=DisplayMapper._get_required(dto.width_px, "width_px"),
            height_px=DisplayMapper._get_required(dto.height_px, "height_px"),
            display_aspect_ratio=DisplayMapper._get_required(
                dto.display_aspect_ratio,
                "display_aspect_ratio"
            ),
            pixel_format=DisplayMapper._get_required(dto.pixel_format, "pixel_format"),
            chroma_sample_location=DisplayMapper._get_required(
                dto.chroma_sample_location,
                "chroma_sample_location"
            )
        )

    @staticmethod
    def to_dto(entity: DisplayEntity) -> Display:
        return Display(
            id=entity.id,
            width_px=entity.width_px,
            height_px=entity.height_px,
            display_aspect_ratio=entity.display_aspect_ratio,
            pixel_format=entity.pixel_format,
            chroma_sample_location=entity.chroma_sample_location
        )
