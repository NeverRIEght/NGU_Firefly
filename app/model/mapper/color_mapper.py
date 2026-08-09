from app.model.dto import Color, HdrFormat, ColorStandard, ColorRange
from app.model.entity.entities import ColorEntity, HdrFormatEntity, ColorStandardsEntity, ColorRangesEntity
from app.model.mapper.abstract_mapper import AbstractMapper


class ColorMapper(AbstractMapper[Color, ColorEntity]):
    @staticmethod
    def to_entity(dto: Color) -> ColorEntity:
        hdr_format = None
        if dto.hdr_format is not None:
            hdr_format = HdrFormatEntity(name=dto.hdr_format.value)

        color_primaries = None
        if dto.color_primaries is not None:
            color_primaries = ColorStandardsEntity(name=dto.color_primaries.value)

        color_trc = None
        if dto.color_trc is not None:
            color_trc = ColorStandardsEntity(name=dto.color_trc.value)

        colorspace = None
        if dto.colorspace is not None:
            colorspace = ColorStandardsEntity(name=dto.colorspace.value)

        color_range = None
        if dto.color_range is not None:
            color_range = ColorRangesEntity(name=dto.color_range.value)

        return ColorEntity(
            id=dto.id,
            hdr_format=hdr_format,
            color_primaries=color_primaries,
            color_trc=color_trc,
            colorspace=colorspace,
            color_range=color_range,
            max_cll=dto.max_cll,
            master_display=dto.master_display,
            dovi_profile=dto.dovi_profile
        )

    @staticmethod
    def to_dto(entity: ColorEntity) -> Color:
        def _to_hdr(value):
            if value is None or not value.name:
                return None
            try:
                return HdrFormat(value.name)
            except ValueError:
                return None

        def _to_standard(value):
            if value is None or not value.name:
                return None
            try:
                return ColorStandard(value.name)
            except ValueError:
                return None

        def _to_range(value):
            if value is None or not value.name:
                return None
            try:
                return ColorRange(value.name)
            except ValueError:
                return None

        return Color(
            id=entity.id,
            hdr_format=_to_hdr(entity.hdr_format),
            color_primaries=_to_standard(entity.color_primaries),
            color_trc=_to_standard(entity.color_trc),
            colorspace=_to_standard(entity.colorspace),
            color_range=_to_range(entity.color_range),
            max_cll=entity.max_cll,
            master_display=entity.master_display,
            dovi_profile=entity.dovi_profile
        )
