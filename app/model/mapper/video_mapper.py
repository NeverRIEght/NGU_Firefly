from app.model.dto import Video
from app.model.entity.entities import VideoEntity
from app.model.mapper import ColorMapper, DisplayMapper, EncodingMapper, FileMapper, PlaybackMapper
from app.model.mapper.abstract_mapper import AbstractMapper


class VideoMapper(AbstractMapper[Video, VideoEntity]):
    @staticmethod
    def to_entity(dto: Video) -> VideoEntity:
        file = FileMapper.to_entity(dto.file) if dto.file else None
        display = DisplayMapper.to_entity(dto.display) if dto.display else None
        playback = PlaybackMapper.to_entity(dto.playback) if dto.playback else None
        encoding = EncodingMapper.to_entity(dto.encoding) if dto.encoding else None
        color = ColorMapper.to_entity(dto.color) if dto.color else None

        return VideoEntity(
            id=dto.id,
            file=file,
            display=display,
            playback=playback,
            encoding=encoding,
            color=color,
        )

    @staticmethod
    def to_dto(entity: VideoEntity) -> Video:
        file_dto = FileMapper.to_dto(entity.file) if entity.file else None
        display_dto = DisplayMapper.to_dto(entity.display) if entity.display else None
        playback_dto = PlaybackMapper.to_dto(entity.playback) if entity.playback else None
        encoding_dto = EncodingMapper.to_dto(entity.encoding) if entity.encoding else None
        color_dto = ColorMapper.to_dto(entity.color) if entity.color else None

        return Video(
            id=entity.id,
            file=file_dto,
            display=display_dto,
            playback=playback_dto,
            encoding=encoding_dto,
            color=color_dto,
        )
