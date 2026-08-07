from app.model.dto import Playback
from app.model.entity.entities import PlaybackEntity
from app.model.mapper.abstract_mapper import AbstractMapper


class PlaybackMapper(AbstractMapper[Playback, PlaybackEntity]):
    @staticmethod
    def to_entity(dto: Playback) -> PlaybackEntity:
        return PlaybackEntity(
            id=dto.id,
            duration_seconds=PlaybackMapper._get_required(dto.duration_seconds, "duration_seconds"),
            frames_counted=dto.frames_counted,
            avg_frame_rate=dto.avg_frame_rate,
            r_frame_rate=dto.r_frame_rate
        )

    @staticmethod
    def to_dto(entity: PlaybackEntity) -> Playback:
        return Playback(
            id=entity.id,
            duration_seconds=entity.duration_seconds,
            frames_counted=entity.frames_counted,
            avg_frame_rate=entity.avg_frame_rate,
            r_frame_rate=entity.r_frame_rate
        )
