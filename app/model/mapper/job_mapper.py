from app.model.dto import Job, Segment
from app.model.entity.entities import JobEntity, SegmentEntity
from app.model.mapper import JobStageMapper, SegmentMapper, VideoMapper
from app.model.mapper.abstract_mapper import AbstractMapper


class JobMapper(AbstractMapper[Job, JobEntity]):
    @staticmethod
    def to_entity(dto: Job) -> JobEntity:
        source_video = VideoMapper.to_entity(
            JobMapper._get_required(dto.source_video, "source_video")
        )

        stage_enum = JobMapper._get_required(dto.stage, "stage")
        stage = JobStageMapper.to_entity(stage_enum)

        segment_dtos = JobMapper._get_required(dto.segments, "segments")
        segment_entities: list[SegmentEntity] = []
        for segment_dto in segment_dtos:
            segment_entities.append(SegmentMapper.to_entity(segment_dto))

        return JobEntity(
            id=dto.id,
            source_video=source_video,
            stage=stage,
            segments=segment_entities,
            created_datetime_utc=JobMapper._get_required(dto.created_datetime_utc, "created_datetime_utc"),
            total_time_seconds=dto.total_time_seconds
        )

    @staticmethod
    def to_dto(entity: JobEntity) -> Job:
        source_video_dto = VideoMapper.to_dto(entity.source_video)
        stage_dto = JobStageMapper.to_dto(entity.stage)
        segment_dtos: list[Segment] = []
        for segment_entity in entity.segments:
            segment_dtos.append(SegmentMapper.to_dto(segment_entity))

        return Job(
            id=entity.id,
            source_video=source_video_dto,
            stage=stage_dto,
            segments=segment_dtos,
            created_datetime_utc=entity.created_datetime_utc,
            total_time_seconds=entity.total_time_seconds
        )
