from pathlib import Path

from app.db.cache import JobStagesCache
from app.model.dto import Color, ColorRange, ColorStandard, Cpu, Display, EmbeddedMetadata, Encoding, Environment, \
    Evaluation, EvaluationMetric, ExecutionData, HdrFormat, Iteration, Job, JobStage, Playback, Segment, Video
from app.model.dto.file import File
from app.model.dto.segment_status import SegmentStatus
from app.model.entity.entities import ColorEntity, ColorRangesEntity, ColorStandardsEntity, CpuEntity, DisplayEntity, \
    EmbeddedMetadataEntity, EncodingEntity, EnvironmentEntity, EvaluationEntity, EvaluationMetricsEntity, \
    ExecutionDataEntity, FileEntity, HdrFormatEntity, IterationEntity, JobEntity, JobStagesEntity, PlaybackEntity, \
    SegmentEntity, SegmentStatusesEntity, VideoEntity
from app.model.mapper import DisplayMapper, EncodingMapper, FileMapper, PlaybackMapper
from app.model.mapper.abstract_mapper import AbstractMapper
from app.model.mapper.video_mapper import VideoMapper


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


class CpuMapper(AbstractMapper[Cpu, CpuEntity]):
    @staticmethod
    def to_entity(dto: Cpu) -> CpuEntity:
        return CpuEntity(
            id=dto.id,
            cpu_name=CpuMapper._get_required(dto.name, "cpu_name"),
            cpu_threads=CpuMapper._get_required(dto.threads, "cpu_threads"),
        )

    @staticmethod
    def to_dto(entity: CpuEntity) -> Cpu:
        return Cpu(
            id=entity.id,
            name=entity.cpu_name,
            threads=entity.cpu_threads,
        )


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


class EvaluationMapper(AbstractMapper[Evaluation, EvaluationEntity]):
    @staticmethod
    def to_entity(dto: Evaluation) -> EvaluationEntity:
        metric = EvaluationMetricMapper.to_entity(
            EvaluationMapper._get_required(dto.metric, "metric")
        )

        return EvaluationEntity(
            id=dto.id,
            metric=metric,
            score=EvaluationMapper._get_required(dto.score, "score"),
        )

    @staticmethod
    def to_dto(entity: EvaluationEntity) -> Evaluation:
        return Evaluation(
            id=entity.id,
            metric=EvaluationMetricMapper.to_dto(entity.metric) if entity.metric else None,
            score=entity.score,
        )


class EvaluationMetricMapper(AbstractMapper[EvaluationMetric, EvaluationMetricsEntity]):
    @staticmethod
    def to_entity(dto: EvaluationMetric) -> EvaluationMetricsEntity:
        return EvaluationMetricsEntity(
            id=dto.id,
            name=EvaluationMetricMapper._get_required(dto.name, "name"),
            version=dto.version
        )

    @staticmethod
    def to_dto(entity: EvaluationMetricsEntity) -> EvaluationMetric:
        return EvaluationMetric(
            id=entity.id,
            name=entity.name,
            version=entity.version
        )


class ExecutionDataMapper(AbstractMapper[ExecutionData, ExecutionDataEntity]):
    @staticmethod
    def to_entity(dto: ExecutionData) -> ExecutionDataEntity:
        encoding_cpu = CpuMapper.to_entity(
            ExecutionDataMapper._get_required(dto.encoding_cpu, "encoding_cpu")
        )
        evaluation_cpu = CpuMapper.to_entity(dto.evaluation_cpu) if dto.evaluation_cpu else None

        return ExecutionDataEntity(
            id=dto.id,
            ffmpeg_command_used=ExecutionDataMapper._get_required(
                dto.ffmpeg_command_used,
                "ffmpeg_command_used"
            ),
            finished_datetime_utc=ExecutionDataMapper._get_required(
                dto.finished_datetime_utc,
                "finished_datetime_utc"
            ),
            encoding_wall_time_seconds=ExecutionDataMapper._get_required(
                dto.encoding_wall_time_seconds,
                "encoding_wall_time_seconds"
            ),
            evaluation_wall_time_seconds=dto.evaluation_wall_time_seconds,
            encoding_cpu_time_seconds=ExecutionDataMapper._get_required(
                dto.encoding_cpu_time_seconds,
                "encoding_cpu_time_seconds"
            ),
            evaluation_cpu_time_seconds=dto.evaluation_cpu_time_seconds,
            total_wall_time_seconds=dto.total_wall_time_seconds,
            total_cpu_time_seconds=dto.total_cpu_time_seconds,
            encoding_cpu_threads_used=ExecutionDataMapper._get_required(
                dto.encoding_cpu_threads_used,
                "encoding_cpu_threads_used"
            ),
            evaluation_cpu_threads_used=dto.evaluation_cpu_threads_used,
            encoding_cpu=encoding_cpu,
            evaluation_cpu=evaluation_cpu
        )

    @staticmethod
    def to_dto(entity: ExecutionDataEntity) -> ExecutionData:
        return ExecutionData(
            id=entity.id,
            ffmpeg_command_used=entity.ffmpeg_command_used,
            finished_datetime_utc=entity.finished_datetime_utc,
            encoding_wall_time_seconds=entity.encoding_wall_time_seconds,
            evaluation_wall_time_seconds=entity.evaluation_wall_time_seconds,
            encoding_cpu_time_seconds=entity.encoding_cpu_time_seconds,
            evaluation_cpu_time_seconds=entity.evaluation_cpu_time_seconds,
            total_wall_time_seconds=entity.total_wall_time_seconds,
            total_cpu_time_seconds=entity.total_cpu_time_seconds,
            encoding_cpu_threads_used=entity.encoding_cpu_threads_used,
            evaluation_cpu_threads_used=entity.evaluation_cpu_threads_used,
            encoding_cpu=CpuMapper.to_dto(entity.encoding_cpu) if entity.encoding_cpu else None,
            evaluation_cpu=CpuMapper.to_dto(entity.evaluation_cpu) if entity.evaluation_cpu else None
        )


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


# Iteration mapper between these classes
class IterationMapper(AbstractMapper[Iteration, IterationEntity]):
    @staticmethod
    def to_entity(dto: Iteration) -> IterationEntity:
        pass

    @staticmethod
    def to_dto(entity: IterationEntity) -> Iteration:
        pass


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


class JobStageMapper(AbstractMapper[JobStage, JobStagesEntity]):
    @staticmethod
    def to_entity(dto: JobStage) -> JobStagesEntity:
        stage_id = JobStagesCache.get_instance().get_id(dto)
        return JobStagesEntity(id=stage_id, name=dto.name)

    @staticmethod
    def to_dto(entity: JobStagesEntity) -> JobStage:
        cache: JobStagesCache = JobStagesCache.get_instance()
        stage_entity_id = JobStageMapper._get_required(entity.id, "stage_entity.id")
        return cache.get_stage(stage_entity_id)


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


class SegmentMapper(AbstractMapper[Segment, SegmentEntity]):
    @staticmethod
    def to_entity(dto: Segment) -> SegmentEntity:
        job_entity = JobMapper.to_entity(
            SegmentMapper._get_required(dto.job, "job")
        )

        status_entity = SegmentStatusMapper.to_entity(
            SegmentMapper._get_required(dto.status, "status")
        )

        iteration_dtos = SegmentMapper._get_required(dto.iterations, "iterations")
        iteration_entities: list[IterationEntity] = []
        for iteration_dto in iteration_dtos:
            iteration_entities.append(IterationMapper.to_entity(iteration_dto))

        return SegmentEntity(
            id=dto.id,
            job=job_entity,
            from_frame=SegmentMapper._get_required(dto.from_frame, "from_frame"),
            to_frame=SegmentMapper._get_required(dto.to_frame, "to_frame"),
            status=status_entity,
            iterations=iteration_entities,
            total_time_seconds=dto.total_time_seconds
        )

    @staticmethod
    def to_dto(entity: SegmentEntity) -> Segment:
        job_dto = JobMapper.to_dto(entity.job)
        status_dto = SegmentStatusMapper.to_dto(entity.status)
        iteration_dtos: list[Iteration] = []
        for iteration_entity in entity.iterations:
            iteration_dtos.append(IterationMapper.to_dto(iteration_entity))

        return Segment(
            id=entity.id,
            job=job_dto,
            from_frame=entity.from_frame,
            to_frame=entity.to_frame,
            status=status_dto,
            iterations=iteration_dtos,
            total_time_seconds=entity.total_time_seconds
        )

class SegmentStatusMapper(AbstractMapper[SegmentStatus, SegmentStatusesEntity]):
    @staticmethod
    def to_entity(dto: SegmentStatus) -> SegmentStatusesEntity:
        pass

    @staticmethod
    def to_dto(entity: SegmentStatusesEntity) -> SegmentStatus:
        pass




class VideoMapper(AbstractMapper[Video, VideoEntity]):
    @staticmethod
    def to_entity(dto: Video) -> VideoEntity:
        pass

    @staticmethod
    def to_dto(entity: VideoEntity) -> Video:
        pass
