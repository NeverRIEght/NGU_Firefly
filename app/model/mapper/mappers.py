from pathlib import Path

from app.db.cache import LookupCacheRegistry
from app.model.dto import (
    Color,
    Cpu,
    Display,
    EmbeddedMetadata,
    Encoding,
    Environment,
    Evaluation,
    EvaluationMetric,
    ExecutionData,
    File,
    Iteration,
    Job,
    Playback,
    Segment,
    Video,
)
from app.model.entity.entities import (
    ColorEntity,
    CpuEntity,
    DisplayEntity,
    EmbeddedMetadataEntity,
    EncodingEntity,
    EnvironmentEntity,
    EvaluationEntity,
    EvaluationMetricsEntity,
    ExecutionDataEntity,
    FileEntity,
    IterationEntity,
    JobEntity,
    PlaybackEntity,
    SegmentEntity,
    VideoEntity,
)
from app.model.mapper.abstract_mapper import AbstractMapper


class ColorMapper(AbstractMapper[Color, ColorEntity]):
    @staticmethod
    def to_entity(dto: Color) -> ColorEntity:
        cache = LookupCacheRegistry.get_instance()

        return ColorEntity(
            id=dto.id,
            hdr_format_id=(
                cache.hdr_formats.get_id(dto.hdr_format)
                if dto.hdr_format is not None
                else None
            ),
            color_primaries_id=(
                cache.color_standards.get_id(dto.color_primaries)
                if dto.color_primaries is not None
                else None
            ),
            color_trc_id=(
                cache.color_standards.get_id(dto.color_trc)
                if dto.color_trc is not None
                else None
            ),
            colorspace_id=(
                cache.color_standards.get_id(dto.colorspace)
                if dto.colorspace is not None
                else None
            ),
            color_range_id=(
                cache.color_ranges.get_id(dto.color_range)
                if dto.color_range is not None
                else None
            ),
            max_cll=dto.max_cll,
            master_display=dto.master_display,
            dovi_profile=dto.dovi_profile,
        )

    @staticmethod
    def to_dto(entity: ColorEntity) -> Color:
        cache = LookupCacheRegistry.get_instance()
        return Color(
            id=entity.id,
            hdr_format=(
                cache.hdr_formats.get_enum(entity.hdr_format_id)
                if entity.hdr_format_id is not None
                else None
            ),
            color_primaries=(
                cache.color_standards.get_enum(entity.color_primaries_id)
                if entity.color_primaries_id is not None
                else None
            ),
            color_trc=(
                cache.color_standards.get_enum(entity.color_trc_id)
                if entity.color_trc_id is not None
                else None
            ),
            colorspace=(
                cache.color_standards.get_enum(entity.colorspace_id)
                if entity.colorspace_id is not None
                else None
            ),
            color_range=(
                cache.color_ranges.get_enum(entity.color_range_id)
                if entity.color_range_id is not None
                else None
            ),
            max_cll=entity.max_cll,
            master_display=entity.master_display,
            dovi_profile=entity.dovi_profile,
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
            width_px=DisplayMapper._get_required(
                dto.width_px, "width_px"
            ),
            height_px=DisplayMapper._get_required(
                dto.height_px, "height_px"
            ),
            display_aspect_ratio=DisplayMapper._get_required(
                dto.display_aspect_ratio, "display_aspect_ratio"
            ),
            pixel_aspect_ratio=DisplayMapper._get_required(
                dto.pixel_aspect_ratio, "pixel_aspect_ratio"
            ),
            pixel_format=DisplayMapper._get_required(
                dto.pixel_format, "pixel_format"
            ),
            chroma_sample_location=DisplayMapper._get_required(
                dto.chroma_sample_location, "chroma_sample_location"
            ),
        )

    @staticmethod
    def to_dto(entity: DisplayEntity) -> Display:
        return Display(
            id=entity.id,
            width_px=entity.width_px,
            height_px=entity.height_px,
            display_aspect_ratio=entity.display_aspect_ratio,
            pixel_aspect_ratio=entity.pixel_aspect_ratio,
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
                dto.encodes_count, "encodes_count"
            ),
            last_encode_datetime_utc=EmbeddedMetadataMapper._get_required(
                dto.last_encode_datetime, "last_encode_datetime"
            ),
            source_video_sha256_hash=EmbeddedMetadataMapper._get_required(
                dto.source_video_sha256_hash, "source_video_sha256_hash"
            ),
            environment=environment,
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
                dto.compression_engine_version, "compression_engine_version"
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
                dto.ffmpeg_command_used, "ffmpeg_command_used"
            ),
            finished_datetime_utc=ExecutionDataMapper._get_required(
                dto.finished_datetime_utc, "finished_datetime_utc"
            ),
            encoding_wall_time_seconds=ExecutionDataMapper._get_required(
                dto.encoding_wall_time_seconds, "encoding_wall_time_seconds"
            ),
            evaluation_wall_time_seconds=dto.evaluation_wall_time_seconds,
            encoding_cpu_time_seconds=ExecutionDataMapper._get_required(
                dto.encoding_cpu_time_seconds, "encoding_cpu_time_seconds"
            ),
            evaluation_cpu_time_seconds=dto.evaluation_cpu_time_seconds,
            total_wall_time_seconds=dto.total_wall_time_seconds,
            total_cpu_time_seconds=dto.total_cpu_time_seconds,
            encoding_cpu_threads_used=ExecutionDataMapper._get_required(
                dto.encoding_cpu_threads_used, "encoding_cpu_threads_used"
            ),
            evaluation_cpu_threads_used=dto.evaluation_cpu_threads_used,
            encoding_cpu=encoding_cpu,
            evaluation_cpu=evaluation_cpu,
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
        absolute_path = FileMapper._get_required(dto.absolute_path, "absolute_path")
        return FileEntity(
            id=dto.id,
            file_name=FileMapper._get_required(dto.file_name, "file_name"),
            absolute_path=str(absolute_path),
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


class IterationMapper(AbstractMapper[Iteration, IterationEntity]):
    @staticmethod
    def to_entity(dto: Iteration) -> IterationEntity:
        stage_enum = IterationMapper._get_required(dto.stage, "stage")
        stage_id = LookupCacheRegistry.get_instance().iteration_stages.get_id(stage_enum)
        video = VideoMapper.to_entity(
            IterationMapper._get_required(dto.video, "video")
        )
        environment = EnvironmentMapper.to_entity(
            IterationMapper._get_required(dto.environment, "environment")
        )
        execution_data = ExecutionDataMapper.to_entity(
            IterationMapper._get_required(dto.execution_data, "execution_data")
        )

        evaluation_entities: list[EvaluationEntity] = []
        if dto.evaluations:
            for eval_dto in dto.evaluations:
                evaluation_entities.append(EvaluationMapper.to_entity(eval_dto))

        return IterationEntity(
            id=dto.id,
            stage_id=stage_id,
            video=video,
            environment=environment,
            execution_data=execution_data,
            crf=IterationMapper._get_required(dto.crf, "crf"),
            evaluations=evaluation_entities,
        )

    @staticmethod
    def to_dto(entity: IterationEntity) -> Iteration:
        stage_dto = (
            LookupCacheRegistry.get_instance().iteration_stages.get_enum(entity.stage_id)
            if entity.stage_id is not None
            else None
        )
        video_dto = VideoMapper.to_dto(entity.video) if entity.video else None
        environment_dto = EnvironmentMapper.to_dto(entity.environment) if entity.environment else None
        execution_data_dto = (
            ExecutionDataMapper.to_dto(entity.execution_data) if entity.execution_data else None
        )

        evaluation_dtos: list[Evaluation] = []
        if entity.evaluations:
            for eval_entity in entity.evaluations:
                evaluation_dtos.append(EvaluationMapper.to_dto(eval_entity))

        return Iteration(
            id=entity.id,
            stage=stage_dto,
            video=video_dto,
            environment=environment_dto,
            execution_data=execution_data_dto,
            crf=entity.crf,
            evaluations=evaluation_dtos,
        )


class JobMapper(AbstractMapper[Job, JobEntity]):
    @staticmethod
    def to_entity(dto: Job) -> JobEntity:
        source_video = VideoMapper.to_entity(
            JobMapper._get_required(dto.source_video, "source_video")
        )
        stage_enum = JobMapper._get_required(dto.stage, "stage")
        stage_id = LookupCacheRegistry.get_instance().job_stages.get_id(stage_enum)

        segment_entities: list[SegmentEntity] = []
        if dto.segments:
            for segment_dto in dto.segments:
                segment_entities.append(SegmentMapper.to_entity(segment_dto))

        return JobEntity(
            id=dto.id,
            source_video=source_video,
            stage_id=stage_id,
            segments=segment_entities,
            created_datetime_utc=JobMapper._get_required(
                dto.created_datetime_utc, "created_datetime_utc"
            ),
            total_time_seconds=dto.total_time_seconds
        )

    @staticmethod
    def to_dto(entity: JobEntity) -> Job:
        source_video_dto = VideoMapper.to_dto(entity.source_video) if entity.source_video else None
        stage_dto = (
            LookupCacheRegistry.get_instance().job_stages.get_enum(entity.stage_id)
            if entity.stage_id is not None
            else None
        )

        segment_dtos: list[Segment] = []
        if entity.segments:
            for segment_entity in entity.segments:
                segment_dtos.append(SegmentMapper.to_dto(segment_entity))

        return Job(
            id=entity.id,
            source_video=source_video_dto,
            stage=stage_dto,
            segments=segment_dtos,
            created_datetime_utc=entity.created_datetime_utc,
            total_time_seconds=entity.total_time_seconds,
        )


class PlaybackMapper(AbstractMapper[Playback, PlaybackEntity]):
    @staticmethod
    def to_entity(dto: Playback) -> PlaybackEntity:
        return PlaybackEntity(
            id=dto.id,
            duration_seconds=PlaybackMapper._get_required(
                dto.duration_seconds, "duration_seconds"
            ),
            frames_counted=dto.frames_counted,
            avg_frame_rate=dto.avg_frame_rate,
            r_frame_rate=dto.r_frame_rate,
        )

    @staticmethod
    def to_dto(entity: PlaybackEntity) -> Playback:
        return Playback(
            id=entity.id,
            duration_seconds=entity.duration_seconds,
            frames_counted=entity.frames_counted,
            avg_frame_rate=entity.avg_frame_rate,
            r_frame_rate=entity.r_frame_rate,
        )


class SegmentMapper(AbstractMapper[Segment, SegmentEntity]):
    @staticmethod
    def to_entity(dto: Segment) -> SegmentEntity:
        status_enum = SegmentMapper._get_required(dto.status, "status")
        status_id = LookupCacheRegistry.get_instance().segment_statuses.get_id(status_enum)

        iteration_entities: list[IterationEntity] = []
        if dto.iterations:
            for iteration_dto in dto.iterations:
                iteration_entities.append(IterationMapper.to_entity(iteration_dto))

        return SegmentEntity(
            id=dto.id,
            from_frame=SegmentMapper._get_required(dto.from_frame, "from_frame"),
            to_frame=SegmentMapper._get_required(dto.to_frame, "to_frame"),
            status_id=status_id,
            iterations=iteration_entities,
            total_time_seconds=dto.total_time_seconds,
        )

    @staticmethod
    def to_dto(entity: SegmentEntity) -> Segment:
        status_dto = (
            LookupCacheRegistry.get_instance().segment_statuses.get_enum(entity.status_id)
            if entity.status_id is not None
            else None
        )

        iteration_dtos: list[Iteration] = []
        if entity.iterations:
            for iteration_entity in entity.iterations:
                iteration_dtos.append(IterationMapper.to_dto(iteration_entity))

        return Segment(
            id=entity.id,
            from_frame=entity.from_frame,
            to_frame=entity.to_frame,
            status=status_dto,
            iterations=iteration_dtos,
            total_time_seconds=entity.total_time_seconds,
        )


class VideoMapper(AbstractMapper[Video, VideoEntity]):
    @staticmethod
    def to_entity(dto: Video) -> VideoEntity:
        file = FileMapper.to_entity(dto.file) if dto.file else None
        display = DisplayMapper.to_entity(dto.display) if dto.display else None
        playback = PlaybackMapper.to_entity(dto.playback) if dto.playback else None
        encoding = EncodingMapper.to_entity(dto.encoding) if dto.encoding else None
        color = ColorMapper.to_entity(dto.color) if dto.color else None
        embedded_metadata = (
            EmbeddedMetadataMapper.to_entity(dto.embedded_metadata)
            if dto.embedded_metadata
            else None
        )

        return VideoEntity(
            id=dto.id,
            file=file,
            display=display,
            playback=playback,
            encoding=encoding,
            color=color,
            embedded_metadata=embedded_metadata,
        )

    @staticmethod
    def to_dto(entity: VideoEntity) -> Video:
        file_dto = FileMapper.to_dto(entity.file) if entity.file else None
        display_dto = DisplayMapper.to_dto(entity.display) if entity.display else None
        playback_dto = PlaybackMapper.to_dto(entity.playback) if entity.playback else None
        encoding_dto = EncodingMapper.to_dto(entity.encoding) if entity.encoding else None
        color_dto = ColorMapper.to_dto(entity.color) if entity.color else None
        embedded_metadata_dto = (
            EmbeddedMetadataMapper.to_dto(entity.embedded_metadata)
            if entity.embedded_metadata
            else None
        )

        return Video(
            id=entity.id,
            file=file_dto,
            display=display_dto,
            playback=playback_dto,
            encoding=encoding_dto,
            color=color_dto,
            embedded_metadata=embedded_metadata_dto,
        )
