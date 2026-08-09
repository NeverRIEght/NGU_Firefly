from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, Float, ForeignKey, Integer, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class FileEntity(Base):
    __tablename__ = "file"
    id: Mapped[int | None] = mapped_column(primary_key=True)
    file_name: Mapped[str] = mapped_column(String, nullable=False)
    absolute_path: Mapped[str] = mapped_column(String, nullable=False)
    file_size_bytes: Mapped[int] = mapped_column(Integer, nullable=False)
    sha256_hash: Mapped[str | None] = mapped_column(String)


class DisplayEntity(Base):
    __tablename__ = "display"
    id: Mapped[int | None] = mapped_column(primary_key=True)
    width_px: Mapped[int] = mapped_column(Integer, nullable=False)
    height_px: Mapped[int] = mapped_column(Integer, nullable=False)
    display_aspect_ratio: Mapped[str] = mapped_column(String, nullable=False)
    pixel_aspect_ratio: Mapped[str] = mapped_column(String, nullable=False)
    pixel_format: Mapped[str] = mapped_column(String, nullable=False)
    chroma_sample_location: Mapped[str] = mapped_column(String, nullable=False)


class PlaybackEntity(Base):
    __tablename__ = "playback"
    id: Mapped[int | None] = mapped_column(primary_key=True)
    duration_seconds: Mapped[float] = mapped_column(Float, nullable=False)
    frames_counted: Mapped[int | None] = mapped_column(Integer)
    avg_frame_rate: Mapped[str | None] = mapped_column(String)
    r_frame_rate: Mapped[str | None] = mapped_column(String)


class EncodingEntity(Base):
    __tablename__ = "encoding"
    id: Mapped[int | None] = mapped_column(primary_key=True)
    codec: Mapped[str] = mapped_column(String, nullable=False)
    preset: Mapped[str | None] = mapped_column(String)
    encoder: Mapped[str | None] = mapped_column(String)


class CpuEntity(Base):
    __tablename__ = "cpu"
    id: Mapped[int | None] = mapped_column(primary_key=True)
    cpu_name: Mapped[str] = mapped_column(String, nullable=False)
    cpu_threads: Mapped[int] = mapped_column(Integer, nullable=False)


class EnvironmentEntity(Base):
    __tablename__ = "environment"
    id: Mapped[int | None] = mapped_column(primary_key=True)
    firefly_version: Mapped[str] = mapped_column(String, nullable=False)
    ffmpeg_version: Mapped[str] = mapped_column(String, nullable=False)
    compression_engine_version: Mapped[int] = mapped_column(Integer, nullable=False)


class EvaluationMetricsEntity(Base):
    __tablename__ = "evaluation_metrics"
    id: Mapped[int | None] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    version: Mapped[str | None] = mapped_column(String)


class JobStagesEntity(Base):
    __tablename__ = "job_stages"
    id: Mapped[int | None] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String, nullable=False)


class SegmentStatusesEntity(Base):
    __tablename__ = "segment_statuses"
    id: Mapped[int | None] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String, nullable=False)


class IterationStagesEntity(Base):
    __tablename__ = "iteration_stages"
    id: Mapped[int | None] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String, nullable=False)


class SchemaEntity(Base):
    __tablename__ = "schema"
    version: Mapped[int] = mapped_column(Integer, primary_key=True)


class HdrFormatEntity(Base):
    __tablename__ = "hdr_format"
    id: Mapped[int | None] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String, nullable=False)


class ColorStandardsEntity(Base):
    __tablename__ = "color_standards"
    id: Mapped[int | None] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String, nullable=False)


class ColorRangesEntity(Base):
    __tablename__ = "color_ranges"
    id: Mapped[int | None] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String, nullable=False)


class ColorEntity(Base):
    __tablename__ = "color"
    id: Mapped[int | None] = mapped_column(primary_key=True)
    hdr_format_id: Mapped[int | None] = mapped_column(ForeignKey("hdr_format.id"))
    color_primaries_id: Mapped[int | None] = mapped_column(ForeignKey("color_standards.id"))
    color_trc_id: Mapped[int | None] = mapped_column(ForeignKey("color_standards.id"))
    colorspace_id: Mapped[int | None] = mapped_column(ForeignKey("color_standards.id"))
    color_range_id: Mapped[int | None] = mapped_column(ForeignKey("color_ranges.id"))
    max_cll: Mapped[str | None] = mapped_column(String)
    master_display: Mapped[str | None] = mapped_column(String)
    dovi_profile: Mapped[str | None] = mapped_column(String)

    hdr_format: Mapped[HdrFormatEntity | None] = relationship()
    color_primaries: Mapped[ColorStandardsEntity | None] = relationship(
        foreign_keys=[color_primaries_id]
    )
    color_trc: Mapped[ColorStandardsEntity | None] = relationship(
        foreign_keys=[color_trc_id]
    )
    colorspace: Mapped[ColorStandardsEntity | None] = relationship(
        foreign_keys=[colorspace_id]
    )
    color_range: Mapped[ColorRangesEntity | None] = relationship(
        foreign_keys=[color_range_id]
    )


class EmbeddedMetadataEntity(Base):
    __tablename__ = "embedded_metadata"
    id: Mapped[int | None] = mapped_column(primary_key=True)
    encodes_count: Mapped[int] = mapped_column(Integer, nullable=False)
    last_encode_datetime_utc: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    source_video_sha256_hash: Mapped[str] = mapped_column(String, nullable=False)
    environment_id: Mapped[int] = mapped_column(ForeignKey("environment.id"), nullable=False)

    environment: Mapped[EnvironmentEntity] = relationship()


class VideoEntity(Base):
    __tablename__ = "video"
    id: Mapped[int | None] = mapped_column(primary_key=True)
    file_id: Mapped[int | None] = mapped_column(ForeignKey("file.id"))
    display_id: Mapped[int | None] = mapped_column(ForeignKey("display.id"))
    playback_id: Mapped[int | None] = mapped_column(ForeignKey("playback.id"))
    encoding_id: Mapped[int | None] = mapped_column(ForeignKey("encoding.id"))
    color_id: Mapped[int | None] = mapped_column(ForeignKey("color.id"))
    embedded_metadata_id: Mapped[int | None] = mapped_column(ForeignKey("embedded_metadata.id"))

    file: Mapped[FileEntity | None] = relationship()
    display: Mapped[DisplayEntity | None] = relationship()
    playback: Mapped[PlaybackEntity | None] = relationship()
    encoding: Mapped[EncodingEntity | None] = relationship()
    color: Mapped[ColorEntity | None] = relationship()
    embedded_metadata: Mapped[EmbeddedMetadataEntity | None] = relationship()


class JobEntity(Base):
    __tablename__ = "job"
    id: Mapped[int | None] = mapped_column(primary_key=True)
    source_video_id: Mapped[int] = mapped_column(ForeignKey("video.id"), nullable=False)
    stage_id: Mapped[int] = mapped_column(ForeignKey("job_stages.id"), nullable=False)
    created_datetime_utc: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    total_time_seconds: Mapped[float | None] = mapped_column(Float)

    source_video: Mapped[VideoEntity] = relationship()
    stage: Mapped[JobStagesEntity] = relationship()
    segments: Mapped[list[SegmentEntity]] = relationship(
        back_populates="job", cascade="all, delete-orphan"
    )


class SegmentEntity(Base):
    __tablename__ = "segments"
    id: Mapped[int | None] = mapped_column(primary_key=True)
    job_id: Mapped[int] = mapped_column(ForeignKey("job.id"), nullable=False)
    from_frame: Mapped[int] = mapped_column(Integer, nullable=False)
    to_frame: Mapped[int] = mapped_column(Integer, nullable=False)
    status_id: Mapped[int] = mapped_column(ForeignKey("segment_statuses.id"), nullable=False)
    total_time_seconds: Mapped[float | None] = mapped_column(Float)

    job: Mapped[JobEntity] = relationship(back_populates="segments")
    status: Mapped[SegmentStatusesEntity] = relationship()
    iterations: Mapped[list[IterationEntity]] = relationship(
        back_populates="segment", cascade="all, delete-orphan"
    )


class ExecutionDataEntity(Base):
    __tablename__ = "execution_data"
    id: Mapped[int | None] = mapped_column(primary_key=True)
    ffmpeg_command_used: Mapped[str] = mapped_column(String, nullable=False)
    finished_datetime_utc: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    encoding_wall_time_seconds: Mapped[float] = mapped_column(Float, nullable=False)
    evaluation_wall_time_seconds: Mapped[float | None] = mapped_column(Float)
    encoding_cpu_time_seconds: Mapped[float] = mapped_column(Float, nullable=False)
    evaluation_cpu_time_seconds: Mapped[float | None] = mapped_column(Float)
    total_wall_time_seconds: Mapped[float | None] = mapped_column(Float)
    total_cpu_time_seconds: Mapped[float | None] = mapped_column(Float)
    encoding_cpu_threads_used: Mapped[int] = mapped_column(Integer, nullable=False)
    evaluation_cpu_threads_used: Mapped[int | None] = mapped_column(Integer)
    encoding_cpu_id: Mapped[int] = mapped_column(ForeignKey("cpu.id"), nullable=False)
    evaluation_cpu_id: Mapped[int | None] = mapped_column(ForeignKey("cpu.id"))

    encoding_cpu: Mapped[CpuEntity] = relationship(foreign_keys=[encoding_cpu_id])
    evaluation_cpu: Mapped[CpuEntity | None] = relationship(foreign_keys=[evaluation_cpu_id])


class IterationEntity(Base):
    __tablename__ = "iteration"
    id: Mapped[int | None] = mapped_column(primary_key=True)
    segment_id: Mapped[int] = mapped_column(ForeignKey("segments.id"), nullable=False)
    stage_id: Mapped[int] = mapped_column(ForeignKey("iteration_stages.id"), nullable=False)
    video_id: Mapped[int] = mapped_column(ForeignKey("video.id"), nullable=False)
    environment_id: Mapped[int] = mapped_column(ForeignKey("environment.id"), nullable=False)
    execution_data_id: Mapped[int] = mapped_column(ForeignKey("execution_data.id"), nullable=False)
    crf: Mapped[int] = mapped_column(Integer, nullable=False)

    segment: Mapped[SegmentEntity] = relationship(back_populates="iterations")
    stage: Mapped[IterationStagesEntity] = relationship()
    video: Mapped[VideoEntity] = relationship()
    environment: Mapped[EnvironmentEntity] = relationship()
    execution_data: Mapped[ExecutionDataEntity] = relationship()
    evaluations: Mapped[list[EvaluationEntity]] = relationship(
        back_populates="iteration", cascade="all, delete-orphan"
    )


class EvaluationEntity(Base):
    __tablename__ = "evaluation"
    id: Mapped[int | None] = mapped_column(primary_key=True)
    iteration_id: Mapped[int] = mapped_column(ForeignKey("iteration.id"), nullable=False)
    metric_id: Mapped[int] = mapped_column(ForeignKey("evaluation_metrics.id"), nullable=False)
    score: Mapped[float] = mapped_column(Float, nullable=False)

    iteration: Mapped[IterationEntity] = relationship(back_populates="evaluations")
    metric: Mapped[EvaluationMetricsEntity] = relationship()
