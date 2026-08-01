from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class FileEntity(Base):
    __tablename__ = "file"
    id = Column(Integer, primary_key=True)
    file_name = Column(String, nullable=False)
    absolute_path = Column(String, nullable=False)
    file_size_bytes = Column(Integer, nullable=False)
    sha256_hash = Column(String)

class DisplayEntity(Base):
    __tablename__ = "display"
    id = Column(Integer, primary_key=True)
    width_px = Column(Integer, nullable=False)
    height_px = Column(Integer, nullable=False)
    display_aspect_ratio = Column(String, nullable=False)
    pixel_aspect_ratio = Column(String, nullable=False)
    pixel_format = Column(String, nullable=False)
    chroma_sample_location = Column(String, nullable=False)

class PlaybackEntity(Base):
    __tablename__ = "playback"
    id = Column(Integer, primary_key=True)
    duration_seconds = Column(Float, nullable=False)
    frames_counted = Column(Integer)
    avg_frame_rate = Column(String)
    r_frame_rate = Column(String)

class EncodingEntity(Base):
    __tablename__ = "encoding"
    id = Column(Integer, primary_key=True)
    codec = Column(String, nullable=False)
    preset = Column(String)
    encoder = Column(String)

class CpuEntity(Base):
    __tablename__ = "cpu"
    id = Column(Integer, primary_key=True)
    cpu_name = Column(String, nullable=False)
    cpu_threads = Column(Integer, nullable=False)

class EnvironmentEntity(Base):
    __tablename__ = "environment"
    id = Column(Integer, primary_key=True)
    firefly_version = Column(String, nullable=False)
    ffmpeg_version = Column(String, nullable=False)
    compression_engine_version = Column(Integer, nullable=False)

class EvaluationMetricsEntity(Base):
    __tablename__ = "evaluation_metrics"
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    version = Column(String)

class JobStagesEntity(Base):
    __tablename__ = "job_stages"
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)

class SegmentStatusesEntity(Base):
    __tablename__ = "segment_statuses"
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)

class IterationStagesEntity(Base):
    __tablename__ = "iteration_stages"
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)

class SchemaEntity(Base):
    __tablename__ = "schema"
    version = Column(Integer, primary_key=True)

class ColorEntity(Base):
    __tablename__ = "color"
    id = Column(Integer, primary_key=True)
    hdr_format_id = Column(Integer, ForeignKey("hdr_format.id"))
    color_primaries_id = Column(Integer, ForeignKey("color_standards.id"))
    color_trc_id = Column(Integer, ForeignKey("color_standards.id"))
    colorspace_id = Column(Integer, ForeignKey("color_standards.id"))
    color_range_id = Column(Integer, ForeignKey("color_ranges.id"))
    max_cll = Column(String)
    master_display = Column(String)
    dovi_profile = Column(String)

class HdrFormatEntity(Base):
    __tablename__ = "hdr_format"
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)

class ColorStandardsEntity(Base):
    __tablename__ = "color_standards"
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)

class ColorRangesEntity(Base):
    __tablename__ = "color_ranges"
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)

class EmbeddedMetadataEntity(Base):
    __tablename__ = "embedded_metadata"
    id = Column(Integer, primary_key=True)
    encodes_count = Column(Integer, nullable=False)
    last_encode_datetime_utc = Column(DateTime, nullable=False)
    source_video_sha256_hash = Column(String, nullable=False)
    environment_id = Column(Integer, ForeignKey("environment.id"), nullable=False)

class JobEntity(Base):
    __tablename__ = "job"
    id = Column(Integer, primary_key=True)
    source_video_id = Column(Integer, ForeignKey("video.id"), nullable=False)
    stage_id = Column(Integer, ForeignKey("job_stages.id"), nullable=False)
    created_datetime_utc = Column(DateTime, nullable=False)
    total_time_seconds = Column(Float)

class VideoEntity(Base):
    __tablename__ = "video"
    id = Column(Integer, primary_key=True)
    file_id = Column(Integer, ForeignKey("file.id"))
    display_id = Column(Integer, ForeignKey("display.id"))
    playback_id = Column(Integer, ForeignKey("playback.id"))
    encoding_id = Column(Integer, ForeignKey("encoding.id"))
    color_id = Column(Integer, ForeignKey("color.id"))
    embedded_metadata_id = Column(Integer, ForeignKey("embedded_metadata.id"))

class SegmentEntity(Base):
    __tablename__ = "segments"
    id = Column(Integer, primary_key=True)
    job_id = Column(Integer, ForeignKey("job.id"), nullable=False)
    from_frame = Column(Integer, nullable=False)
    to_frame = Column(Integer, nullable=False)
    status_id = Column(Integer, ForeignKey("segment_statuses.id"), nullable=False)
    total_time_seconds = Column(Float)

class IterationEntity(Base):
    __tablename__ = "iteration"
    id = Column(Integer, primary_key=True)
    segment_id = Column(Integer, ForeignKey("segments.id"), nullable=False)
    stage_id = Column(Integer, ForeignKey("iteration_stages.id"), nullable=False)
    video_id = Column(Integer, ForeignKey("video.id"), nullable=False)
    cpu_id = Column(Integer, ForeignKey("cpu.id"), nullable=False)
    environment_id = Column(Integer, ForeignKey("environment.id"), nullable=False)
    execution_data_id = Column(Integer, ForeignKey("execution_data.id"), nullable=False)
    crf = Column(Integer, nullable=False)

class ExecutionDataEntity(Base):
    __tablename__ = "execution_data"
    id = Column(Integer, primary_key=True)
    ffmpeg_command_used = Column(String, nullable=False)
    finished_datetime_utc = Column(DateTime, nullable=False)
    encoding_time_seconds = Column(Float, nullable=False)
    evaluation_time_seconds = Column(Float)
    total_time_seconds = Column(Float)
    encoding_cpu_threads_used = Column(Integer, nullable=False)
    evaluation_cpu_threads_used = Column(Integer)

class EvaluationEntity(Base):
    __tablename__ = "evaluation"
    id = Column(Integer, primary_key=True)
    iteration_id = Column(Integer, ForeignKey("iteration.id"), nullable=False)
    metric_id = Column(Integer, ForeignKey("evaluation_metrics.id"), nullable=False)
    score = Column(Float, nullable=False)
