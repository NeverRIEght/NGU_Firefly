from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import List, Optional

from pydantic import BaseModel, Field


class ColorRange(str, Enum):
    UNKNOWN = "unknown"
    TV = "tv"
    PC = "pc"


class ColorPrimaries(str, Enum):
    UNKNOWN = "unknown"
    BT709 = "bt709"
    BT2020 = "bt2020"
    SMPTE170M = "smpte170m"
    SMPTE431 = "smpte431"
    SMPTE432 = "smpte432"


class ColorTransfer(str, Enum):
    UNKNOWN = "unknown"
    BT709 = "bt709"
    SMPTE2084 = "smpte2084"
    ARIB_STD_B67 = "arib-std-b67"
    SMPTE170M = "smpte170m"
    IEC61966_2_1 = "iec61966-2-1"
    LINEAR = "linear"


class ColorSpace(str, Enum):
    UNKNOWN = "unknown"
    BT709 = "bt709"
    BT2020NC = "bt2020nc"
    BT2020C = "bt2020c"
    SMPTE170M = "smpte170m"
    ICTCP = "ictcp"


class HdrFormat(str, Enum):
    SDR = "sdr"
    HDR10 = "hdr10"
    HDR10_PLUS = "hdr10plus"
    HLG = "hlg"
    DOLBY_VISION = "dolby_vision"


class IterationStage(str, Enum):
    DEFINED = "defined"
    IN_PROGRESS = "in_progress"
    FAILED = "failed"
    COMPLETED = "completed"
    MERGED = "merged"


class JobStage(str, Enum):
    CREATED = "created"
    METADATA_EXTRACTION = "metadata_extraction"
    FILTERED_OUT = "filtered_out"
    IN_PROGRESS = "in_progress"
    FAILED = "failed"
    COMPLETED = "completed"


class SegmentStatus(str, Enum):
    DEFINED = "defined"
    IN_PROGRESS = "in_progress"
    FAILED = "failed"
    COMPLETED = "completed"


class Cpu(BaseModel):
    id: Optional[int] = None
    name: Optional[str] = None
    threads: Optional[int] = None


class Display(BaseModel):
    id: Optional[int] = None
    width_px: Optional[int] = None
    height_px: Optional[int] = None
    display_aspect_ratio: Optional[str] = None
    pixel_aspect_ratio: Optional[str] = None
    pixel_format: Optional[str] = None
    chroma_sample_location: Optional[str] = None


class Encoding(BaseModel):
    id: Optional[int] = None
    codec: Optional[str] = None
    preset: Optional[str] = None
    encoder: Optional[str] = None
    average_bitrate_kilobits_per_second: Optional[float] = None


class Environment(BaseModel):
    id: Optional[int] = None
    firefly_version: Optional[str] = None
    ffmpeg_version: Optional[str] = None
    compression_engine_version: Optional[int] = None


class EvaluationMetric(BaseModel):
    id: Optional[int] = None
    name: Optional[str] = None
    version: Optional[str] = None


class File(BaseModel):
    id: Optional[int] = None
    file_name: Optional[str] = None
    absolute_path: Optional[Path] = None
    file_size_bytes: Optional[int] = None
    sha256_hash: Optional[str] = None
    file_mtime_nanoseconds: Optional[int] = None


class Playback(BaseModel):
    id: Optional[int] = None
    duration_seconds: Optional[float] = None
    frames_counted: Optional[int] = None
    avg_frame_rate: Optional[str] = None
    r_frame_rate: Optional[str] = None


class Color(BaseModel):
    id: Optional[int] = None
    hdr_format: Optional[HdrFormat] = None
    color_primaries: Optional[ColorPrimaries] = None
    color_trc: Optional[ColorTransfer] = None
    colorspace: Optional[ColorSpace] = None
    color_range: Optional[ColorRange] = None
    max_cll: Optional[str] = None
    master_display: Optional[str] = None
    dovi_profile: Optional[str] = None


class EmbeddedMetadata(BaseModel):
    id: Optional[int] = None
    encodes_count: Optional[int] = None
    last_encode_datetime: Optional[datetime] = None
    source_video_sha256_hash: Optional[str] = None
    environment: Optional[Environment] = None


class Evaluation(BaseModel):
    id: Optional[int] = None
    metric: Optional[EvaluationMetric] = None
    score: Optional[float] = None


class ExecutionData(BaseModel):
    id: Optional[int] = None
    ffmpeg_command_used: Optional[str] = None
    finished_datetime_utc: Optional[datetime] = None
    encoding_wall_time_seconds: Optional[float] = None
    evaluation_wall_time_seconds: Optional[float] = None
    encoding_cpu_time_seconds: Optional[float] = None
    evaluation_cpu_time_seconds: Optional[float] = None
    total_wall_time_seconds: Optional[float] = None
    total_cpu_time_seconds: Optional[float] = None
    encoding_cpu_threads_used: Optional[int] = None
    evaluation_cpu_threads_used: Optional[int] = None
    encoding_cpu: Optional[Cpu] = None
    evaluation_cpu: Optional[Cpu] = None
    is_legacy_import: bool = False


class Video(BaseModel):
    id: Optional[int] = None
    file: Optional[File] = None
    display: Optional[Display] = None
    playback: Optional[Playback] = None
    encoding: Optional[Encoding] = None
    color: Optional[Color] = None
    embedded_metadata: Optional[EmbeddedMetadata] = None


class Iteration(BaseModel):
    id: Optional[int] = None
    stage: Optional[IterationStage] = None
    video: Optional[Video] = None
    environment: Optional[Environment] = None
    execution_data: Optional[ExecutionData] = None
    crf: Optional[int] = None
    evaluations: List[Evaluation] = Field(default_factory=list)


class Segment(BaseModel):
    id: Optional[int] = None
    from_frame: Optional[int] = None
    to_frame: Optional[int] = None
    status: Optional[SegmentStatus] = None
    total_time_seconds: Optional[float] = None
    iterations: List[Iteration] = Field(default_factory=list)


class Job(BaseModel):
    id: Optional[int] = None
    source_video: Optional[Video] = None
    output_video: Optional[Video] = None
    stage: Optional[JobStage] = None
    created_datetime_utc: Optional[datetime] = None
    is_legacy_import: bool = False
    priority: float = 1.0
    total_time_seconds: Optional[float] = None
    segments: List[Segment] = Field(default_factory=list)
