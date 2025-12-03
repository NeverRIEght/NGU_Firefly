from pathlib import Path
from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime


class Resolution(BaseModel):
    width_px: int = Field(..., description="Width in pixels")
    height_px: int = Field(..., description="Height in pixels")


class FfmpegMetadata(BaseModel):
    pixel_aspect_ratio: Optional[str] = None
    pixel_format: Optional[str] = None
    chroma_sample_location: Optional[str] = None
    color_primaries: Optional[str] = None
    color_trc: Optional[str] = None
    colorspace: Optional[str] = None
    profile: Optional[str] = None
    level: Optional[str] = None


class EncoderSettings(BaseModel):
    encoder: str
    preset: str
    crf: int
    pools: int


class Environment(BaseModel):
    script_version: str
    ffmpeg_version: str
    encoder_version: str
    cpu_name: str
    cpu_threads: int


class SourceVideo(BaseModel):
    file_name: str
    file_size_megabytes: float
    resolution: Resolution
    video_duration_seconds: float
    codec: str
    average_bitrate_kilobits_per_second: float
    fps: float
    actual_frame_count: int  # Can be calculated: fps * duration
    sha256_hash: str
    ffmpeg_metadata: Optional[FfmpegMetadata] = None


class EncodingStage(BaseModel):
    stage_number_from_1: int
    stage_name: str
    crf_range_min: Optional[int] = -1
    crf_range_max: Optional[int] = -1
    last_vmaf: Optional[float] = -1
    last_crf: Optional[float] = -1


class Iteration(BaseModel):
    file_name: str
    file_size_megabytes: float
    video_duration_seconds: float
    codec: str
    encoder_settings: EncoderSettings
    source_to_encoded_vmaf_percent: float

    encoding_finished_datetime: datetime

    encoding_time_seconds: float
    actual_frame_count: int
    sha256_hash: str
    ffmpeg_command_used: str
    environment: Environment
    ffmpeg_metadata: FfmpegMetadata


class EncoderDataJson(BaseModel):
    source_video: Optional[SourceVideo] = None
    encoding_stage: EncodingStage
    iterations: List[Iteration] = Field(default_factory=list)


class EncoderJobContext(BaseModel):
    source_file_path: Path
    metadata_json_file_path: Path
    is_locked: bool = False
    is_complete: bool = False
    report_data: Optional[EncoderDataJson] = None
