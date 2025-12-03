from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime

class Resolution(BaseModel):
    width_px: int = Field(..., description="Width in pixels")
    height_px: int = Field(..., description="Height in pixels")


class FfmpegMetadata(BaseModel):
    pixel_aspect_ratio: Optional[str]
    pixel_format: Optional[str]
    chroma_sample_location: Optional[str]
    color_primaries: Optional[str]
    color_trc: Optional[str]
    colorspace: Optional[str]
    profile: Optional[str]
    level: Optional[str]


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
    average_bitrate_kilobits_per_second: int
    fps: float
    actual_frame_count: int # Can be calculated: fps * duration
    sha256_hash: str
    ffmpeg_metadata: FfmpegMetadata


class EncodingStage(BaseModel):
    stage_number_from_1: int
    stage_name: str
    crf_range_min: Optional[int]
    crf_range_max: Optional[int]
    last_vmaf: Optional[float]
    last_crf: Optional[float]


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
    source_video: SourceVideo
    encoding_stage: EncodingStage
    iterations: List[Iteration]

class EncoderJobContext(BaseModel):
    source_file_path: Path
    metadata_json_file_path: Path
    is_locked: bool = False
    is_complete: bool = False
    report_data: Optional[EncoderDataJson]