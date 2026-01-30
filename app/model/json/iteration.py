from typing import Optional

from pydantic import BaseModel

from model.json.encoder_settings import EncoderSettings
from model.json.environment import Environment
from model.json.execution_data import ExecutionData
from model.json.ffmpeg_metadata import FfmpegMetadata
from model.json.file_attributes import FileAttributes
from model.json.video_attributes import VideoAttributes


class Iteration(BaseModel):
    file_attributes: FileAttributes
    sha256_hash: Optional[str]
    video_attributes: Optional[VideoAttributes] = None
    encoder_settings: Optional[EncoderSettings] = None
    execution_data: Optional[ExecutionData] = None
    environment: Optional[Environment] = None
    ffmpeg_metadata: Optional[FfmpegMetadata] = None
