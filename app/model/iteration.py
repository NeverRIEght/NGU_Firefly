from typing import Optional

from pydantic import BaseModel

from app.model.encoder_settings import EncoderSettings
from app.model.environment import Environment
from app.model.execution_data import ExecutionData
from app.model.ffmpeg_metadata import FfmpegMetadata
from app.model.file_attributes import FileAttributes
from app.model.video_attributes import VideoAttributes


class Iteration(BaseModel):
    file_attributes: FileAttributes
    sha256_hash: Optional[str]
    video_attributes: Optional[VideoAttributes] = None
    encoder_settings: Optional[EncoderSettings] = None
    execution_data: Optional[ExecutionData] = None
    environment: Optional[Environment] = None
    ffmpeg_metadata: Optional[FfmpegMetadata] = None
