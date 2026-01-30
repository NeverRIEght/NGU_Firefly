from typing import Optional

from pydantic import BaseModel

from app.model.json.encoder_settings import EncoderSettings
from app.model.json.environment import Environment
from app.model.json.execution_data import ExecutionData
from app.model.json.ffmpeg_metadata import FfmpegMetadata
from app.model.json.file_attributes import FileAttributes
from app.model.json.video_attributes import VideoAttributes


class Iteration(BaseModel):
    file_attributes: FileAttributes
    sha256_hash: Optional[str]
    video_attributes: Optional[VideoAttributes] = None
    encoder_settings: Optional[EncoderSettings] = None
    execution_data: Optional[ExecutionData] = None
    environment: Optional[Environment] = None
    ffmpeg_metadata: Optional[FfmpegMetadata] = None
