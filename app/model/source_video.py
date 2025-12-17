from typing import Optional

from pydantic import BaseModel

from app.model.ffmpeg_metadata import FfmpegMetadata
from app.model.file_attributes import FileAttributes
from app.model.video_attributes import VideoAttributes


class SourceVideo(BaseModel):
    file_attributes: FileAttributes
    sha256_hash: Optional[str]
    video_attributes: VideoAttributes
    ffmpeg_metadata: FfmpegMetadata
