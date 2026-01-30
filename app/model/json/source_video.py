from typing import Optional

from pydantic import BaseModel

from app.model.json.ffmpeg_metadata import FfmpegMetadata
from app.model.json.file_attributes import FileAttributes
from app.model.json.video_attributes import VideoAttributes


class SourceVideo(BaseModel):
    file_attributes: FileAttributes
    sha256_hash: Optional[str] = None
    video_attributes: Optional[VideoAttributes] = None
    ffmpeg_metadata: Optional[FfmpegMetadata] = None
