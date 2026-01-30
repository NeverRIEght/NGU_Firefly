from typing import Optional

from pydantic import BaseModel

from model.json.ffmpeg_metadata import FfmpegMetadata
from model.json.file_attributes import FileAttributes
from model.json.video_attributes import VideoAttributes


class SourceVideo(BaseModel):
    file_attributes: FileAttributes
    sha256_hash: Optional[str] = None
    video_attributes: Optional[VideoAttributes] = None
    ffmpeg_metadata: Optional[FfmpegMetadata] = None
