from typing import Optional

from pydantic import BaseModel

from app.model.dto import File, Display, Playback, Encoding, Color, EmbeddedMetadata


class Video(BaseModel):
    id: Optional[int] = None
    file: Optional[File] = None
    display: Optional[Display] = None
    playback: Optional[Playback] = None
    encoding: Optional[Encoding] = None
    color: Optional[Color] = None
    embedded_metadata: Optional[EmbeddedMetadata] = None
