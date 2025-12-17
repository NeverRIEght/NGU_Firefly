from typing import Optional

from pydantic import BaseModel


class FfmpegMetadata(BaseModel):
    pixel_aspect_ratio: Optional[str] = None
    pixel_format: Optional[str] = None
    chroma_sample_location: Optional[str] = None
    color_primaries: Optional[str] = None
    color_trc: Optional[str] = None
    colorspace: Optional[str] = None
    profile: Optional[str] = None
    level: Optional[int] = None