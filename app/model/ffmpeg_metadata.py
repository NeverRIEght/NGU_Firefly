from enum import Enum
from typing import Optional, Set

from pydantic import BaseModel, Field


class HdrType(str, Enum):
    DOLBY_VISION = "dolby_vision"
    HDR10 = "hdr10"
    HDR10_PLUS = "hdr10_plus"
    HLG = "hlg"  # SDR-compatible HDR format
    PQ = "pq"  # PQ is a family of HDR formats: HDR10, HDR10+, Dolby Vision


class FfmpegMetadata(BaseModel):
    pixel_aspect_ratio: Optional[str] = None
    pixel_format: Optional[str] = None
    chroma_sample_location: Optional[str] = None
    color_primaries: Optional[str] = None
    color_trc: Optional[str] = None
    colorspace: Optional[str] = None
    profile: Optional[str] = None
    level: Optional[int] = None
    hdr_types: Set[HdrType] = Field(default_factory=set)
