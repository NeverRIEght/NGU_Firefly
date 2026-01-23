from enum import Enum
from typing import Optional

from pydantic import BaseModel


class HdrType(str, Enum):
    DOLBY_VISION = "dolby_vision"
    HDR10 = "hdr10"
    HDR10_PLUS = "hdr10_plus"
    HLG = "hlg"  # SDR-compatible HDR format
    PQ = "pq"  # PQ is a family of HDR formats: HDR10, HDR10+, Dolby Vision
    SDR = "sdr"


class FfmpegMetadata(BaseModel):
    pixel_aspect_ratio: Optional[str] = None
    pixel_format: Optional[str] = None
    chroma_sample_location: Optional[str] = None
    color_primaries: Optional[str] = None
    color_trc: Optional[str] = None
    colorspace: Optional[str] = None
    profile: Optional[str] = None
    level: Optional[int] = None
    is_hdr: Optional[bool] = None
    hdr_type: list[HdrType] = []
