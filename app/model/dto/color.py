from typing import Optional

from pydantic import BaseModel

from app.model.dto import HdrFormat, ColorStandard, ColorRange


class Color(BaseModel):
    id: Optional[int] = None
    hdr_format: Optional[HdrFormat] = None
    color_primaries: Optional[ColorStandard] = None
    color_trc: Optional[ColorStandard] = None
    colorspace: Optional[ColorStandard] = None
    color_range: Optional[ColorRange] = None
    max_cll: Optional[str] = None
    master_display: Optional[str] = None
    dovi_profile: Optional[str] = None
