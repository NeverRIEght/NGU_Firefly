from typing import Optional

from pydantic import BaseModel


class Display(BaseModel):
    id: Optional[int] = None
    width: Optional[int] = None
    height: Optional[int] = None
    display_aspect_ratio: Optional[str] = None
    pixel_aspect_ratio: Optional[str] = None
    pixel_format: Optional[str] = None
    chroma_sample_location: Optional[str] = None
