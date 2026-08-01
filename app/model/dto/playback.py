from typing import Optional

from pydantic import BaseModel


class Playback(BaseModel):
    id: Optional[int] = None
    duration_seconds: Optional[float] = None
    frames_counted: Optional[int] = None
    avg_frame_rate: Optional[str] = None
    r_frame_rate: Optional[str] = None
