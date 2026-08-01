from typing import List, Optional

from pydantic import BaseModel, Field

from app.model.dto import SegmentStatus, Iteration


class Segment(BaseModel):
    id: Optional[int] = None
    from_frame: Optional[int] = None
    to_frame: Optional[int] = None
    status: Optional[SegmentStatus] = None
    total_time_seconds: Optional[float] = None
    iterations: List[Iteration] = Field(default_factory=list)
