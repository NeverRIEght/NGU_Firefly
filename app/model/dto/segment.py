from typing import List, Optional

from pydantic import BaseModel, Field

from app.model.dto import Iteration, Job, SegmentStatus


class Segment(BaseModel):
    id: Optional[int] = None
    job: Optional[Job] = None
    from_frame: Optional[int] = None
    to_frame: Optional[int] = None
    status: Optional[SegmentStatus] = None
    total_time_seconds: Optional[float] = None
    iterations: List[Iteration] = Field(default_factory=list)
