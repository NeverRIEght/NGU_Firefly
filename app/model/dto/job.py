from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, Field

from app.model.dto import JobStage, Segment, Video


class Job(BaseModel):
    id: Optional[int] = None
    source_video: Optional[Video] = None
    stage: Optional[JobStage] = None
    created_datetime_utc: Optional[datetime] = None
    total_time_seconds: Optional[float] = None
    segments: List[Segment] = Field(default_factory=list)
