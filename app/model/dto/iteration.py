from typing import Optional

from pydantic import BaseModel

from app.model.dto import Environment, ExecutionData, IterationStage, Segment, Video


class Iteration(BaseModel):
    id: Optional[int] = None
    segment: Optional[Segment] = None
    stage: Optional[IterationStage] = None
    video: Optional[Video] = None
    environment: Optional[Environment] = None
    execution_data: Optional[ExecutionData] = None
    crf: Optional[int] = None
