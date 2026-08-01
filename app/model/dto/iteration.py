from typing import Optional

from pydantic import BaseModel

from app.model.dto import IterationStage, Video, Cpu, Environment, ExecutionData


class Iteration(BaseModel):
    id: Optional[int] = None
    stage: Optional[IterationStage] = None
    video: Optional[Video] = None
    cpu: Optional[Cpu] = None
    environment: Optional[Environment] = None
    execution_data: Optional[ExecutionData] = None
    crf: Optional[int] = None
