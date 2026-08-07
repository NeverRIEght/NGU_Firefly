from datetime import datetime
from typing import Optional

from pydantic import BaseModel

from app.model.dto import Cpu


class ExecutionData(BaseModel):
    id: Optional[int] = None
    ffmpeg_command_used: Optional[str] = None
    finished_datetime_utc: Optional[datetime] = None
    encoding_wall_time_seconds: Optional[float] = None
    evaluation_wall_time_seconds: Optional[float] = None
    encoding_cpu_time_seconds: Optional[float] = None
    evaluation_cpu_time_seconds: Optional[float] = None
    total_wall_time_seconds: Optional[float] = None
    total_cpu_time_seconds: Optional[float] = None
    encoding_cpu_threads_used: Optional[int] = None
    evaluation_cpu_threads_used: Optional[int] = None
    encoding_cpu: Optional[Cpu] = None
    evaluation_cpu: Optional[Cpu] = None
