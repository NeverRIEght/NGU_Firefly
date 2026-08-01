from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class ExecutionData(BaseModel):
    id: Optional[int] = None
    ffmpeg_command_used: Optional[str] = None
    finished_datetime_utc: Optional[datetime] = None
    encoding_time_seconds: Optional[float] = None
    evaluation_time_seconds: Optional[float] = None
    total_time_seconds: Optional[float] = None
    encoding_cpu_threads_used: Optional[int] = None
    evaluation_cpu_threads_used: Optional[int] = None
