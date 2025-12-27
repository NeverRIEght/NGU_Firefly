from typing import Optional

from pydantic import BaseModel


class ExecutionData(BaseModel):
    ffmpeg_command_used: str
    source_to_encoded_vmaf_percent: float
    encoding_finished_datetime: str
    encoding_time_seconds: float
    calculating_vmaf_time_seconds: Optional[float] = None
    iteration_time_seconds: Optional[float] = None
