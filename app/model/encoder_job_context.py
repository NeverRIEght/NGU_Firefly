from pathlib import Path
from typing import Optional

from pydantic import BaseModel

from app.model.json.job_data import JobData


class EncoderJob(BaseModel):
    source_file_path: Path
    metadata_json_file_path: Path
    is_locked: bool = False
    is_complete: bool = False
    job_data: Optional[JobData] = None
