from pydantic import BaseModel
from typing import Optional


class FileAttributes(BaseModel):
    file_name: str
    file_size_megabytes: Optional[float]
