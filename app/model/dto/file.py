from pathlib import Path
from typing import Optional

from pydantic import BaseModel


class File(BaseModel):
    id: Optional[int] = None
    file_name: Optional[str] = None
    absolute_path: Optional[Path] = None
    file_size_bytes: Optional[int] = None
    sha256_hash: Optional[str] = None
