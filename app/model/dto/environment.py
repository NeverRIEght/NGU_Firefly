from typing import Optional

from pydantic import BaseModel


class Environment(BaseModel):
    id: Optional[int] = None
    firefly_version: Optional[str] = None
    ffmpeg_version: Optional[str] = None
    compression_engine_version: Optional[int] = None