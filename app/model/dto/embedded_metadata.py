from datetime import datetime
from typing import Optional

from pydantic import BaseModel

from app.model.dto import Environment


class EmbeddedMetadata(BaseModel):
    id: Optional[int] = None
    encodes_count: Optional[int] = None
    last_encode_datetime: Optional[datetime] = None
    source_video_sha256_hash: Optional[str] = None
    environment: Optional[Environment] = None

