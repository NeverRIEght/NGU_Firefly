from pathlib import Path
from typing import Optional

from pydantic import BaseModel

from app.model.json.encoder_data import EncoderData


class EncoderJobContext(BaseModel):
    source_file_path: Path
    metadata_json_file_path: Path
    is_locked: bool = False
    is_complete: bool = False
    encoder_data: Optional[EncoderData] = None
