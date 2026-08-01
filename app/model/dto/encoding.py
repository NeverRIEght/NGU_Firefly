from typing import Optional

from pydantic import BaseModel


class Encoding(BaseModel):
    id: Optional[int] = None
    codec: Optional[str] = None
    preset: Optional[str] = None
    encoder: Optional[str] = None
