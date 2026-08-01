from typing import Optional

from pydantic import BaseModel


class Cpu(BaseModel):
    id: Optional[int] = None
    name: Optional[str] = None
    threads: Optional[int] = None
