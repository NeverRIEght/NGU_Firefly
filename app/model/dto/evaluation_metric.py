from typing import Optional

from pydantic import BaseModel


class EvaluationMetric(BaseModel):
    id: Optional[int] = None
    name: Optional[str] = None
    version: Optional[str] = None
