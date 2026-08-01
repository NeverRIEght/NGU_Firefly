from typing import Optional

from pydantic import BaseModel

from app.model.dto import EvaluationMetric


class Evaluation(BaseModel):
    id: Optional[int] = None
    metric: Optional[EvaluationMetric] = None
    score: Optional[float] = None
