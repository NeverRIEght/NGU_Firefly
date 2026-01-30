from typing import List

from pydantic import BaseModel, Field

from app.model.encoding_stage import EncodingStage
from app.model.iteration import Iteration
from app.model.source_video import SourceVideo


class EncoderDataJson(BaseModel):
    schema_version: int
    source_video: SourceVideo
    encoding_stage: EncodingStage
    iterations: List[Iteration] = Field(default_factory=list)
