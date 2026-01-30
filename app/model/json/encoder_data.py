from typing import List

from pydantic import BaseModel, Field

from app.model.json.encoding_stage import EncodingStage
from app.model.json.iteration import Iteration
from app.model.json.source_video import SourceVideo


class EncoderData(BaseModel):
    schema_version: int
    source_video: SourceVideo
    encoding_stage: EncodingStage
    iterations: List[Iteration] = Field(default_factory=list)
