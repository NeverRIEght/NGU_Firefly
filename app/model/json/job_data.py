from typing import List, Any

from pydantic import BaseModel, Field, model_validator

from app.migrations import MigrationManager
from app.model.json.encoding_stage import EncodingStage
from app.model.json.iteration import Iteration
from app.model.json.source_video import SourceVideo


class JobData(BaseModel):
    schema_version: int
    source_video: SourceVideo
    encoding_stage: EncodingStage
    iterations: List[Iteration] = Field(default_factory=list)

    @model_validator(mode='before')
    @classmethod
    def migrate_before_validation(cls, data: Any) -> Any:
        if isinstance(data, dict):
            return MigrationManager.get_instance().apply(data)
        return data
