from enum import Enum
from typing import Optional

from pydantic import BaseModel


class EncodingStage(BaseModel):
    stage_number_from_1: int
    stage_name: EncodingStageNamesEnum
    crf_range_min: Optional[int] = None
    crf_range_max: Optional[int] = None
    last_vmaf: Optional[float] = None
    last_crf: Optional[float] = None

class EncodingStageNamesEnum(str, Enum):
    PREPARED = "job_prepared" # 1
    METADATA_EXTRACTED = "metadata_extracted" # 2
    SEARCHING_CRF = "searching_crf" # 3
    CRF_FOUND = "perfect_crf_found" # 4
    COMPLETED = "encoding_completed" # 5