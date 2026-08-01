from enum import Enum


class JobStage(str, Enum):
    CREATED = "created"
    METADATA_EXTRACTION = "metadata_extraction"
    FILTERED_OUT = "filtered_out"
    IN_PROGRESS = "in_progress"
    FAILED = "failed"
    COMPLETED = "completed"
