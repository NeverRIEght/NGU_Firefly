from enum import Enum


class IterationStage(str, Enum):
    DEFINED = "defined"
    IN_PROGRESS = "in_progress"
    FAILED = "failed" # Unable to encode or inefficient encode, use original
    COMPLETED = "completed"
    MERGED = "merged"
