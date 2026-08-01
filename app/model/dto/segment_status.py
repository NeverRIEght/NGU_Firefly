from enum import Enum


class SegmentStatus(str, Enum):
    DEFINED = "defined"
    IN_PROGRESS = "in_progress"
    FAILED = "failed" # Unable to encode or inefficient encode, use original
    COMPLETED = "completed"
