from enum import Enum


class LockType(Enum):
    APPLICATION = "application"  # Prevent multiple application instances
    JOB = "job"  # Prevent same video being processed twice
    METADATA = "metadata"  # Thread-safe JSON file access
    FILE_OPERATION = "file_operation"
