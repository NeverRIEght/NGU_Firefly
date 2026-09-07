from enum import Enum


class LockMode(Enum):
    EXCLUSIVE = "exclusive"  # Write lock
    SHARED = "shared"  # Read lock
