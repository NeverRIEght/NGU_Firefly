class LockConfig:
    DEFAULT_TIMEOUT = 5.0
    APPLICATION_LOCK_NAME = ".firefly.lock"
    JOB_LOCK_PREFIX = ".firefly_job_"
    METADATA_LOCK_SUFFIX = ".lock"
    STALE_LOCK_THRESHOLD = 3600  # 1 hour
