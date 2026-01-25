import logging
from pathlib import Path
from typing import Optional, ContextManager

from app.config.lock_config import LockConfig
from app.locking.lock_mode import LockMode
from app.locking.file_lock import ManagedFileLock

log = logging.getLogger(__name__)


class LockManager:
    """
    Centralized lock manager for all locking operations in the application.
    Provides factory methods for different lock types with appropriate defaults.
    """

    @staticmethod
    def acquire_application_lock(
            output_dir: Path,
            timeout: Optional[float] = None
    ) -> ContextManager[ManagedFileLock]:
        """
        Acquire application-wide lock to prevent multiple instances.

        Args:
            output_dir: Directory where application lock file will be created
            timeout: Lock acquisition timeout (uses default if None)

        Returns:
            Context manager for the application lock

        Usage:
            with LockManager.acquire_application_lock(output_dir):
                # Application code runs exclusively
                pass
        """
        timeout = timeout or LockConfig.DEFAULT_TIMEOUT

        if not output_dir.exists():
            output_dir.mkdir(parents=True, exist_ok=True)

        lock_path = output_dir / LockConfig.APPLICATION_LOCK_NAME

        log.debug(f"Acquiring application lock at {lock_path}")
        return ManagedFileLock(
            target_path=lock_path,
            lock_mode=LockMode.EXCLUSIVE,
            timeout=timeout
        )

    @staticmethod
    def acquire_job_lock(
            source_video_path: Path,
            output_dir: Path,
            timeout: Optional[float] = None
    ) -> ContextManager[ManagedFileLock]:
        """
        Acquire job-level lock.

        Args:
            source_video_path: Path to the source video file
            output_dir: Directory where job lock file will be created
            timeout: Lock acquisition timeout (uses default if None)

        Returns:
            Context manager for the job lock

        Usage:
            with LockManager.acquire_job_lock(video_path, output_dir):
                # Process video exclusively
                pass
        """
        timeout = timeout or LockConfig.DEFAULT_TIMEOUT

        # Create lock file name based on video file name to ensure uniqueness
        video_name = source_video_path.stem
        lock_filename = f"{LockConfig.JOB_LOCK_PREFIX}{video_name}"
        lock_path = output_dir / lock_filename

        log.debug(f"Acquiring job lock for {source_video_path.name} at {lock_path}")
        return ManagedFileLock(
            target_path=lock_path,
            lock_mode=LockMode.EXCLUSIVE,
            timeout=timeout
        )

    @staticmethod
    def acquire_metadata_lock(
            metadata_file_path: Path,
            lock_mode: LockMode = LockMode.EXCLUSIVE,
            timeout: Optional[float] = None
    ) -> ContextManager[ManagedFileLock]:
        """
        Acquire lock for metadata file access.
        Supports both read (SHARED) and write (EXCLUSIVE) locks.

        Args:
            metadata_file_path: Path to the metadata JSON file
            lock_mode: SHARED for reads, EXCLUSIVE for writes
            timeout: Lock acquisition timeout (uses default if None)

        Returns:
            Context manager for the metadata lock

        Usage:
            # Reading metadata (allows multiple readers)
            with LockManager.acquire_metadata_lock(path, LockMode.SHARED):
                data = json.load(f)

            # Writing metadata (exclusive access)
            with LockManager.acquire_metadata_lock(path, LockMode.EXCLUSIVE):
                json.dump(data, f)
        """
        timeout = timeout or LockConfig.DEFAULT_TIMEOUT

        log.debug(
            f"Acquiring {lock_mode.value} lock for metadata file {metadata_file_path.name}"
        )
        return ManagedFileLock(
            target_path=metadata_file_path,
            lock_mode=lock_mode,
            timeout=timeout,
            lock_suffix=LockConfig.METADATA_LOCK_SUFFIX
        )

    @staticmethod
    def acquire_file_operation_lock(
            file_path: Path,
            lock_mode: LockMode = LockMode.EXCLUSIVE,
            timeout: Optional[float] = None
    ) -> ContextManager[ManagedFileLock]:
        """
        Acquire lock for general file operations (copy, move, delete).

        Args:
            file_path: Path to the file being operated on
            lock_mode: SHARED for reads, EXCLUSIVE for writes/modifications
            timeout: Lock acquisition timeout (uses default if None)

        Returns:
            Context manager for the file operation lock

        Usage:
            with LockManager.acquire_file_operation_lock(file_path):
                shutil.copy(source, dest)
        """
        timeout = timeout or LockConfig.DEFAULT_TIMEOUT

        log.debug(
            f"Acquiring {lock_mode.value} lock for file operation on {file_path.name}"
        )
        return ManagedFileLock(
            target_path=file_path,
            lock_mode=lock_mode,
            timeout=timeout
        )

    @staticmethod
    def acquire_segment_lock(
            source_video_path: Path,
            segment_index: int,
            output_dir: Path,
            timeout: Optional[float] = None
    ) -> ContextManager[ManagedFileLock]:
        """
        Acquire lock for a specific video segment.
        Allows parallel processing of different segments of the same video.

        Args:
            source_video_path: Path to the source video file
            segment_index: Index of the segment being processed
            output_dir: Directory where segment lock file will be created
            timeout: Lock acquisition timeout (uses default if None)

        Returns:
            Context manager for the segment lock

        Usage:
            with LockManager.acquire_segment_lock(video_path, segment_idx, output_dir):
                # Process segment exclusively
                pass
        """
        timeout = timeout or LockConfig.DEFAULT_TIMEOUT

        video_name = source_video_path.stem
        lock_filename = f"{LockConfig.JOB_LOCK_PREFIX}{video_name}_segment_{segment_index}"
        lock_path = output_dir / lock_filename

        log.debug(
            f"Acquiring segment lock for {source_video_path.name} "
            f"segment {segment_index} at {lock_path}"
        )
        return ManagedFileLock(
            target_path=lock_path,
            lock_mode=LockMode.EXCLUSIVE,
            timeout=timeout
        )
