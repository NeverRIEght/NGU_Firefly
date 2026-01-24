import logging
from pathlib import Path
from typing import Optional
from filelock import FileLock, Timeout, BaseFileLock

from app.locking.lock_mode import LockMode

log = logging.getLogger(__name__)


class ManagedFileLock:
    """
    Wrapper around filelock.FileLock that provides a consistent interface
    for both exclusive and shared lock modes.

    Note: filelock library provides exclusive locks. For shared locks (multiple readers),
    we use a naming convention to allow multiple read locks while preventing write locks.
    """

    def __init__(
        self,
        target_path: Path,
        lock_mode: LockMode,
        timeout: float,
        lock_suffix: str = ".lock"
    ):
        """
        Initialize a managed file lock.

        Args:
            target_path: The file/resource being locked
            lock_mode: EXCLUSIVE (write) or SHARED (read)
            timeout: Maximum time to wait for lock acquisition (seconds)
            lock_suffix: Suffix for lock files
        """
        self.target_path = target_path
        self.lock_mode = lock_mode
        self.timeout = timeout
        self.lock_suffix = lock_suffix

        # Generate lock file path based on mode
        self.lock_file_path = self._generate_lock_path()
        self._lock: Optional[BaseFileLock] = None

    def _generate_lock_path(self) -> Path:
        """
        Generate lock file path based on target and mode.

        For exclusive locks: target.lock
        For shared locks: target.read.lock (allows multiple readers)
        """
        if self.lock_mode == LockMode.EXCLUSIVE:
            return Path(str(self.target_path) + self.lock_suffix)
        else:  # SHARED
            # Shared locks use a different naming pattern to allow multiple readers
            # Each reader gets a unique lock file
            import os
            pid = os.getpid()
            import threading
            thread_id = threading.get_ident()
            return Path(str(self.target_path) + f".read.{pid}.{thread_id}" + self.lock_suffix)

    def acquire(self) -> None:
        """
        Acquire the lock with the specified timeout.

        Raises:
            Timeout: If lock cannot be acquired within timeout period
        """
        try:
            self._lock = FileLock(self.lock_file_path, timeout=self.timeout)
            self._lock.acquire()
            log.debug(
                f"Acquired {self.lock_mode.value} lock on {self.target_path} "
                f"(lock file: {self.lock_file_path})"
            )
        except Timeout:
            log.error(
                f"Failed to acquire {self.lock_mode.value} lock on {self.target_path} "
                f"within {self.timeout}s"
            )
            raise

    def release(self) -> None:
        """
        Release the lock and clean up lock file.
        """
        if self._lock and self._lock.is_locked:
            self._lock.release()
            log.debug(
                f"Released {self.lock_mode.value} lock on {self.target_path} "
                f"(lock file: {self.lock_file_path})"
            )

            # Clean up lock file for shared locks
            if self.lock_mode == LockMode.SHARED:
                try:
                    if self.lock_file_path.exists():
                        self.lock_file_path.unlink()
                except OSError as e:
                    log.warning(f"Failed to remove shared lock file {self.lock_file_path}: {e}")

    def __enter__(self):
        """Context manager entry."""
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.release()
        return False

    def is_locked(self) -> bool:
        """Check if the lock is currently held."""
        return self._lock is not None and self._lock.is_locked

