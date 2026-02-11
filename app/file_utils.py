import logging

from app.locking import LockManager, LockMode

log = logging.getLogger(__name__)

from pathlib import Path

import shutil


def get_file_name_with_extension(file_path: Path) -> str:
    if file_path is None:
        log.error("get_file_name_with_extension: file_path parameter cannot be None")
        raise ValueError("get_file_name_with_extension: file_path parameter cannot be None")
    return file_path.name


def get_file_name_without_extension(file_path: Path) -> str:
    if file_path is None:
        log.error("get_file_name_without_extension: file_path parameter cannot be None")
        raise ValueError("get_file_name_without_extension: file_path parameter cannot be None")
    return file_path.stem


def get_file_extension(file_path: Path) -> str:
    if file_path is None:
        log.error("get_file_extension: file_path parameter cannot be None")
        raise ValueError("get_file_extension: file_path parameter cannot be None")
    return file_path.suffix


def get_file_parent_folder(file_path: Path) -> Path:
    if file_path is None:
        log.error("get_file_parent_folder: file_path parameter cannot be None")
        raise ValueError("get_file_parent_folder: file_path parameter cannot be None")
    return file_path.parent


def get_file_size_mebibytes(file_path: Path) -> float:
    size_in_bytes = get_file_size_bytes(file_path)
    return size_in_bytes / (1024 * 1024)


def get_file_size_bytes(file_path: Path) -> int:
    if file_path is None:
        log.error("get_file_size_bytes: file_path parameter cannot be None")
        raise ValueError("get_file_size_bytes: file_path parameter cannot be None")
    with LockManager.acquire_file_operation_lock(file_path, LockMode.EXCLUSIVE):
        log.debug(f"Getting file size for: {file_path}")
        try:
            if not file_path.is_file():
                log.error(f"File not found for size calculation: {file_path}")
                raise FileNotFoundError(f"File not found for size calculation: {file_path}")

            size_in_bytes = file_path.stat().st_size
            return size_in_bytes
        except OSError as e:
            log.error(f"Failed to access file {file_path}: {e}")
            raise


def check_file_exists(file_path: Path) -> bool:
    if file_path is None:
        log.error("check_file_exists: file_path parameter cannot be None")
        raise ValueError("check_file_exists: file_path parameter cannot be None")
    return file_path.is_file()


def check_directory_exists(dir_path: Path) -> bool:
    if dir_path is None:
        log.error("check_directory_exists: dir_path parameter cannot be None")
        raise ValueError("check_directory_exists: dir_path parameter cannot be None")
    return dir_path.is_dir()

def delete_file(file_path: Path) -> bool:
    if file_path is None:
        log.error("delete_file: file_path parameter cannot be None")
        raise ValueError("delete_file: file_path parameter cannot be None")
    if file_path.is_file():
        try:
            file_path.unlink()
            log.debug(f"Deleted file: {file_path}")
            return True
        except OSError as e:
            log.error(f"Error deleting file {file_path}: {e}")
            return False
    return False


def delete_file_with_lock(file_path: Path) -> bool:
    with LockManager.acquire_file_operation_lock(file_path, LockMode.EXCLUSIVE):
        return delete_file(file_path)


def copy_file(source_path: Path, destination_path: Path) -> bool:
    if source_path is None:
        log.error("copy_file: source_path parameter cannot be None")
        raise ValueError("copy_file: source_path parameter cannot be None")
    if destination_path is None:
        log.error("copy_file: destination_path parameter cannot be None")
        raise ValueError("copy_file: destination_path parameter cannot be None")
    with LockManager.acquire_file_operation_lock(destination_path, LockMode.EXCLUSIVE):
        with LockManager.acquire_file_operation_lock(source_path, LockMode.SHARED):
            try:
                shutil.copy2(source_path, destination_path)
                log.debug(f"Copied file from {source_path} to {destination_path}")
                return True
            except OSError as e:
                log.error(f"Error copying file from {source_path} to {destination_path}. Details: \n{e}")
                return False


def rename_file(source_path: Path, destination_path: Path, overwrite: bool = False) -> bool:
    if source_path is None or destination_path is None:
        log.error("rename_file: parameters cannot be None")
        raise ValueError("rename_file: parameters cannot be None")

    if source_path.resolve() == destination_path.resolve():
        return True

    if not source_path.is_file():
        return False

    if destination_path.exists() and not overwrite:
        return False

    destination_path.parent.mkdir(parents=True, exist_ok=True)
    source_path.replace(destination_path)
    return True


def rename_file_with_lock(source_path: Path, destination_path: Path, overwrite: bool = False) -> bool:
    with LockManager.acquire_file_operation_lock(source_path, LockMode.EXCLUSIVE):
        with LockManager.acquire_file_operation_lock(destination_path, LockMode.EXCLUSIVE):
            return rename_file(source_path, destination_path, overwrite)
