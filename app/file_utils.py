import logging

log = logging.getLogger(__name__)

from pathlib import Path


def get_file_name_with_extension(file_path: Path) -> str:
    return file_path.name


def get_file_name_without_extension(file_path: Path) -> str:
    return file_path.stem


def get_file_extension(file_path: Path) -> str:
    return file_path.suffix


def get_file_parent_folder(file_path: Path) -> Path:
    return file_path.parent


def get_file_size_megabytes(file_path: Path) -> float | None:
    log.info(f"Getting file size for: {file_path}")
    try:
        if not file_path.is_file():
            log.error(f"File not found for size calculation: {file_path}")
            raise FileNotFoundError(f"File not found for size calculation: {file_path}")

        size_in_bytes = file_path.stat().st_size
        size_in_mb = size_in_bytes / (1024 * 1024)
        return size_in_mb
    except OSError as e:
        log.error(f"Failed to access file {file_path}: {e}")
        return None


def check_file_exists(file_path: Path) -> bool:
    return file_path.is_file()


def delete_file(file_path: Path) -> bool:
    if file_path.is_file():
        try:
            file_path.unlink()
            log.info(f"Deleted file: {file_path}")
            return True
        except OSError as e:
            log.error(f"Error deleting file {file_path}. Details: \n{e}")
            return False
    return False
