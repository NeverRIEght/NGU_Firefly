import logging
from pathlib import Path

from app.compare import find_file_pairs

logs_dir = Path("../logs")
logs_dir.mkdir(exist_ok=True)

log = logging.getLogger()
log.setLevel(logging.INFO)

if log.hasHandlers():
    log.handlers.clear()

logs_formatter = logging.Formatter('[%(asctime)s][%(levelname)s]: %(message)s')

all_logs_handler = logging.FileHandler(logs_dir / "full.log", mode='a', encoding='utf-8')
all_logs_handler.setLevel(logging.INFO)
all_logs_formatter = logs_formatter
all_logs_handler.setFormatter(all_logs_formatter)
log.addHandler(all_logs_handler)

error_logs_handler = logging.FileHandler(logs_dir / "errors.log", mode='a', encoding='utf-8')
error_logs_handler.setLevel(logging.ERROR)
error_logs_formatter = logs_formatter
error_logs_handler.setFormatter(error_logs_formatter)
log.addHandler(error_logs_handler)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_formatter = logs_formatter
console_handler.setFormatter(console_formatter)
log.addHandler(console_handler)


def get_lower_size_file(file1_path: Path, file2_path: Path) -> Path:
    """
    Returns the path to the file with the smaller size.

    :param file1_path: Path to the first file.
    :param file2_path: Path to the second file.
    :return: Path to the file with the smaller size.
    """
    if not file1_path.is_file():
        log.error(f"File does not exist or is not a file: {file1_path}")
        return file2_path
    if not file2_path.is_file():
        log.error(f"File does not exist or is not a file: {file2_path}")
        return file1_path

    size1 = file1_path.stat().st_size
    size2 = file2_path.stat().st_size

    return file1_path if size1 < size2 else file2_path

def remove_higher_size_file(file1_path: Path, file2_path: Path):
    """
    Removes the file with the larger size.

    :param file1_path: Path to the first file.
    :param file2_path: Path to the second file.
    """
    if not file1_path.is_file():
        log.error(f"File does not exist or is not a file: {file1_path}")
        return
    if not file2_path.is_file():
        log.error(f"File does not exist or is not a file: {file2_path}")
        return

    if get_lower_size_file(file1_path, file2_path) == file1_path:
        log.info(f"Removing larger file: {file2_path}")
        file2_path.unlink()
    else:
        log.info(f"Removing larger file: {file1_path}")
        file1_path.unlink()

if __name__ == "__main__":
    original_videos_dir = Path("/Users/michaelkomarov/Documents/encode/input")
    encoded_videos_dir = Path("/Users/michaelkomarov/Documents/encode/output")

    pairs = find_file_pairs(original_videos_dir, encoded_videos_dir, list(".mp4"))

    for original_file, encoded_file in pairs.items():
        remove_higher_size_file(original_file, encoded_file)