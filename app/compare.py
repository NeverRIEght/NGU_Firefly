import logging
from pathlib import Path

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

from typing import Dict


def get_file_size_megabytes(file_path: Path) -> float:
    """
    Returns the size of a file in megabytes.
    :param file_path: Path to the file.
    :return: Size of the file in megabytes.
    """
    if not file_path.is_file():
        log.error(f"File does not exist or is not a file: {file_path}")
        return 0.0
    return file_path.stat().st_size / (1024 * 1024)

def find_file_pairs(folder1_path: Path, folder2_path: Path, extensions: list[str]) -> Dict[Path, Path]:
    """
    Finds pairs of files in two folders based on their names and extensions.
    :param folder1_path: Path to the first folder.
    :param folder2_path: Path to the second folder.
    :param extensions: List of file extensions to consider, starting with full stop (e.g., ['.mp4', '.mkv']).
    :return: Dictionary where keys are paths to files in folder1 and values are paths to files in folder2
    """
    if not folder1_path.is_dir():
        log.error(f"Folder 1 does not exists or is not a folder: {folder1_path}")
        return {}
    if not folder2_path.is_dir():
        log.error(f"Folder 2 does not exists or is not a folder: {folder2_path}")
        return {}

    files1_relative = {f.relative_to(folder1_path) for f in folder1_path.rglob('*')
                       if f.is_file() and f.suffix.lower() in extensions}
    files2_relative = {f.relative_to(folder2_path) for f in folder2_path.rglob('*')
                       if f.is_file() and f.suffix.lower() in extensions}

    common_relative_paths = files1_relative.intersection(files2_relative)

    paired_files = {
        folder1_path / rel_path: folder2_path / rel_path
        for rel_path in common_relative_paths
    }

    return paired_files


def compare_folder_sizes(folder1_path: Path, folder2_path: Path):
    """
    Сравнивает размеры файлов в двух папках, находящих пары по имени файла и его пути.
    Теперь работает на неограниченную глубину.

    Args:
        folder1_path: Путь к первой папке.
        folder2_path: Путь ко второй папке.
    """
    # Сразу используем нашу новую функцию для получения словаря с парами
    file_pairs = find_file_pairs(folder1_path, folder2_path, extensions=['.mp4'])
    common_keys = sorted(file_pairs.keys())
    found_pairs = len(common_keys)

    if found_pairs == 0:
        print("Пары файлов не найдены.")
        return

    total_size1_paired = 0
    total_size2_paired = 0

    print(f"\n--- Сравнение размеров файлов между '{folder1_path.name}' и '{folder2_path.name}' ---")
    print(
        f"{'Относительный путь к файлу':<60} | {'Размер в папке 1 (МБ)':<25} | {'Размер в папке 2 (МБ)':<25} | {'Соотношение (2/1)':<18}")
    print("-" * 140)

    for path1 in common_keys:
        path2 = file_pairs[path1]

        size1_bytes = path1.stat().st_size
        size2_bytes = path2.stat().st_size

        size1_mb = get_file_size_megabytes(path1)
        size2_mb = get_file_size_megabytes(path2)

        ratio = size2_mb / size1_mb if size1_mb > 0 else 0

        total_size1_paired += size1_bytes
        total_size2_paired += size2_bytes

        relative_path_str = str(path1.relative_to(folder1_path))
        if len(relative_path_str) > 58:
            relative_path_str = "..." + relative_path_str[-55:]

        print(f"{relative_path_str:<60} | {size1_mb:<25.2f} | {size2_mb:<25.2f} | {ratio:<18.3f}")

    print("-" * 140)
    print(f"\nОбщий размер парных файлов в '{folder1_path.name}': {total_size1_paired / (1024 * 1024):.2f} МБ")
    print(f"Общий размер парных файлов в '{folder2_path.name}': {total_size2_paired / (1024 * 1024):.2f} МБ")

    overall_ratio = total_size2_paired / total_size1_paired if total_size1_paired > 0 else 0
    print(f"Общее соотношение размеров (папка 2 / папка 1): {overall_ratio:.3f}")
    print(f"Экономия места: {((1 - overall_ratio) * 100):.2f}%")
    print(f"\n--- Сравнение завершено. Найдено пар: {found_pairs} ---")


if __name__ == "__main__":
    original_videos_dir = Path("/Users/michaelkomarov/Documents/encode/input")
    encoded_videos_dir = Path("/Users/michaelkomarov/Documents/encode/output")

    compare_folder_sizes(original_videos_dir, encoded_videos_dir)
