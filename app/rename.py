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

from shutil import copy2

from app.compare import find_file_pairs, get_file_size_megabytes


def process_and_copy_smaller_files(folder1_path: Path, folder2_path: Path, destination_folder: Path):
    """
    Сравнивает файлы по размеру и копирует меньший в папку назначения с новым именем.

    Args:
        folder1_path: Путь к первой папке (оригинальные файлы).
        folder2_path: Путь ко второй папке (кодированные файлы).
        destination_folder: Путь к папке, куда будут скопированы файлы.
    """
    log.info(f"Начинаем обработку и копирование файлов в '{destination_folder.name}'...")
    destination_folder.mkdir(exist_ok=True)

    file_pairs = find_file_pairs(folder1_path, folder2_path, extensions=['.mp4'])

    if not file_pairs:
        log.info("Пары файлов для обработки не найдены.")
        return

    for path1, path2 in file_pairs.items():
        try:
            size1 = get_file_size_megabytes(path1)
            size2 = get_file_size_megabytes(path2)

            if size1 < size2:
                # Файл из папки 1 меньше
                source_path = path1
                new_filename = f"{path1.stem}_h264_do_not_reencode{path1.suffix}"
            else:
                # Файл из папки 2 меньше или равен
                source_path = path2
                new_filename = f"{path2.stem}_h265_veryslow{path2.suffix}"

            destination_path = destination_folder / new_filename

            copy2(source_path, destination_path)
            log.info(f"Скопирован {source_path.name} в {destination_path.name}")

        except Exception as e:
            log.error(f"Ошибка при обработке файлов {path1} и {path2}: {e}")

if __name__ == "__main__":
    original_videos_dir = Path("/Users/michaelkomarov/Documents/encode/input")
    encoded_videos_dir = Path("/Users/michaelkomarov/Documents/encode/output")
    # Создаем новую папку для итоговых файлов
    final_videos_dir = Path("/Users/michaelkomarov/Documents/encode/final")
    process_and_copy_smaller_files(original_videos_dir, encoded_videos_dir, final_videos_dir)