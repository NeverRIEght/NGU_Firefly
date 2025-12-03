import subprocess
import json
import shutil
from pathlib import Path


def get_max_dimension(video_path: Path) -> int | None:
    """
    Извлекает наибольшую из ширины или высоты видеофайла.

    Args:
        video_path: Объект Path, указывающий на видеофайл.

    Returns:
        Наибольшая сторона видео как int, или None, если информация не найдена.
    """
    try:
        command = [
            'ffprobe',
            '-v', 'error',
            '-select_streams', 'v:0',
            '-show_entries', 'stream=width,height',
            '-of', 'json',
            str(video_path)
        ]
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        video_info = json.loads(result.stdout)

        width = video_info['streams'][0].get('width')
        height = video_info['streams'][0].get('height')

        if width is not None and height is not None:
            return max(width, height)
        else:
            print(f"Предупреждение: Не удалось получить размеры для {video_path}")
            return None
    except (subprocess.CalledProcessError, json.JSONDecodeError, IndexError) as e:
        print(f"Ошибка при получении размеров для {video_path}: {e}")
        return None


def find_video_and_metadata_files(source_folder: Path) -> list[tuple[Path, Path | None]]:
    """
    Находит все пути к MP4 видеофайлам и их соответствующим файлам метаданных JSON
    в указанной папке и её подпапках с неограниченной глубиной.

    Args:
        source_folder: Объект Path, указывающий на исходную папку.

    Returns:
        Список кортежей. Каждый кортеж содержит (путь_к_видео, путь_к_метаданным).
        Путь_к_метаданным может быть None, если файл не найден.
    """
    if not source_folder.is_dir():
        print(f"Ошибка: Исходная папка не найдена или не является директорией: {source_folder}")
        return []

    video_files_found = []
    # **/* означает поиск во всех подпапках на любой глубине
    video_files_lower = set(source_folder.rglob('*.mp4'))
    video_files_upper = set(source_folder.rglob('*.MP4'))
    for video_file in video_files_lower.union(video_files_upper):
        # Определяем ожидаемый путь к папке metadata
        # Например, для D:\path\to\video.mp4, metadata_dir_path будет D:\path\to\metadata
        parent_dir = video_file.parent
        metadata_dir_path = parent_dir / "metadata"

        # Определяем имя файла метаданных: original_video_name.mp4.json
        metadata_file_name = f"{video_file.name}.json"
        metadata_file_path = metadata_dir_path / metadata_file_name

        if metadata_file_path.is_file():
            video_files_found.append((video_file, metadata_file_path))
        else:
            print(f"Предупреждение: Файл метаданных '{metadata_file_path}' не найден для '{video_file}'")
            video_files_found.append((video_file, None))  # Добавляем None, если метаданных нет

    return video_files_found


def create_directory_if_not_exists(directory_path: Path):
    """
    Создает папку, если она ещё не существует.

    Args:
        directory_path: Объект Path, указывающий на путь к создаваемой папке.
    """
    try:
        directory_path.mkdir(parents=True, exist_ok=True)
        # print(f"Папка создана или уже существует: {directory_path}") # Закомментируем, чтобы не спамило
    except OSError as e:
        print(f"Ошибка при создании папки {directory_path}: {e}")


def copy_files_to_directory(source_video_file: Path, source_metadata_file: Path | None,
                            destination_directory: Path):
    """
    Копирует видеофайл и, если есть, соответствующий файл метаданных,
    пропуская файлы, которые уже существуют в папке назначения.
    """

    # --- Проверка и копирование видеофайла ---
    dest_video_path = destination_directory / source_video_file.name

    if dest_video_path.is_file():
        # Добавляем условие пропуска
        print(f"  ➡️ Видеофайл {source_video_file.name} уже существует в целевой папке. Пропуск.")
    else:
        try:
            # Копируем видеофайл
            shutil.copy2(source_video_file, dest_video_path)
            print(f"  Копирование видео: {source_video_file.name} -> {destination_directory}")
        except shutil.Error as e:
            print(f"  ❌ Ошибка при копировании видео {source_video_file.name}: {e}")
            return  # Выходим, если не удалось скопировать видео

    # --- Проверка и копирование метаданных ---
    if source_metadata_file:
        destination_metadata_dir = destination_directory / "metadata"
        create_directory_if_not_exists(destination_metadata_dir)  # Создаем папку metadata

        dest_metadata_path = destination_metadata_dir / source_metadata_file.name

        if dest_metadata_path.is_file():
            # Добавляем условие пропуска для метаданных
            print(f"  ➡️ Метаданные {source_metadata_file.name} уже существуют. Пропуск.")
        else:
            try:
                # Копируем метаданные
                shutil.copy2(source_metadata_file, dest_metadata_path)
                print(f"  Копирование метаданных: {source_metadata_file.name} -> {destination_metadata_dir}")
            except shutil.Error as e:
                print(f"  ❌ Ошибка при копировании метаданных {source_metadata_file.name}: {e}")
    else:
        print(f"  Файл метаданных для {source_video_file.name} не найден, пропуск копирования метаданных.")


def main():
    """
    Основной метод скрипта. Координирует процесс сортировки видео и их метаданных.
    """
    # --- НАСТРОЙКИ ---
    # Путь к исходной папке с видео
    SOURCE_VIDEOS_DIR = Path("E:\\userdata\\encode\\input")  # Укажи свой путь
    # Базовая папка для отсортированных видео
    DESTINATION_BASE_DIR = Path("E:\\userdata\\encode\\input")  # Укажи свой путь

    # Определения категорий разрешения и их порогов (наибольшая сторона)
    RESOLUTION_CATEGORIES = [
        ("low_res", 719),
        ("hd_res", 2159),
        ("4k_res", 2879),
        ("5k_plus_res", float('inf'))
    ]
    # -----------------

    print(f"Начало сортировки видео из '{SOURCE_VIDEOS_DIR}' в '{DESTINATION_BASE_DIR}'")
    create_directory_if_not_exists(DESTINATION_BASE_DIR)

    video_and_metadata_files = find_video_and_metadata_files(SOURCE_VIDEOS_DIR)
    if not video_and_metadata_files:
        print("Видеофайлы MP4 не найдены в указанной папке.")
        return

    for video_file, metadata_file in video_and_metadata_files:
        print(f"\nОбработка файла: {video_file.name}")
        max_dim = get_max_dimension(video_file)

        if max_dim is None:
            print(f"Пропуск файла {video_file.name} из-за невозможности определить разрешение.")
            continue

        destination_category_name = "uncategorized"

        for category_name, max_res_threshold in RESOLUTION_CATEGORIES:
            if max_dim <= max_res_threshold:
                destination_category_name = category_name
                break

        # Определяем конечную папку для видео (например, 'sorted_videos_by_resolution/hd_res')
        destination_folder = DESTINATION_BASE_DIR / destination_category_name
        create_directory_if_not_exists(destination_folder)  # Создаем папку для категории (e.g., hd_res)

        # Копируем видео и метаданные
        copy_files_to_directory(video_file, metadata_file, destination_folder)

    print("\nСортировка завершена!")
    print(f"Видео и метаданные отсортированы в подпапки в '{DESTINATION_BASE_DIR}'")


if __name__ == "__main__":
    main()