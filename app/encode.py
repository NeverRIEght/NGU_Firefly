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

log.info("\n\n-----STARTING SESSION-----\n\n")

import os
import subprocess
import json
from dotenv import load_dotenv, find_dotenv

VIDEO_DURATION_SECONDS_TOLERANCE = 0.2
THREADS_IF_SILENT = 1


def delete_file(file_path: Path) -> bool:
    """
    Deletes the file if it exists.

    Args:
        file_path: Path to the file to be deleted.

    Returns:
        True if the file was deleted, False if it did not exist or could not be deleted.
    """
    if file_path.is_file():
        try:
            file_path.unlink()
            log.info(f"Deleted file: {file_path}")
            return True
        except OSError as e:
            log.error(f"Error deleting file {file_path}. Details: \n{e}")
            return False
    return False


def get_short_path(file_path: Path) -> str:
    """
    Return the short path of the file, including its parent folder name.
    Separator is extracted from the file system, so it works on both Windows and Unix-like systems.
    Examples:
        - "/parent_folder/file_name.ext"
    """
    if not isinstance(file_path, Path):
        file_path = Path(file_path)

    parent_folder_name = file_path.parent.name
    file_name = file_path.name
    separator = os.path.sep

    return f"{separator}{parent_folder_name}{separator}{file_name}"


def get_video_duration(video_path: Path) -> float | None:
    short_path = get_short_path(video_path)
    log.debug(f"Getting video duration for: {video_path}")
    try:
        command = [
            'ffprobe',
            '-v', 'error',
            '-select_streams', 'v:0',
            '-show_entries', 'format=duration',
            '-of', 'json',
            str(video_path)
        ]
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        duration_info = json.loads(result.stdout)
        duration = float(duration_info['format']['duration'])
        if duration < 0:
            log.warning(f"Negative duration for {short_path}. Returning None.")
            return None
        log.debug(f"Got duration for {short_path}: {duration} seconds")
        return duration
    except (subprocess.CalledProcessError, KeyError, ValueError):
        log.error(f"Error getting duration for {video_path}. Returning None.")
        return None


def compare_video_durations(video_file_1_path: Path, video_file_2_path: Path, duration_tolerance: float) -> bool:
    """
    Compares the durations of two video files.

    Args:
        video_file_1_path: Path to the first video file.
        video_file_2_path: Path to the second video file.
        duration_tolerance: Tolerance in seconds for the duration comparison.

    Returns:
        True if the durations are equal, False otherwise.
    """
    log.info(f"Comparing durations for {video_file_1_path} and {video_file_2_path}")
    short_path_1 = get_short_path(video_file_1_path)
    short_path_2 = get_short_path(video_file_2_path)

    duration_1 = get_video_duration(video_file_1_path)
    duration_2 = get_video_duration(video_file_2_path)

    if duration_1 is None or duration_2 is None:
        log.error(f"Could not compare durations for {video_file_1_path} and {video_file_2_path}.")
        return False

    if abs(duration_1 - duration_2) < duration_tolerance:
        log.info(f"Durations match:"
                 f"\n{short_path_1}: {duration_1} seconds"
                 f"\n{short_path_2}: {duration_2} seconds")
        return True
    else:
        log.info(f"Durations do not match:"
                 f"\n{short_path_1}: {duration_1} seconds"
                 f"\n{short_path_2}: {duration_2} seconds")
        return False


def encode_video_with_ffmpeg(
        input_video_path: Path,
        output_video_path: Path,
        crf: int,
        preset: str,
        audio_codec: str = 'copy',
        is_silent: bool = False
) -> bool:
    """
    Encodes the video file using FFmpeg and H.265 codec (libx265).

    Args:
        input_video_path: Path to the input video file.
        output_video_path: Path to the output video file.
        crf: Value of Constant Rate Factor (CRF).
        preset: Preset value for libx265 ('medium', 'slow', 'veryslow', etc.)
        audio_codec: Audio codec to use for the output video (default is 'copy').
        is_silent: Uses reduced number of threads if True (default is False).
    Returns:
        True, if file was successfully encoded and has the same duration as the input file.
        False, if there was an error during encoding or the durations do not match.
    """
    output_video_path.parent.mkdir(parents=True, exist_ok=True)

    if not input_video_path.is_file():
        log.info(f"Error: Input file {input_video_path} does not exist. Skip.")
        return False

    if output_video_path.is_file():
        log.info(f"Output file {output_video_path} already exists. Checking duration...")
        is_same_duration = compare_video_durations(
            input_video_path,
            output_video_path,
            VIDEO_DURATION_SECONDS_TOLERANCE
        )
        short_path = get_short_path(output_video_path)
        if is_same_duration:
            log.info(f"'{short_path}' is already encoded with the same duration. Skip.")
            return True

        if not is_same_duration:
            log.warning(f"'{short_path}' is partially encoded. Deleting and re-encoding.")
            delete_file(output_video_path)

    threads_count = os.cpu_count()
    if is_silent:
        threads_count = THREADS_IF_SILENT

    command = [
        'ffmpeg',
        '-i', str(input_video_path),
        '-c:v', 'libx265',
        '-x265-params', f'crf={str(crf)}:pools={str(threads_count)}',
        '-preset', preset,
        '-tag:v', 'hvc1',
        '-profile:v', 'main',
        '-level:v', '5.1',
        '-pix_fmt', 'yuv420p',
        '-c:a', audio_codec,
        '-map', '0:v:0',
        '-map', '0:a?',
        '-map_metadata', '0',
        '-map_chapters', '0',
        '-movflags', '+faststart',
        str(output_video_path),
        '-loglevel', 'error',
        '-hide_banner'
    ]
    if is_silent:
        command.extend(['-threads', str(1)])

    short_input_path = get_short_path(input_video_path)
    log.info(f"Starting encode for : {short_input_path}")

    process = None
    try:
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1)
        _, stderr = process.communicate()

        if process.returncode == 0:
            # Третий вызов ffprobe - для финальной проверки
            is_same_duration = compare_video_durations(
                input_video_path,
                output_video_path,
                VIDEO_DURATION_SECONDS_TOLERANCE
            )
            if is_same_duration:
                log.info("Encoding finished successfully.")
                return True
            else:
                log.error(
                    f"'{short_input_path}' is encoded, but duration is invalid."
                    f"Removing file: {output_video_path}"
                )
                delete_file(output_video_path)
                return False
        else:
            log.error(f"Error while encoding the file: '{input_video_path}'."
                      f"\nReturn code: {process.returncode}")
            log.error(f"FFmpeg Error Output:\n{stderr}")
            log.info("Deleting incomplete output file.")
            delete_file(output_video_path)
            return False
    except FileNotFoundError:
        log.error("FFmpeg not found. Please check your installation and PATH settings.")
        return False
    except KeyboardInterrupt:
        log.info("Encoding interrupted by user")
        if process:
            process.kill()
        log.info("Deleting incomplete output file.")
        delete_file(output_video_path)
        raise
    except Exception as e:
        log.info(f"Unexpected error while encoding file: '{input_video_path}'. Details:\n{e}")
        log.info("Deleting incomplete output file.")
        delete_file(output_video_path)
        return False


def main():
    load_dotenv(find_dotenv())
    base_directory = Path(os.getenv("INPUT_DIR"))
    output_directory = Path(os.getenv("OUTPUT_DIR"))
    is_silent = False

    # Settings for encoding:
    # sub_folder_in_base_folder_name: {crf: int, preset: str}
    encoding_settings = {
        "low_res": {"crf": 25, "preset": "veryslow"},
        "hd_res": {"crf": 26, "preset": "veryslow"},
        "4k_res": {"crf": 28, "preset": "veryslow"},
        "5k_plus_res": {"crf": 29, "preset": "veryslow"}
    }

    output_directory.mkdir(parents=True, exist_ok=True)

    log.info(f"Composing tasks for encoding in '{base_directory}'...")
    all_tasks = []
    for category_name, settings in encoding_settings.items():
        source_category_dir = base_directory / category_name
        destination_category_dir = output_directory / category_name

        if source_category_dir.is_dir():
            video_files_lower = set(source_category_dir.rglob('*.mp4'))
            video_files_upper = set(source_category_dir.rglob('*.MP4'))
            video_files = video_files_lower.union(video_files_upper)
            for video_file in video_files:
                log.info(f"Composing task for '{video_file}'...")
                output_video_path = destination_category_dir / video_file.name
                all_tasks.append({
                    "input_path": video_file,
                    "output_path": output_video_path,
                    "crf": settings["crf"],
                    "preset": settings["preset"]
                })

    all_tasks.sort(key=lambda t: (t['input_path']), reverse=True)

    total_files_to_process = len(all_tasks)
    if total_files_to_process == 0:
        log.warning("No files found to encode. Exiting.")
        return

    log.info(f"Found {total_files_to_process} files to encode in {len(encoding_settings)} categories.")

    try:
        files_processed = 0

        for task in all_tasks:
            log.info("-" * 20)
            log.info(
                f"Processing file: '{task['input_path']}' (CRF={task['crf']}, Preset={task['preset']})")

            success = encode_video_with_ffmpeg(
                input_video_path=task['input_path'],
                output_video_path=task['output_path'],
                crf=task['crf'],
                preset=task['preset'],
                is_silent=is_silent
            )

            if success:
                files_processed += 1
                log.info(f"Encoding finished: {task['output_path'].name}")
                log.info(f"Progress: {files_processed} of {total_files_to_process} files completed.")

            log.info("-" * 20)

        log.info(f"Encoding completed. Total files processed: {total_files_to_process}.")

    except KeyboardInterrupt:
        log.info("Script terminated by user.")


if __name__ == "__main__":
    main()
