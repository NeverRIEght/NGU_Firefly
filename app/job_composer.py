import logging
from pathlib import Path

from app.app_config import ConfigManager
from app.model import VideoEncodingJob

from app.json_serializer import load_from_json

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


def compose_jobs() -> list[EncodingJobContext]:
    app_config = ConfigManager.get_config()
    log.info(f"Starting to compose jobs for input directory: {app_config.input_dir}")

    jobs = []

    input_dir = Path(app_config.input_dir)

    for item in input_dir.iterdir():
        if item.is_file() and item.suffix.lower() == ".mp4":
            current_file = item

            log.info(f"Composing job for file: {current_file}")

            json_path = find_associated_metadata_json_file(current_file)
            if json_path is None:
                json_name = get_json_name_for_video_file(current_file)
                log.info(f"Json metadata file not found for {current_file}, creating new json file: {json_name}")
                new_json_path = current_file.parent / json_name

                # Extract initial metadata from the source video file
                # Save the initial version of EncoderDataJson to actual file
                # Compose EncodingJobContext and add it to the list
            else:
                try:
                    job = load_from_json(json_path)
                    jobs.append(job)
                except Exception as e:
                    log.error(f"Failed to load job from JSON for file {current_file}. Exception: {e}")

    log.info(f"Finished composing jobs. Total jobs composed: {len(jobs)}")
    return jobs


def find_associated_metadata_json_file(video_file_path: Path) -> Path | None:
    json_file_name = get_json_name_for_video_file(video_file_path)
    expected_json_path = video_file_path.parent / json_file_name

    if expected_json_path.exists():
        return expected_json_path
    else:
        return None


def get_json_name_for_video_file(video_file_path: Path) -> str:
    return f"{video_file_path.stem}_encoderdata.json"
