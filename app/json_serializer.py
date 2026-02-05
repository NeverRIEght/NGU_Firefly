import logging
from pathlib import Path

from app.locking import LockManager, LockMode
from app.model.json.job_data import JobData

log = logging.getLogger(__name__)


def serialize_to_json(job_data: JobData, output_path: str | Path):
    p = Path(output_path)

    try:
        json_string = job_data.model_dump_json(indent=4)

        with LockManager.acquire_metadata_lock(p, LockMode.EXCLUSIVE):
            p.parent.mkdir(parents=True, exist_ok=True)

            # Write to temporary file first, then rename (atomic operation in most filesystems)
            temp_path = p.with_suffix('.tmp')
            with open(temp_path, "w", encoding="utf-8") as f:
                f.write(json_string)
            temp_path.replace(p)

        log.debug(f"Json saved successfully: {p.resolve()}")

    except Exception as e:
        log.error(f"Error serializing json. Output path: {output_path}. Exception: {e}")
        raise


def load_from_json(input_path: str | Path) -> JobData:
    p = Path(input_path)

    if not p.is_file():
        raise FileNotFoundError(f"File not found for deserialization: {p.resolve()}")

    with LockManager.acquire_metadata_lock(p, LockMode.SHARED):
        try:
            json_content = p.read_text(encoding="utf-8")
            job_data = JobData.model_validate_json(json_content)

            log.debug(f"Json loaded: {p.resolve()}")
            return job_data

        except Exception as e:
            raise ValueError(f"Error loading json file. Input path: {input_path}. Exception: {e}")
