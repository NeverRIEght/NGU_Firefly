import logging

from app.model.encoder_data_json import EncoderDataJson

log = logging.getLogger(__name__)

from pathlib import Path


def serialize_to_json(job_object: EncoderDataJson, output_path: str | Path):
    p = Path(output_path)

    p.parent.mkdir(parents=True, exist_ok=True)

    try:
        json_string = job_object.model_dump_json(indent=4)

        with open(p, "w", encoding="utf-8") as f:
            f.write(json_string)

        log.info(f"Json saved successfully: {p.resolve()}")

    except Exception as e:
        log.error(f"Error serializing json. Output path: {output_path}. Exception: {e}")


def load_from_json(input_path: str | Path) -> EncoderDataJson:
    p = Path(input_path)

    if not p.is_file():
        raise FileNotFoundError(f"File not found for serialization: {p.resolve()}")

    try:
        json_content = p.read_text(encoding="utf-8")
        job_object = EncoderDataJson.model_validate_json(json_content)

        log.info(f"Json loaded: {p.resolve()}")
        return job_object

    except Exception as e:
        raise ValueError(f"Error loading json file. Input path: {input_path}. Exception: {e}")
