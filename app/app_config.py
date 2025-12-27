import threading
from pydantic import BaseModel, Field
from typing import Optional

class AppConfig(BaseModel):
    app_name: str = Field("video_encoder", description="The name of the application")
    version: str = Field("0.7.1", description="The version of the application")
    input_dir: str = Field(..., description="Directory with initial videos of mp4 format. Will be scanned recursively without limitation of depth.")
    output_dir: str = Field(..., description="Directory where processed videos will be saved.")
    is_silent: bool = Field(False, description="Silent mode, which uses only one thread.")
    crf_min: int = Field(12, description="CRF values for binary search - minimum")
    crf_max: int = Field(40, description="CRF values for binary search - maximum")
    vmaf_min: float = Field(96.0, description="Minimum acceptable VMAF score for encoded videos")
    vmaf_max: float = Field(97.0, description="Maximum acceptable VMAF score for encoded videos")
    efficiency_threshold: float = Field(0.25, description="Minimum VMAF improvement between iterations to continue searching")
    initial_crf: int = Field(26, description="Initial CRF value")
    encode_preset: str = Field("veryslow", description="Encode preset for libx265 encoder")


def load_config_from_env() -> AppConfig:
    import os
    from dotenv import load_dotenv, find_dotenv

    load_dotenv(find_dotenv())

    config = AppConfig(
        input_dir=os.getenv("INPUT_DIR", ""),
        output_dir=os.getenv("OUTPUT_DIR", ""),
        is_silent=os.getenv("IS_SILENT", "False").lower() in ("true", "True", "1", "yes"),
        crf_min=int(os.getenv("CRF_MIN", "12")),
        crf_max=int(os.getenv("CRF_MAX", "40")),
        vmaf_min=float(os.getenv("VMAF_MIN", "96.0")),
        vmaf_max=float(os.getenv("VMAF_MAX", "97.0")),
        efficiency_threshold=float(os.getenv("EFFICIENCY_THRESHOLD", "0.25")),
        initial_crf=int(os.getenv("INITIAL_CRF", "26")),
        encode_preset=os.getenv("ENCODE_PRESET", "veryslow"),
    )

    return config


class ConfigManager:
    _instance: Optional[AppConfig] = None
    _lock = threading.Lock()

    def __init__(self):
        raise RuntimeError("Constructor is not allowed. Use get_config() method.")

    @classmethod
    def get_config(cls) -> AppConfig:
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = load_config_from_env()

        return cls._instance