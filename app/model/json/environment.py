from pydantic import BaseModel


class Environment(BaseModel):
    script_version: str
    ffmpeg_version: str
    compression_engine_version: int
    cpu_name: str
    cpu_threads: int