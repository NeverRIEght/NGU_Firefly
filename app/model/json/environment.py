from pydantic import BaseModel


class Environment(BaseModel):
    script_version: str
    ffmpeg_version: str
    encoder_version: str
    cpu_name: str
    cpu_threads: int