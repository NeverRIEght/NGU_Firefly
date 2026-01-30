from pydantic import BaseModel


class EncoderSettings(BaseModel):
    encoder: str
    preset: str
    crf: int
    cpu_threads_to_use: int