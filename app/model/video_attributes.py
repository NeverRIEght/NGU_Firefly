from pydantic import BaseModel


class VideoAttributes(BaseModel):
    codec: str
    width_px: int
    height_px: int
    duration_seconds: float
    fps: float
    average_bitrate_kilobits_per_second: float
