from dataclasses import dataclass

from app.model.constants.video_codec import VideoCodec


@dataclass(frozen=True)
class EncoderProperties:
    # Canonical FFmpeg encoder library backend name (e.g. 'libsvtav1', 'libx265', 'libx264').
    # Used directly with the FFmpeg video codec flag: `-c:v <ffmpeg_name>` (e.g. `-c:v libx265`).
    ffmpeg_name: str

    target_codec: VideoCodec
    min_crf: int
    max_crf: int
    presets: tuple[str, ...]
