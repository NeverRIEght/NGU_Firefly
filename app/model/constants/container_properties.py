from dataclasses import dataclass

from app.model.constants.video_codec import VideoCodec


@dataclass(frozen=True)
class ContainerProperties:
    # Canonical FFmpeg muxer / container format name (e.g. 'matroska', 'mp4', 'webm').
    # Used directly with the FFmpeg format flag: `-f <ffmpeg_name>` (e.g. `-f matroska`).
    ffmpeg_name: str

    primary_extension: str
    aliases: tuple[str, ...]
    supported_codecs: tuple[VideoCodec, ...]
