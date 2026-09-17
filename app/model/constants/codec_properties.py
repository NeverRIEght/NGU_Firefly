from dataclasses import dataclass


@dataclass(frozen=True)
class CodecProperties:
    # Canonical FFmpeg codec name (e.g. 'hevc', 'h264', 'av1').
    # Matches ffprobe stream property `codec_name` when reading input video tracks.
    ffmpeg_name: str

    display_name: str
    aliases: tuple[str, ...]
