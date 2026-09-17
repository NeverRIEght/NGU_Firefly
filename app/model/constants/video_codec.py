from enum import Enum
from typing import Optional

from app.model.constants.codec_properties import CodecProperties


class VideoCodec(Enum):
    AV1 = CodecProperties(
        ffmpeg_name="av1",
        display_name="AV1",
        aliases=("av1", "av01"),
    )
    H264 = CodecProperties(
        ffmpeg_name="h264",
        display_name="H.264",
        aliases=("h264", "avc", "avc1"),
    )
    HEVC = CodecProperties(
        ffmpeg_name="hevc",
        display_name="H.265",
        aliases=("hevc", "h265", "hvc1", "hev1"),
    )
    VP9 = CodecProperties(
        ffmpeg_name="vp9",
        display_name="VP9",
        aliases=("vp9", "vp09"),
    )

    @property
    def ffmpeg_name(self) -> str:
        return self.value.ffmpeg_name

    @property
    def display_name(self) -> str:
        return self.value.display_name

    @property
    def aliases(self) -> tuple[str, ...]:
        return self.value.aliases

    @classmethod
    def from_probe_name(cls, name: Optional[str]) -> Optional["VideoCodec"]:
        if not name:
            return None
        cleaned_name = name.strip().lower()
        for codec in cls:
            if cleaned_name == codec.ffmpeg_name.lower() or cleaned_name in codec.aliases:
                return codec
        return None

    @classmethod
    def get_by_ffmpeg_name(cls, name: Optional[str]) -> Optional["VideoCodec"]:
        if not name:
            return None
        cleaned_name = name.strip().lower()
        for codec in cls:
            if codec.ffmpeg_name == cleaned_name:
                return codec
        return None
