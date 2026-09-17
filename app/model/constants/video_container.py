from enum import Enum
from typing import Optional

from app.model.constants.container_properties import ContainerProperties
from app.model.constants.video_codec import VideoCodec


class VideoContainer(Enum):
    MKV = ContainerProperties(
        primary_extension=".mkv",
        aliases=(),
        ffmpeg_name="matroska",
        supported_codecs=(
            VideoCodec.AV1,
            VideoCodec.H264,
            VideoCodec.HEVC,
            VideoCodec.VP9,
        ),
    )
    MOV = ContainerProperties(
        primary_extension=".mov",
        aliases=(),
        ffmpeg_name="mov",
        supported_codecs=(
            VideoCodec.H264,
            VideoCodec.HEVC,
        ),
    )
    MP4 = ContainerProperties(
        primary_extension=".mp4",
        aliases=(".m4v",),
        ffmpeg_name="mp4",
        supported_codecs=(
            VideoCodec.AV1,
            VideoCodec.H264,
            VideoCodec.HEVC,
        ),
    )
    WEBM = ContainerProperties(
        primary_extension=".webm",
        aliases=(),
        ffmpeg_name="webm",
        supported_codecs=(
            VideoCodec.AV1,
            VideoCodec.VP9,
        ),
    )

    @property
    def primary_extension(self) -> str:
        return self.value.primary_extension

    @property
    def aliases(self) -> tuple[str, ...]:
        return self.value.aliases

    @property
    def ffmpeg_name(self) -> str:
        return self.value.ffmpeg_name

    @property
    def supported_codecs(self) -> tuple[VideoCodec, ...]:
        return self.value.supported_codecs

    @classmethod
    def from_extension(cls, ext: Optional[str]) -> Optional["VideoContainer"]:
        if not ext:
            return None
        cleaned_ext = ext.strip().lower()
        if not cleaned_ext.startswith("."):
            cleaned_ext = f".{cleaned_ext}"
        for container in cls:
            if cleaned_ext == container.primary_extension.lower() or cleaned_ext in container.aliases:
                return container
        return None

    def supports_codec(self, codec: Optional[VideoCodec]) -> bool:
        if codec is None:
            return False
        return codec in self.supported_codecs
