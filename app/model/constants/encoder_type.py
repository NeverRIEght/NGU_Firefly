from enum import Enum
from typing import Optional, Union

from app.model.constants.encoder_properties import EncoderProperties
from app.model.constants.video_codec import VideoCodec
from app.model.constants.video_container import VideoContainer


class EncoderType(Enum):
    LIBSVTAV1 = EncoderProperties(
        ffmpeg_name="libsvtav1",
        target_codec=VideoCodec.AV1,
        min_crf=0,
        max_crf=63,
        presets=tuple(str(i) for i in range(14)),
    )
    LIBX264 = EncoderProperties(
        ffmpeg_name="libx264",
        target_codec=VideoCodec.H264,
        min_crf=0,
        max_crf=51,
        presets=(
            "ultrafast",
            "superfast",
            "veryfast",
            "faster",
            "fast",
            "medium",
            "slow",
            "slower",
            "veryslow",
            "placebo",
        ),
    )
    LIBX265 = EncoderProperties(
        ffmpeg_name="libx265",
        target_codec=VideoCodec.HEVC,
        min_crf=0,
        max_crf=51,
        presets=(
            "ultrafast",
            "superfast",
            "veryfast",
            "faster",
            "fast",
            "medium",
            "slow",
            "slower",
            "veryslow",
            "placebo",
        ),
    )

    @property
    def ffmpeg_name(self) -> str:
        return self.value.ffmpeg_name

    @property
    def target_codec(self) -> VideoCodec:
        return self.value.target_codec

    @property
    def min_crf(self) -> int:
        return self.value.min_crf

    @property
    def max_crf(self) -> int:
        return self.value.max_crf

    @property
    def presets(self) -> tuple[str, ...]:
        return self.value.presets

    def is_valid_crf(self, crf: Optional[int]) -> bool:
        if crf is None:
            return False
        return self.min_crf <= crf <= self.max_crf

    def is_valid_preset(self, preset: Optional[Union[str, int]]) -> bool:
        if preset is None:
            return False
        return str(preset).strip().lower() in self.presets

    def supports_container(self, container: Optional[VideoContainer]) -> bool:
        if container is None:
            return False
        return container.supports_codec(self.target_codec)

    @classmethod
    def from_ffmpeg_name(cls, name: Optional[str]) -> Optional["EncoderType"]:
        if not name:
            return None
        cleaned_name = name.strip().lower()
        for encoder in cls:
            if encoder.ffmpeg_name == cleaned_name:
                return encoder
        return None
