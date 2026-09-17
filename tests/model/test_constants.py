from dataclasses import FrozenInstanceError

import pytest

from app.model.constants import (
    CodecProperties,
    ContainerProperties,
    EncoderProperties,
    EncoderType,
    VideoCodec,
    VideoContainer,
)


class TestConstants:
    def test_video_codec_from_probe_name(self) -> None:
        assert VideoCodec.from_probe_name("hevc") == VideoCodec.HEVC
        assert VideoCodec.from_probe_name("h265") == VideoCodec.HEVC
        assert VideoCodec.from_probe_name("HVC1") == VideoCodec.HEVC
        assert VideoCodec.from_probe_name("hev1") == VideoCodec.HEVC

        assert VideoCodec.from_probe_name("h264") == VideoCodec.H264
        assert VideoCodec.from_probe_name("AVC") == VideoCodec.H264
        assert VideoCodec.from_probe_name("avc1") == VideoCodec.H264

        assert VideoCodec.from_probe_name("av1") == VideoCodec.AV1
        assert VideoCodec.from_probe_name("av01") == VideoCodec.AV1

        assert VideoCodec.from_probe_name("vp9") == VideoCodec.VP9
        assert VideoCodec.from_probe_name("vp09") == VideoCodec.VP9

    def test_video_codec_get_by_ffmpeg_name(self) -> None:
        assert VideoCodec.get_by_ffmpeg_name("hevc") == VideoCodec.HEVC
        assert VideoCodec.get_by_ffmpeg_name("h264") == VideoCodec.H264
        assert VideoCodec.get_by_ffmpeg_name("av1") == VideoCodec.AV1
        assert VideoCodec.get_by_ffmpeg_name("vp9") == VideoCodec.VP9

        # Aliases are not canonical ffmpeg names
        assert VideoCodec.get_by_ffmpeg_name("h265") is None
        assert VideoCodec.get_by_ffmpeg_name("avc") is None

    def test_video_codec_invalid_lookup(self) -> None:
        assert VideoCodec.from_probe_name("unknown_codec") is None
        assert VideoCodec.from_probe_name("") is None
        assert VideoCodec.from_probe_name(None) is None
        assert VideoCodec.get_by_ffmpeg_name("unknown_codec") is None
        assert VideoCodec.get_by_ffmpeg_name("") is None
        assert VideoCodec.get_by_ffmpeg_name(None) is None

    def test_ffmpeg_name_consistency_across_constants(self) -> None:
        # VideoCodec uses ffmpeg_name for stream decoding / probing
        assert VideoCodec.HEVC.ffmpeg_name == "hevc"
        assert VideoCodec.H264.ffmpeg_name == "h264"
        assert VideoCodec.AV1.ffmpeg_name == "av1"
        assert VideoCodec.VP9.ffmpeg_name == "vp9"

        # VideoContainer uses ffmpeg_name for the `-f` muxer flag
        assert VideoContainer.MKV.ffmpeg_name == "matroska"
        assert VideoContainer.MOV.ffmpeg_name == "mov"
        assert VideoContainer.MP4.ffmpeg_name == "mp4"
        assert VideoContainer.WEBM.ffmpeg_name == "webm"

        # EncoderType uses ffmpeg_name for the `-c:v` encoder flag
        assert EncoderType.LIBSVTAV1.ffmpeg_name == "libsvtav1"
        assert EncoderType.LIBX264.ffmpeg_name == "libx264"
        assert EncoderType.LIBX265.ffmpeg_name == "libx265"

    def test_video_container_from_extension(self) -> None:
        assert VideoContainer.from_extension(".mp4") == VideoContainer.MP4
        assert VideoContainer.from_extension("mp4") == VideoContainer.MP4
        assert VideoContainer.from_extension(".MP4") == VideoContainer.MP4

        assert VideoContainer.from_extension(".m4v") == VideoContainer.MP4
        assert VideoContainer.from_extension("m4v") == VideoContainer.MP4

        assert VideoContainer.from_extension(".mkv") == VideoContainer.MKV
        assert VideoContainer.from_extension("mkv") == VideoContainer.MKV

        assert VideoContainer.from_extension(".webm") == VideoContainer.WEBM
        assert VideoContainer.from_extension("webm") == VideoContainer.WEBM

        assert VideoContainer.from_extension(".mov") == VideoContainer.MOV
        assert VideoContainer.from_extension("mov") == VideoContainer.MOV

    def test_video_container_invalid_extension(self) -> None:
        assert VideoContainer.from_extension(".txt") is None
        assert VideoContainer.from_extension(".nfo") is None
        assert VideoContainer.from_extension("") is None
        assert VideoContainer.from_extension(None) is None

    def test_video_container_supports_codec(self) -> None:
        # MKV supports all Phase 1 codecs
        assert VideoContainer.MKV.supports_codec(VideoCodec.AV1) is True
        assert VideoContainer.MKV.supports_codec(VideoCodec.H264) is True
        assert VideoContainer.MKV.supports_codec(VideoCodec.HEVC) is True
        assert VideoContainer.MKV.supports_codec(VideoCodec.VP9) is True

        # MP4 supports AV1, H264, HEVC; does not support VP9
        assert VideoContainer.MP4.supports_codec(VideoCodec.AV1) is True
        assert VideoContainer.MP4.supports_codec(VideoCodec.H264) is True
        assert VideoContainer.MP4.supports_codec(VideoCodec.HEVC) is True
        assert VideoContainer.MP4.supports_codec(VideoCodec.VP9) is False

        # WEBM supports AV1, VP9; does not support H264, HEVC
        assert VideoContainer.WEBM.supports_codec(VideoCodec.AV1) is True
        assert VideoContainer.WEBM.supports_codec(VideoCodec.VP9) is True
        assert VideoContainer.WEBM.supports_codec(VideoCodec.H264) is False
        assert VideoContainer.WEBM.supports_codec(VideoCodec.HEVC) is False

        # MOV supports H264, HEVC; does not support AV1, VP9
        assert VideoContainer.MOV.supports_codec(VideoCodec.H264) is True
        assert VideoContainer.MOV.supports_codec(VideoCodec.HEVC) is True
        assert VideoContainer.MOV.supports_codec(VideoCodec.AV1) is False
        assert VideoContainer.MOV.supports_codec(VideoCodec.VP9) is False

        # None codec check returns False
        assert VideoContainer.MKV.supports_codec(None) is False

    def test_encoder_type_crf_bounds(self) -> None:
        svtav1 = EncoderType.LIBSVTAV1
        assert svtav1.is_valid_crf(0) is True
        assert svtav1.is_valid_crf(30) is True
        assert svtav1.is_valid_crf(63) is True
        assert svtav1.is_valid_crf(64) is False
        assert svtav1.is_valid_crf(-1) is False
        assert svtav1.is_valid_crf(None) is False

        x265 = EncoderType.LIBX265
        assert x265.is_valid_crf(0) is True
        assert x265.is_valid_crf(28) is True
        assert x265.is_valid_crf(51) is True
        assert x265.is_valid_crf(52) is False
        assert x265.is_valid_crf(-1) is False
        assert x265.is_valid_crf(None) is False

        x264 = EncoderType.LIBX264
        assert x264.is_valid_crf(0) is True
        assert x264.is_valid_crf(23) is True
        assert x264.is_valid_crf(51) is True
        assert x264.is_valid_crf(52) is False
        assert x264.is_valid_crf(-1) is False
        assert x264.is_valid_crf(None) is False

    def test_encoder_type_presets(self) -> None:
        svtav1 = EncoderType.LIBSVTAV1
        assert svtav1.is_valid_preset(0) is True
        assert svtav1.is_valid_preset("0") is True
        assert svtav1.is_valid_preset(1) is True
        assert svtav1.is_valid_preset("1") is True
        assert svtav1.is_valid_preset(13) is True
        assert svtav1.is_valid_preset("13") is True
        assert svtav1.is_valid_preset(14) is False
        assert svtav1.is_valid_preset("slow") is False
        assert svtav1.is_valid_preset(None) is False

        x265 = EncoderType.LIBX265
        assert x265.is_valid_preset("ultrafast") is True
        assert x265.is_valid_preset("slow") is True
        assert x265.is_valid_preset("SLOW") is True
        assert x265.is_valid_preset("placebo") is True
        assert x265.is_valid_preset(1) is False
        assert x265.is_valid_preset("invalid") is False
        assert x265.is_valid_preset(None) is False

        x264 = EncoderType.LIBX264
        assert x264.is_valid_preset("ultrafast") is True
        assert x264.is_valid_preset("medium") is True
        assert x264.is_valid_preset("veryfast") is True
        assert x264.is_valid_preset(0) is False
        assert x264.is_valid_preset(None) is False

    def test_encoder_type_supports_container(self) -> None:
        svtav1 = EncoderType.LIBSVTAV1
        assert svtav1.supports_container(VideoContainer.MKV) is True
        assert svtav1.supports_container(VideoContainer.MP4) is True
        assert svtav1.supports_container(VideoContainer.WEBM) is True
        assert svtav1.supports_container(VideoContainer.MOV) is False
        assert svtav1.supports_container(None) is False

        x265 = EncoderType.LIBX265
        assert x265.supports_container(VideoContainer.MKV) is True
        assert x265.supports_container(VideoContainer.MP4) is True
        assert x265.supports_container(VideoContainer.MOV) is True
        assert x265.supports_container(VideoContainer.WEBM) is False
        assert x265.supports_container(None) is False

        x264 = EncoderType.LIBX264
        assert x264.supports_container(VideoContainer.MKV) is True
        assert x264.supports_container(VideoContainer.MP4) is True
        assert x264.supports_container(VideoContainer.MOV) is True
        assert x264.supports_container(VideoContainer.WEBM) is False
        assert x264.supports_container(None) is False

    def test_encoder_type_from_ffmpeg_name(self) -> None:
        assert EncoderType.from_ffmpeg_name("libsvtav1") == EncoderType.LIBSVTAV1
        assert EncoderType.from_ffmpeg_name("libx265") == EncoderType.LIBX265
        assert EncoderType.from_ffmpeg_name("libx264") == EncoderType.LIBX264
        assert EncoderType.from_ffmpeg_name("unknown") is None
        assert EncoderType.from_ffmpeg_name("") is None
        assert EncoderType.from_ffmpeg_name(None) is None

    def test_constants_are_immutable(self) -> None:
        # Properties on Enum members cannot be assigned
        with pytest.raises(AttributeError):
            setattr(EncoderType.LIBSVTAV1, "min_crf", 10)

        with pytest.raises(AttributeError):
            setattr(VideoContainer.MKV, "primary_extension", ".avi")

        with pytest.raises(AttributeError):
            setattr(VideoCodec.HEVC, "ffmpeg_name", "h265")

        # Underlying properties dataclasses are frozen and cannot be modified
        assert isinstance(EncoderType.LIBSVTAV1.value, EncoderProperties)
        with pytest.raises(FrozenInstanceError):
            setattr(EncoderType.LIBSVTAV1.value, "min_crf", 10)

        assert isinstance(VideoContainer.MKV.value, ContainerProperties)
        with pytest.raises(FrozenInstanceError):
            setattr(VideoContainer.MKV.value, "primary_extension", ".avi")

        assert isinstance(VideoCodec.HEVC.value, CodecProperties)
        with pytest.raises(FrozenInstanceError):
            setattr(VideoCodec.HEVC.value, "ffmpeg_name", "h265")

