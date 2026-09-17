from datetime import datetime, timezone
from pathlib import Path

import pytest

from app.migrations.legacy_json_job_mapper import LegacyJsonJobMapper
from app.model.dto import HdrFormat, IterationStage, JobStage, SegmentStatus
from app.model.json.encoder_settings import EncoderSettings
from app.model.json.encoding_stage import EncodingStage, EncodingStageNamesEnum
from app.model.json.environment import Environment as JsonEnvironment
from app.model.json.execution_data import ExecutionData as JsonExecutionData
from app.model.json.ffmpeg_metadata import FfmpegMetadata, HdrType
from app.model.json.file_attributes import FileAttributes
from app.model.json.iteration import Iteration as JsonIteration
from app.model.json.job_data import JobData
from app.model.json.source_video import SourceVideo
from app.model.json.video_attributes import VideoAttributes


def test_fps_to_avg_frame_rate_conversions():
    mapper = LegacyJsonJobMapper

    assert mapper._fps_to_avg_frame_rate(23.976) == "24000/1001"
    assert mapper._fps_to_avg_frame_rate(23.976023976) == "24000/1001"
    assert mapper._fps_to_avg_frame_rate(24.0) == "24/1"
    assert mapper._fps_to_avg_frame_rate(25.0) == "25/1"
    assert mapper._fps_to_avg_frame_rate(29.97) == "30000/1001"
    assert mapper._fps_to_avg_frame_rate(29.97002997) == "30000/1001"
    assert mapper._fps_to_avg_frame_rate(30.0) == "30/1"
    assert mapper._fps_to_avg_frame_rate(50.0) == "50/1"
    assert mapper._fps_to_avg_frame_rate(59.94) == "60000/1001"
    assert mapper._fps_to_avg_frame_rate(60.0) == "60/1"
    assert mapper._fps_to_avg_frame_rate(0.0) == "0/1"
    assert mapper._fps_to_avg_frame_rate(-1.0) == "0/1"


def test_maps_completed_legacy_job_to_dto(mock_app_config):
    input_dir = mock_app_config.input_dir
    output_dir = mock_app_config.output_dir

    source_video = SourceVideo(
        file_attributes=FileAttributes(file_name="movie.mkv", file_size_bytes=104857600),
        sha256_hash="source_hash_123",
        video_attributes=VideoAttributes(
            codec="hevc",
            width_px=1920,
            height_px=1080,
            duration_seconds=120.5,
            fps=23.976,
            average_bitrate_kilobits_per_second=5500.0,
        ),
        ffmpeg_metadata=FfmpegMetadata(
            pixel_aspect_ratio="1:1",
            pixel_format="yuv420p10le",
            chroma_sample_location="left",
            color_primaries="bt709",
            color_trc="bt709",
            colorspace="bt709",
            hdr_types={HdrType.DOLBY_VISION},
        ),
    )

    iteration_1 = JsonIteration(
        file_attributes=FileAttributes(file_name="movie_crf20.mkv", file_size_bytes=52428800),
        sha256_hash="iter1_hash",
        video_attributes=VideoAttributes(
            codec="hevc",
            width_px=1920,
            height_px=1080,
            duration_seconds=120.5,
            fps=23.976,
            average_bitrate_kilobits_per_second=3000.0,
        ),
        encoder_settings=EncoderSettings(
            encoder="libsvtav1",
            preset="preset_6",
            crf=20,
            cpu_threads_to_use=8,
        ),
        execution_data=JsonExecutionData(
            ffmpeg_command_used="ffmpeg -i ...",
            source_to_encoded_vmaf_percent=95.8,
            encoding_finished_datetime="2026-09-06T12:00:00+00:00",
            encoding_time_seconds=60.0,
            calculating_vmaf_time_seconds=15.0,
            iteration_time_seconds=75.0,
            vmaf_cpu_threads_used=8,
        ),
        environment=JsonEnvironment(
            script_version="9.6.2",
            ffmpeg_version="6.1",
            compression_engine_version=1,
            cpu_name="Apple M1",
            cpu_threads=8,
        ),
    )

    job_data = JobData(
        schema_version=5,
        source_video=source_video,
        encoding_stage=EncodingStage(
            stage_number_from_1=4,
            stage_name=EncodingStageNamesEnum.CRF_FOUND,
            crf_range_min=18,
            crf_range_max=24,
            last_crf=20.0,
            last_vmaf=95.8,
            job_total_time_seconds=75.0,
        ),
        iterations=[iteration_1],
    )

    job_dto = LegacyJsonJobMapper.to_dto(
        job_data=job_data,
        input_dir=input_dir,
        output_dir=output_dir,
    )

    assert job_dto.is_legacy_import is True
    assert job_dto.stage == JobStage.COMPLETED
    assert job_dto.total_time_seconds == 75.0

    # Source video validation
    assert job_dto.source_video is not None
    assert job_dto.source_video.file.file_name == "movie.mkv"
    assert job_dto.source_video.file.absolute_path == input_dir / "movie.mkv"
    assert job_dto.source_video.file.file_mtime_nanoseconds is None
    assert job_dto.source_video.display.width_px == 1920
    assert job_dto.source_video.display.height_px == 1080
    assert job_dto.source_video.display.display_aspect_ratio == "1920:1080"
    assert job_dto.source_video.playback.avg_frame_rate == "24000/1001"
    assert job_dto.source_video.playback.duration_seconds == 120.5
    assert job_dto.source_video.encoding.average_bitrate_kilobits_per_second == 5500.0
    assert job_dto.source_video.color.hdr_format == HdrFormat.DOLBY_VISION

    # Segment validation
    assert len(job_dto.segments) == 1
    segment = job_dto.segments[0]
    assert segment.from_frame == 0
    assert segment.to_frame == int(120.5 * 23.976)
    assert segment.status == SegmentStatus.COMPLETED

    # Iteration validation
    assert len(segment.iterations) == 1
    iteration_dto = segment.iterations[0]
    assert iteration_dto.stage == IterationStage.COMPLETED
    assert iteration_dto.crf == 20
    assert iteration_dto.execution_data.is_legacy_import is True
    assert iteration_dto.execution_data.encoding_cpu_time_seconds is None
    assert iteration_dto.execution_data.total_cpu_time_seconds is None
    assert iteration_dto.execution_data.encoding_wall_time_seconds == 60.0
    assert iteration_dto.execution_data.encoding_cpu.name == "Apple M1"
    assert iteration_dto.execution_data.encoding_cpu.threads == 8

    # VMAF Evaluation validation
    assert len(iteration_dto.evaluations) == 1
    eval_dto = iteration_dto.evaluations[0]
    assert eval_dto.metric.name == "VMAF"
    assert eval_dto.metric.version == "vmaf_v0.6.1"
    assert eval_dto.score == 95.8

    # Output video validation (completed job points to winning iteration video)
    assert job_dto.output_video is not None
    assert job_dto.output_video.file.file_name == "movie_crf20.mkv"
    assert job_dto.output_video.file.absolute_path == output_dir / "movie_crf20.mkv"
    assert job_dto.output_video.file.file_mtime_nanoseconds is None


def test_maps_in_progress_job_without_output_video(mock_app_config):
    input_dir = mock_app_config.input_dir
    output_dir = mock_app_config.output_dir

    source_video = SourceVideo(
        file_attributes=FileAttributes(file_name="clip.mp4", file_size_bytes=1000),
    )
    job_data = JobData(
        schema_version=5,
        source_video=source_video,
        encoding_stage=EncodingStage(
            stage_number_from_1=3,
            stage_name=EncodingStageNamesEnum.SEARCHING_CRF,
        ),
        iterations=[],
    )

    job_dto = LegacyJsonJobMapper.to_dto(
        job_data=job_data,
        input_dir=input_dir,
        output_dir=output_dir,
    )

    assert job_dto.stage == JobStage.IN_PROGRESS
    assert job_dto.output_video is None
    assert job_dto.source_video.file.file_mtime_nanoseconds is None
    assert len(job_dto.segments) == 1
    assert job_dto.segments[0].status == SegmentStatus.IN_PROGRESS
    assert len(job_dto.segments[0].iterations) == 0
