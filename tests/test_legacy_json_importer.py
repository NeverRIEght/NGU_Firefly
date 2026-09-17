import json
from pathlib import Path

import pytest

from app.db.repository.job_repository import JobRepository
from app.migrations.legacy_json_importer import LegacyJsonImporter
from app.model.dto import JobStage


def _create_sample_legacy_job_json(file_path: Path, source_file_name: str, crf: int = 22, vmaf: float = 96.2) -> None:
    data = {
        "schema_version": 5,
        "source_video": {
            "file_attributes": {
                "file_name": source_file_name,
                "file_size_bytes": 100000000,
            },
            "sha256_hash": "sample_hash_abc",
            "video_attributes": {
                "codec": "h264",
                "width_px": 1920,
                "height_px": 1080,
                "duration_seconds": 60.0,
                "fps": 23.976,
                "average_bitrate_kilobits_per_second": 8000.0,
            },
            "ffmpeg_metadata": {
                "pixel_aspect_ratio": "1:1",
                "pixel_format": "yuv420p",
                "chroma_sample_location": "left",
                "color_primaries": "bt709",
                "color_trc": "bt709",
                "colorspace": "bt709",
                "hdr_types": [],
            },
        },
        "encoding_stage": {
            "stage_number_from_1": 4,
            "stage_name": "perfect_crf_found",
            "crf_range_min": 18,
            "crf_range_max": 28,
            "last_crf": float(crf),
            "last_vmaf": vmaf,
            "job_total_time_seconds": 120.0,
        },
        "iterations": [
            {
                "file_attributes": {
                    "file_name": f"{source_file_name}_crf{crf}.mkv",
                    "file_size_bytes": 45000000,
                },
                "sha256_hash": "iter_hash_xyz",
                "video_attributes": {
                    "codec": "hevc",
                    "width_px": 1920,
                    "height_px": 1080,
                    "duration_seconds": 60.0,
                    "fps": 23.976,
                    "average_bitrate_kilobits_per_second": 3600.0,
                },
                "encoder_settings": {
                    "encoder": "libx265",
                    "preset": "slow",
                    "crf": crf,
                    "cpu_threads_to_use": 8,
                },
                "execution_data": {
                    "ffmpeg_command_used": "ffmpeg -i sample.mkv ...",
                    "source_to_encoded_vmaf_percent": vmaf,
                    "encoding_finished_datetime": "2026-09-06T10:00:00+00:00",
                    "encoding_time_seconds": 90.0,
                    "calculating_vmaf_time_seconds": 30.0,
                    "iteration_time_seconds": 120.0,
                    "vmaf_cpu_threads_used": 8,
                },
                "environment": {
                    "script_version": "9.6.2",
                    "ffmpeg_version": "6.1",
                    "compression_engine_version": 1,
                    "cpu_name": "AMD Ryzen",
                    "cpu_threads": 16,
                },
            }
        ],
    }
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_text(json.dumps(data, indent=2), encoding="utf-8")


def test_import_all_migrates_legacy_json_file_and_cleans_up(init_test_db, mock_app_config):
    jobs_dir = mock_app_config.output_dir / "firefly" / "data" / "jobs"
    json_path = jobs_dir / "video_01.job.json"
    _create_sample_legacy_job_json(json_path, source_file_name="video_01.mkv", crf=22, vmaf=96.5)

    assert json_path.exists()

    importer = LegacyJsonImporter()
    count = importer.import_all()

    assert count == 1
    assert not json_path.exists()

    repo = JobRepository()
    source_path = mock_app_config.input_dir / "video_01.mkv"
    saved_job = repo.get_by_source_path(source_path)

    assert saved_job is not None
    assert saved_job.is_legacy_import is True
    assert saved_job.stage == JobStage.COMPLETED
    assert saved_job.priority == 1.0
    assert saved_job.source_video is not None
    assert saved_job.source_video.file.file_name == "video_01.mkv"
    assert saved_job.source_video.file.file_mtime_nanoseconds is None
    assert saved_job.output_video is not None
    assert saved_job.output_video.file.file_name == "video_01.mkv_crf22.mkv"
    assert saved_job.output_video.file.file_mtime_nanoseconds is None

    assert len(saved_job.segments) == 1
    segment = saved_job.segments[0]
    assert len(segment.iterations) == 1
    iteration = segment.iterations[0]
    assert iteration.crf == 22
    assert iteration.execution_data.is_legacy_import is True
    assert iteration.execution_data.encoding_cpu_time_seconds is None
    assert len(iteration.evaluations) == 1
    assert iteration.evaluations[0].metric.name == "VMAF"
    assert iteration.evaluations[0].score == 96.5


def test_import_all_idempotent_when_job_already_in_database(init_test_db, mock_app_config):
    jobs_dir = mock_app_config.output_dir / "firefly" / "data" / "jobs"
    json_path = jobs_dir / "video_02.job.json"
    _create_sample_legacy_job_json(json_path, source_file_name="video_02.mkv", crf=24, vmaf=96.1)

    importer = LegacyJsonImporter()
    first_count = importer.import_all()
    assert first_count == 1
    assert not json_path.exists()

    # Re-create file to simulate re-running with existing DB record
    _create_sample_legacy_job_json(json_path, source_file_name="video_02.mkv", crf=24, vmaf=96.1)
    assert json_path.exists()

    second_count = importer.import_all()
    assert second_count == 0
    assert not json_path.exists()


def test_zero_data_loss_preserves_corrupted_json(init_test_db, mock_app_config):
    jobs_dir = mock_app_config.output_dir / "firefly" / "data" / "jobs"
    bad_json_path = jobs_dir / "corrupted.job.json"
    bad_json_path.parent.mkdir(parents=True, exist_ok=True)
    bad_json_path.write_text("{invalid json structure: !!!", encoding="utf-8")

    assert bad_json_path.exists()

    importer = LegacyJsonImporter()
    count = importer.import_all()

    assert count == 0
    assert bad_json_path.exists()
