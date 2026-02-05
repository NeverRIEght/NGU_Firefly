from app import job_composer


def test_creates_directories_for_jobs(mock_app_config):
    jobs_dir = mock_app_config.output_dir / "firefly" / "data" / "jobs"
    assert not jobs_dir.exists()

    job_composer.compose_jobs()

    assert jobs_dir.exists()
    assert jobs_dir.is_dir()

def test_load_existing_jobs_deletes_invalid_json(mock_app_config):
    jobs_dir = mock_app_config.output_dir / "firefly" / "data" / "jobs"
    jobs_dir.mkdir(parents=True, exist_ok=True)

    bad_file = jobs_dir / f"Invalid_file_{job_composer.JOB_FILE_SUFFIX}"
    bad_file.write_text("Not json structure")

    jobs = job_composer._load_existing_jobs(jobs_dir)

    assert len(jobs) == 0
    assert not bad_file.exists()
