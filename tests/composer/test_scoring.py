import pytest

from app.composer.scoring import (
    JobScorer,
    ScoringException,
)
from app.composer.scoring.rules import (
    LowBitrateRule,
    ResolutionRule,
)
from app.model.dto import Display, Encoding, File, Job, Video


class TestScoring:
    def _create_sample_job(
        self,
        width: int = 1920,
        height: int = 1080,
        bitrate_kbps: float = 5000.0,
        file_name: str = "sample.mp4",
    ) -> Job:
        return Job(
            source_video=Video(
                display=Display(width_px=width, height_px=height),
                encoding=Encoding(average_bitrate_kilobits_per_second=bitrate_kbps),
                file=File(
                    file_name=file_name,
                    absolute_path=f"/path/{file_name}",
                    file_size_bytes=1000,
                ),
            )
        )

    def test_resolution_rule_multipliers(self) -> None:
        rule = ResolutionRule()

        # 4K (shorter dimension >= 2160)
        job_4k = self._create_sample_job(width=3840, height=2160)
        assert rule.get_score_multiplier(job_4k) == 2.0

        # Full HD (shorter dimension >= 1080)
        job_1080p = self._create_sample_job(width=1920, height=1080)
        assert rule.get_score_multiplier(job_1080p) == 1.5

        # Vertical Full HD (width=1080, height=1920 -> shorter is 1080)
        job_vertical = self._create_sample_job(width=1080, height=1920)
        assert rule.get_score_multiplier(job_vertical) == 1.5

        # HD (shorter dimension >= 720)
        job_720p = self._create_sample_job(width=1280, height=720)
        assert rule.get_score_multiplier(job_720p) == 1.0

        # SD (shorter dimension < 720)
        job_sd = self._create_sample_job(width=640, height=480)
        assert rule.get_score_multiplier(job_sd) == 0.5

    def test_resolution_rule_missing_display_raises(self) -> None:
        rule = ResolutionRule()

        job_missing_display = Job(source_video=Video(display=None))
        with pytest.raises(ScoringException):
            rule.get_score_multiplier(job_missing_display)

        job_missing_dimensions = Job(source_video=Video(display=Display(width_px=None, height_px=None)))
        with pytest.raises(ScoringException):
            rule.get_score_multiplier(job_missing_dimensions)

    def test_low_bitrate_rule_multipliers(self) -> None:
        rule = LowBitrateRule()

        # High bitrate (>= 1000 kbps)
        job_high_bitrate = self._create_sample_job(bitrate_kbps=5000.0)
        assert rule.get_score_multiplier(job_high_bitrate) == 1.0

        # Low bitrate (< 1000 kbps)
        job_low_bitrate = self._create_sample_job(bitrate_kbps=800.0)
        assert rule.get_score_multiplier(job_low_bitrate) == 0.1

    def test_low_bitrate_rule_missing_encoding_raises(self) -> None:
        rule = LowBitrateRule()

        job_missing_encoding = Job(source_video=Video(encoding=None))
        with pytest.raises(ScoringException):
            rule.get_score_multiplier(job_missing_encoding)

        job_missing_bitrate = Job(source_video=Video(encoding=Encoding(average_bitrate_kilobits_per_second=None)))
        with pytest.raises(ScoringException):
            rule.get_score_multiplier(job_missing_bitrate)

    def test_job_scorer_combines_rules(self) -> None:
        scorer = JobScorer.get_instance()

        # 1080p (1.5) + High bitrate (1.0) = 1.5
        job_normal = self._create_sample_job(width=1920, height=1080, bitrate_kbps=5000.0, file_name="normal.mp4")

        # 4K (2.0) + Low bitrate (0.1) = 0.2
        job_4k_low = self._create_sample_job(width=3840, height=2160, bitrate_kbps=500.0, file_name="4k_low.mp4")

        jobs = [job_normal, job_4k_low]
        scorer.score(jobs)

        assert pytest.approx(job_normal.priority) == 1.5
        assert pytest.approx(job_4k_low.priority) == 0.2

    def test_job_scorer_singleton(self) -> None:
        instance1 = JobScorer.get_instance()
        instance2 = JobScorer.get_instance()
        assert instance1 is instance2

    def test_job_scorer_empty_list(self) -> None:
        scorer = JobScorer.get_instance()
        # Should execute safely without error
        scorer.score([])
