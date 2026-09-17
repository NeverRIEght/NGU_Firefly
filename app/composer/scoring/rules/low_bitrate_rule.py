from app.composer.scoring.abstract_scoring_rule import AbstractScoringRule
from app.composer.scoring.scoring_exception import ScoringException
from app.model.dto import Job


class LowBitrateRule(AbstractScoringRule):
    def get_score_multiplier(self, job: Job) -> float:
        """
        Deprioritizes videos with low bitrate (< 1 Mbps), as they are likely
        already compressed or have low quality, making further compression inefficient.
        """
        try:
            bitrate_kbps = job.source_video.encoding.average_bitrate_kilobits_per_second
            if not bitrate_kbps:
                raise ValueError
        except (AttributeError, ValueError) as e:
            raise ScoringException(
                "Cannot score job: missing source video bitrate information."
            ) from e

        if bitrate_kbps < 1000:
            return 0.1

        return 1.0
