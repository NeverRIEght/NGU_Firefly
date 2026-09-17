from app.composer.scoring.abstract_scoring_rule import AbstractScoringRule
from app.composer.scoring.scoring_exception import ScoringException
from app.model.dto import Job


class ResolutionRule(AbstractScoringRule):
    def get_score_multiplier(self, job: Job) -> float:
        """
        Prioritizes higher-resolution videos, which demonstrate a higher probability
        of effective compression.
        """
        try:
            width = job.source_video.display.width_px
            height = job.source_video.display.height_px
            if not width or not height:
                raise ValueError
        except (AttributeError, ValueError) as e:
            raise ScoringException(
                "Cannot score job: missing source video display dimensions."
            ) from e

        shorter = min(width, height)

        if shorter >= 2160:  # 4K and above
            return 2.0
        elif shorter >= 1080:  # Full HD
            return 1.5
        elif shorter >= 720:  # HD
            return 1.0
        else:  # SD and lower
            return 0.5
