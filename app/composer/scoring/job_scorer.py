import logging
import threading
from typing import List, Optional

from app.composer.scoring.abstract_scoring_rule import AbstractScoringRule
from app.composer.scoring.rules.low_bitrate_rule import LowBitrateRule
from app.composer.scoring.rules.resolution_rule import ResolutionRule
from app.composer.scoring.scoring_exception import ScoringException
from app.model.dto import Job

log = logging.getLogger(__name__)


class JobScorer:
    _instance: Optional["JobScorer"] = None
    _lock = threading.Lock()

    def __init__(self):
        self.rules: List[AbstractScoringRule] = [
            LowBitrateRule(),
            ResolutionRule(),
        ]

    @classmethod
    def get_instance(cls) -> "JobScorer":
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    def score(self, jobs: List[Job]) -> None:
        """
        Calculates and sets the priority score for each job.
        """
        if not jobs:
            return

        log.info("Calculating priority score for %d jobs...", len(jobs))

        for job in jobs:
            calculated_score = 1.0
            for rule in self.rules:
                multiplier = rule.get_score_multiplier(job)
                calculated_score *= multiplier
            try:
                file_name = job.source_video.file.file_name
                if not file_name:
                    raise ValueError
            except (AttributeError, ValueError) as e:
                raise ScoringException(
                    "Cannot score job: missing source video file name."
                ) from e

            job.priority = calculated_score
            log.debug(f"Job: {file_name}, Priority Score: {calculated_score:.4f}")
