import logging
import threading
from typing import List, Optional

from app.model.encoder_job_context import EncoderJob
from app.prioritization.priority_rule import PriorityRule
from app.prioritization.rules.low_bitrate_rule import LowBitrateRule
from app.prioritization.rules.resolution_rule import ResolutionRule

log = logging.getLogger(__name__)


class JobPrioritizer:
    _instance: Optional[JobPrioritizer] = None
    _lock = threading.Lock()

    def __init__(self):
        self.rules: List[PriorityRule] = [
            LowBitrateRule(),
            ResolutionRule(),
        ]

    @classmethod
    def get_instance(cls) -> JobPrioritizer:
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = JobPrioritizer()
        return cls._instance

    def prioritize(self, jobs: List[EncoderJob]) -> None:
        """
        Calculates and sets the priority for each job.
        """
        if not jobs:
            return

        log.info("Calculating priority for %d jobs...", len(jobs))
        
        for job in jobs:
            score = 1.0
            for rule in self.rules:
                multiplier = rule.get_priority_multiplier(job)
                score *= multiplier
            
            job.priority = score
            log.debug(f"Job: {job.source_file_path.name}, Priority: {score:.4f}")
