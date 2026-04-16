import logging
import threading
from typing import List, Optional

from app.filtering.abstract_filtering_rule import AbstractFilteringRule
from app.filtering.rules.is_hdr_rule import IsHdrRule
from app.model.encoder_job_context import EncoderJob

log = logging.getLogger(__name__)


class JobFilter:
    _instance: Optional[JobFilter] = None
    _lock = threading.Lock()

    def __init__(self):
        self.rules: List[AbstractFilteringRule] = [
            IsHdrRule(),
        ]

    @classmethod
    def get_instance(cls) -> JobFilter:
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = JobFilter()
        return cls._instance

    def filter(self, jobs: List[EncoderJob]) -> List[EncoderJob]:
        """
        Filters jobs based on rules
        :param jobs: list of jobs to filter
        :return: list of jobs, which passed the filter
        """
        if not jobs:
            return []

        log.info("Starting filter for %d jobs...", len(jobs))

        passed_jobs: List[EncoderJob] = []

        for job in jobs:
            is_filtered = False
            for rule in self.rules:
                should_be_filtered = rule.apply(job)
                if should_be_filtered:
                    log.debug(f"Job filtered out: {job.source_file_path.name} by rule: {rule.__class__.__name__}")
                    is_filtered = True
                    break
            if not is_filtered:
                passed_jobs.append(job)

        filtered_jobs_count = len(jobs) - len(passed_jobs)

        log.info("Filtering finished. Filtered out/passed/total jobs: %d/%d/%d",
                 filtered_jobs_count, len(passed_jobs), len(jobs))

        return passed_jobs
