from abc import ABC, abstractmethod

from app.model.dto import Job


class AbstractScoringRule(ABC):
    @abstractmethod
    def get_score_multiplier(self, job: Job) -> float:
        """
        Returns a score multiplier for the given job.

        :param job: The job to evaluate.
        :return: A float multiplier.
                 1.0 means neutral (no change).
                 < 1.0 means lower priority.
                 > 1.0 means higher priority.
        """
        pass
