from abc import ABC, abstractmethod

from app.model.encoder_job_context import EncoderJob


class PriorityRule(ABC):
    @abstractmethod
    def get_priority_multiplier(self, job: EncoderJob) -> float:
        """
        Returns a priority multiplier for the given job.
        
        :param job: The job to evaluate.
        :return: A float multiplier. 
                 1.0 means neutral (no change).
                 < 1.0 means lower priority.
                 > 1.0 means higher priority.
        """
        pass
