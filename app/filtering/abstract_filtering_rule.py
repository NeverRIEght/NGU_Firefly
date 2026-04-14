from abc import ABC, abstractmethod

from app.model.encoder_job_context import EncoderJob


class AbstractFilteringRule(ABC):
    @abstractmethod
    def apply(self, job: EncoderJob) -> bool:
        """
        Returns a result of filtration for a job, according to this rule.
        
        :param job: The job to evaluate.
        :return: True if the job should be filtered out (excluded from processing), False otherwise.
        """
        pass
