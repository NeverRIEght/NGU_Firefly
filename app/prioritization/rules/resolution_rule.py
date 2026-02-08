from app.model.encoder_job_context import EncoderJob
from app.prioritization.priority_rule import PriorityRule


class ResolutionRule(PriorityRule):
    def get_priority_multiplier(self, job: EncoderJob) -> float:
        """
        Prioritizes higher-resolution videos, which demonstrate a higher probability
        of effective compression.
        """
        if not job.job_data.source_video.video_attributes:
            return 1.0

        height = job.job_data.source_video.video_attributes.height_px

        if height >= 2160:  # 4K and above
            return 2.0
        elif height >= 1080:  # Full HD
            return 1.5
        elif height >= 720:  # HD
            return 1.0
        else:  # SD and lower
            return 0.5
