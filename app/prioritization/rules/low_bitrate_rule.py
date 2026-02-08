from app.model.encoder_job_context import EncoderJob
from app.prioritization.priority_rule import PriorityRule


class LowBitrateRule(PriorityRule):
    def get_priority_multiplier(self, job: EncoderJob) -> float:
        """
        Deprioritizes videos with low bitrate (< 1 Mbps), as they are likely
        already compressed or have low quality, making further compression inefficient.
        """
        if not job.job_data.source_video.video_attributes:
            return 1.0

        bitrate_kbps = job.job_data.source_video.video_attributes.average_bitrate_kilobits_per_second

        if bitrate_kbps < 1000:
            return 0.1
            
        return 1.0
