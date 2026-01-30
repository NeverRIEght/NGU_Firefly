from pydantic import BaseModel

from app.config.app_config import ConfigManager
from app.model.encoder_job_context import EncoderJobContext
from app.model.json.iteration import Iteration


class VideoEmbeddedMetadata(BaseModel):
    source_video_file_name: str
    source_video_sha256_hash: str
    encoding_software: str
    encoding_software_version: str
    ffmpeg_version: str
    encoder: str
    codec: str
    preset: str
    crf: int
    vmaf_from_source: float
    ffmpeg_command_used: str
    encoding_finished_datetime: str

    @classmethod
    def from_job(cls, job:EncoderJobContext, iteration: Iteration):
        app_config = ConfigManager.get_config()
        return cls(
            source_video_file_name=job.encoder_data.source_video.file_attributes.file_name,
            source_video_sha256_hash=job.encoder_data.source_video.sha256_hash,
            encoding_software=app_config.app_name,
            encoding_software_version=app_config.version,
            ffmpeg_version=iteration.environment.ffmpeg_version,
            encoder=iteration.encoder_settings.encoder,
            codec=iteration.video_attributes.codec,
            preset=iteration.encoder_settings.preset,
            crf=iteration.encoder_settings.crf,
            vmaf_from_source=iteration.execution_data.source_to_encoded_vmaf_percent,
            ffmpeg_command_used=iteration.execution_data.ffmpeg_command_used,
            encoding_finished_datetime=iteration.execution_data.encoding_finished_datetime,
        )