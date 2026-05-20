import logging
from typing import Optional

from app import json_serializer
from app.config.config_manager import ConfigManager
from app.filtering.abstract_filtering_rule import AbstractFilteringRule
from app.model.encoder_job_context import EncoderJob
from app.model.json.encoding_stage import EncodingStageNamesEnum
from app.model.json.video_embedded_metadata import VideoEmbeddedMetadata

log = logging.getLogger(__name__)

class AlreadyEncodedRule(AbstractFilteringRule):
    def apply(self, job: EncoderJob) -> bool:
        """
        Filters out videos that have already been encoded with the current compression
        engine version, codec, etc.
        """
        app_config = ConfigManager.get_config()

        source_file_name = job.job_data.source_video.file_attributes.file_name
        metadata: Optional[VideoEmbeddedMetadata] = job.job_data.source_video.ffmpeg_metadata.video_embedded_metadata

        if not metadata:
            return False

        if metadata.encodes_count >= app_config.max_encodes_limit:
            log.info(f"Video {source_file_name} reached the re-encode limit. Skipping.")
            job.job_data.encoding_stage.stage_number_from_1 = -5
            job.job_data.encoding_stage.stage_name = EncodingStageNamesEnum.ALREADY_ENCODED
            json_serializer.serialize_to_json(job.job_data, job.metadata_json_file_path)
            return True

        needs_re_encode = False

        if metadata.encoding_software != app_config.app_name:
            log.info(f"Video {source_file_name} was encoded by different software: "
                     f"{metadata.encoding_software} != {app_config.app_name}.")
            needs_re_encode = True

        is_same_codec = True
        if metadata.codec != "hevc": # TODO: Make it more flexible to support different codecs in the future
            log.info(f"Video {source_file_name} was encoded with different codec: "
                     f"{metadata.codec} != hevc.")
            is_same_codec = False
            needs_re_encode = True

        if metadata.compression_engine_version < app_config.compression_engine_version:
            log.info(f"Video {source_file_name} was encoded with older engine version: "
                     f"{metadata.compression_engine_version} < {app_config.compression_engine_version}.")
            needs_re_encode = True

        if is_same_codec:
            presets = ["ultrafast", "superfast", "veryfast", "faster", "fast",
                       "medium", "slow", "slower", "veryslow", "placebo"]
            try:
                old_preset_idx = presets.index(metadata.preset.lower())
                new_preset_idx = presets.index(app_config.encoder_preset.lower())
                if old_preset_idx < new_preset_idx:
                    log.info(f"Video {source_file_name} was encoded with faster preset ({metadata.preset}). "
                             f"Current config requires {app_config.encoder_preset}.")
                    needs_re_encode = True
            except ValueError:
                pass

        if needs_re_encode:
            return False

        log.info(f"Video {source_file_name} is already encoded with current standard. Skipping.")
        job.job_data.encoding_stage.stage_number_from_1 = -5
        job.job_data.encoding_stage.stage_name = EncodingStageNamesEnum.ALREADY_ENCODED
        json_serializer.serialize_to_json(job.job_data, job.metadata_json_file_path)
        return True

