import logging

from app import json_serializer
from app.filtering.abstract_filtering_rule import AbstractFilteringRule
from app.model.encoder_job_context import EncoderJob
from app.model.json.encoding_stage import EncodingStageNamesEnum

log = logging.getLogger(__name__)


class IsHdrRule(AbstractFilteringRule):
    def apply(self, job: EncoderJob) -> bool:
        """
        [TEMPORARY] Filters out videos with HDR metadata, as they may require special handling and
        are not currently supported by the encoding pipeline.
        """

        source_file_name = job.job_data.source_video.file_attributes.file_name
        hdr_types = job.job_data.source_video.ffmpeg_metadata.hdr_types

        if not hdr_types or len(hdr_types) == 0:
            return False
        elif hdr_types and len(hdr_types) > 0:
            log.info("HDR detected: %s. Skipping, HDR is not supported.", source_file_name)
            job.job_data.encoding_stage.stage_number_from_1 = -4
            job.job_data.encoding_stage.stage_name = EncodingStageNamesEnum.SKIPPED_IS_HDR_VIDEO
            json_serializer.serialize_to_json(job.job_data, job.metadata_json_file_path)
            return True

        return False
