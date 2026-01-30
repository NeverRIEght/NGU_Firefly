import logging

from model.json.encoding_stage import EncodingStageNamesEnum

log = logging.getLogger(__name__)

from pathlib import Path

from app.model.encoder_job_context import EncoderJobContext
from app.config.app_config import ConfigManager


def validate(job: EncoderJobContext) -> bool:
    app_config = ConfigManager.get_config()

    source_file_path = job.source_file_path
    metadata_file_path = job.metadata_json_file_path
    stage = job.encoder_data.encoding_stage

    if not source_file_path.exists():
        job.log_error(f"Source file does not exist: {source_file_path}")
        return False

    if not metadata_file_path.exists():
        job.log_error(f"Metadata file does not exist: {metadata_file_path}")
        return False

    if (stage.stage_name == EncodingStageNamesEnum.PREPARED
            or stage.stage_name == EncodingStageNamesEnum.METADATA_EXTRACTED
            or stage.stage_name == EncodingStageNamesEnum.SEARCHING_CRF):
        return True

    is_safe_error = stage.stage_name in {EncodingStageNamesEnum.STOPPED_VMAF_DELTA,
                                         EncodingStageNamesEnum.UNREACHABLE_VMAF}
    if is_safe_error:
        return True

    best_iteration = None
    for iteration in job.encoder_data.iterations:
        if (stage.crf_range_min == stage.crf_range_max
                and iteration.execution_data.source_to_encoded_vmaf_percent == stage.last_vmaf):
            best_iteration = iteration

    if best_iteration is None:
        log.error("No best iteration found for job: %s", job.metadata_json_file_path)
        return False

    best_file_path = Path(app_config.output_dir) / best_iteration.file_attributes.file_name

    if not best_file_path.exists():
        log.error(f"Best encoded file does not exist: {best_file_path}")
        return False

    return True
