import json
import logging
import os
import subprocess
import time
from pathlib import Path

from app import file_utils
from app.config.app_config import ConfigManager
from app.locking import LockManager, LockMode
from app.model.json.video_attributes import VideoAttributes
from app.os_resources import os_resources_utils
from app.os_resources.exceptions import LowResourcesException
from app.os_resources.os_resources_utils import offload_if_memory_low

log = logging.getLogger(__name__)


def calculate_vmaf(
        source_video_path: Path,
        encoded_video_path: Path,
        source_video_attributes: VideoAttributes,
        cpu_threads_count: int
) -> float:
    """
    Compares two video files using VMAF.

    Assumptions & guarantees:
    - No reliance on container color metadata
    - Explicit colorspace normalization
    - Frame-accurate comparison
    - No intermediate files created

    Requirements:
    - ffmpeg built with libvmaf
    """

    with LockManager.acquire_file_operation_lock(source_video_path, LockMode.EXCLUSIVE):
        with LockManager.acquire_file_operation_lock(encoded_video_path, LockMode.EXCLUSIVE):
            if not source_video_path.is_file():
                raise FileNotFoundError(f"Reference file not found: {source_video_path}")
            if not encoded_video_path.is_file():
                raise FileNotFoundError(f"Distorted file not found: {encoded_video_path}")

            app_config = ConfigManager.get_config()

            model_name = _get_optimal_model_name(
                    width=source_video_attributes.width_px,
                    height=source_video_attributes.height_px
            )

            model_path = get_vmaf_model_path(model_name)

            log_filename = f"vmaf_log_{int(time.time())}.json"
            with LockManager.acquire_file_operation_lock(Path(log_filename), LockMode.EXCLUSIVE):
                old_cwd = os.getcwd()
                os.chdir(model_path.parent)

                # We explicitly normalize EVERYTHING to:
                # - yuv420p
                # - bt709
                # - progressive
                # - same resolution & fps (taken from reference)
                #
                # This avoids:
                # - colorspace mismatches
                # - container metadata lies
                # - VMAF undefined behavior

                log.info("Using %d threads for VMAF calculation.", cpu_threads_count)

                try:
                    model_param = model_path.name
                    log_param = log_filename

                    vmaf_filter = (
                        f"[1:v][0:v]scale2ref=flags=bicubic[dist][ref];"
                        f"[dist]format=yuv420p[dist_f];"
                        f"[ref]format=yuv420p[ref_f];"
                        f"[dist_f][ref_f]libvmaf=model='path={model_param}:n_threads={cpu_threads_count}':"
                        f"log_path='{log_param}':log_fmt=json"
                    )

                    cmd = [
                        "ffmpeg",
                        "-hide_banner",
                        "-loglevel", "error",

                        "-i", str(source_video_path),
                        "-i", str(encoded_video_path),

                        "-lavfi", vmaf_filter,
                        "-f", "null",
                        "-"
                    ]

                    log.debug(f"Running VMAF (CWD: {os.getcwd()}): {' '.join(cmd)}")
                    process = subprocess.Popen(
                            cmd,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            text=True,
                            bufsize=1
                    )

                    if not app_config.disable_resources_monitoring:
                        os_resources_utils.set_process_priority(process, app_config.vmaf_process_priority)

                    while process.poll() is None:
                        if not app_config.disable_resources_monitoring:
                            offload_if_memory_low(process)
                        time.sleep(app_config.ram_monitoring_interval_seconds)

                    if process.returncode != 0:
                        _, stderr = process.communicate()
                        raise RuntimeError(f"VMAF FFmpeg failed: {stderr}")

                    with open(log_param, 'r') as f:
                        json_data = json.load(f)
                except LowResourcesException:
                    if process:
                        process.kill()
                    raise
                finally:
                    if process and process.poll() is None:
                        process.kill()
                    file_utils.delete_file_with_lock(Path(log_filename))

                os.chdir(old_cwd)

                return float(json_data["pooled_metrics"]["vmaf"]["mean"])


def _get_optimal_model_name(width: int, height: int) -> str:
    """
    Selects the strict (NEG) VMAF model based on source resolution.
    """
    # We use height 1080 as the threshold.
    # Even for vertical video (like your 576x1024),
    # the standard model is more appropriate.
    if width > 1920 or height > 1080:
        return "vmaf_4k_v0.6.1neg.json"
    return "vmaf_v0.6.1neg.json"


def get_vmaf_model_path(model_filename: str) -> Path:
    app_directory = Path(__file__).parent.resolve()

    model_path = app_directory.parent / "vmaf_models" / model_filename

    if not model_path.exists():
        raise FileNotFoundError(f"VMAF model not found at: {model_path}")

    return model_path
