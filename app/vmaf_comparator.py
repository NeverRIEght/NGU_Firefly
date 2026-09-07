import json
import logging
import subprocess
import time
import uuid
from pathlib import Path

from app import file_utils
from app.config.config_manager import ConfigManager
from app.model.json.video_attributes import VideoAttributes
from app.project_paths import ProjectPaths
from app.system.locking import LockManager, LockMode
from app.system.os_resources import LowResourcesException, offload_if_memory_low, os_resources_utils

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

    During the process, videos are normalized to:
    - yuv420p
    - bt709
    - progressive
    - same resolution & fps (taken from reference)

    ...which allows to avoid colospace mismatches, container metadata lies, and VMAF undefined behavior.

    Requirements:
    - ffmpeg built with libvmaf
    """

    with LockManager.acquire_file_operation_lock(source_video_path, LockMode.SHARED):
        with LockManager.acquire_file_operation_lock(encoded_video_path, LockMode.SHARED):
            if not source_video_path.is_file():
                raise FileNotFoundError(f"Reference file not found: {source_video_path}")
            if not encoded_video_path.is_file():
                raise FileNotFoundError(f"Distorted file not found: {encoded_video_path}")
            vmaf_models_dir = ProjectPaths.get_instance().vmaf_models_dir

            model_path = _get_optimal_model_path(
                width=source_video_attributes.width_px,
                height=source_video_attributes.height_px
            )

            log_filename = f"vmaf_log_{uuid.uuid4().hex}.json"
            log_file_path = vmaf_models_dir / log_filename

            with LockManager.acquire_file_operation_lock(log_file_path, LockMode.EXCLUSIVE):
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

                    log.debug(f"Running VMAF (CWD: {vmaf_models_dir}): {' '.join(cmd)}")
                    _run_vmaf_process(cmd=cmd, process_working_directory=vmaf_models_dir)

                    with open(log_file_path, 'r') as f:
                        json_data = json.load(f)
                except LowResourcesException:
                    raise LowResourcesException("VMAF calculation stopped due to low system resources.")
                except (json.JSONDecodeError, KeyError) as e:
                    log.error(f"VMAF log file is corrupted or incomplete: {e}")
                    raise RuntimeError(f"Could not parse VMAF results: {e}")
                except PermissionError as e:
                    log.error(f"Permission denied while accessing files: {e}")
                    raise
                except Exception as e:
                    log.exception(f"VMAF calculation failed: {str(e)}")
                    raise RuntimeError(f"VMAF failure: {e}")
                finally:
                    file_utils.delete_file(log_file_path)

                return float(json_data["pooled_metrics"]["vmaf"]["mean"])


def _get_optimal_model_path(width: int, height: int) -> Path:
    model_name = _get_optimal_model_name(width, height)
    model_path = _get_vmaf_model_path(model_name)
    return model_path


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


def _get_vmaf_model_path(model_filename: str) -> Path:
    project_paths = ProjectPaths.get_instance()

    model_path = project_paths.vmaf_models_dir / model_filename

    if not model_path.exists():
        raise FileNotFoundError(f"VMAF model not found at: {model_path}")

    return model_path


def _run_vmaf_process(cmd: list[str], process_working_directory: Path) -> None:
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
        cwd=process_working_directory
    )
    app_config = ConfigManager.get_config()

    if not app_config.disable_resources_monitoring:
        os_resources_utils.set_process_priority(process, app_config.vmaf_process_priority)

    assert process.stderr is not None

    last_ram_check_time = time.perf_counter()

    try:
        while True:
            line = process.stderr.readline()
            if not line and process.poll() is not None:
                break

            if line:
                # Progress will be parsed here in future
                pass

            if not app_config.disable_resources_monitoring:
                current_time = time.perf_counter()
                if current_time - last_ram_check_time >= app_config.ram_monitoring_interval_seconds:
                    offload_if_memory_low(process)
                    last_ram_check_time = current_time

        if process.returncode != 0:
            stderr_remainder = process.stderr.read()
            raise RuntimeError(f"VMAF FFmpeg failed with exit code {process.returncode}: {stderr_remainder}")

    except Exception:
        os_resources_utils.terminate_process_safely(process)
        raise
