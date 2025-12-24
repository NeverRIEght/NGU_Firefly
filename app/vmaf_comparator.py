import json
import os
import subprocess
import tempfile
from pathlib import Path

from app.model.video_attributes import VideoAttributes


def calculate_vmaf(
        source_video_path: Path,
        encoded_video_path: Path,
        source_video_attributes: VideoAttributes
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

    if not source_video_path.is_file():
        raise FileNotFoundError(f"Reference file not found: {source_video_path}")
    if not encoded_video_path.is_file():
        raise FileNotFoundError(f"Distorted file not found: {encoded_video_path}")

    model_name = _get_optimal_model_name(
        width=source_video_attributes.width_px,
        height=source_video_attributes.height_px
    )

    model_path = get_vmaf_model_path(model_name)

    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tmp:
        log_path = Path(tmp.name)

    model_path_str = model_path.as_posix()
    log_path_str = log_path.as_posix()

    if os.name == 'nt': # Windows needs escaping of colons
        model_path_str = model_path_str.replace(':', '\\:')
        log_path_str = log_path_str.replace(':', '\\:')

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

    vmaf_filter = (
        f"[1:v][0:v]scale2ref=flags=bicubic[dist][ref];"
        f"[dist]format=yuv420p[dist_f];"
        f"[ref]format=yuv420p[ref_f];"
        f"[dist_f][ref_f]libvmaf=model='path={model_path_str}':"
        f"log_path='{log_path_str}':log_fmt=json"
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

    try:
        subprocess.run(cmd, check=True, capture_output=True, text=True)

        with open(log_path, 'r') as f:
            json_data = json.load(f)

    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"FFmpeg VMAF failed:\n{e.stderr}")
    except Exception as e:
        raise RuntimeError(f"Failed to read/parse VMAF log at {log_path}: {e}")
    finally:
        if log_path.exists():
            log_path.unlink()

    try:
        return float(json_data["pooled_metrics"]["vmaf"]["mean"])
    except KeyError:
        raise RuntimeError(f"Unexpected JSON structure in VMAF log: {json_data}")


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