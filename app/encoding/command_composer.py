import logging
import threading
from pathlib import Path
from typing import Any, Optional

from app.config.config_manager import ConfigManager
from app.model.encoder_job_context import EncoderJob

log = logging.getLogger(__name__)


class CommandComposer:
    _instance: Optional[CommandComposer] = None
    _lock = threading.Lock()

    @classmethod
    def get_instance(cls) -> CommandComposer:
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = CommandComposer()
        return cls._instance

    def compose(self,
                job_context: EncoderJob,
                crf: int,
                threads_count: int,
                output_file_path: Path) -> list[str]:
        app_config = ConfigManager.get_config()

        source_video = job_context.job_data.source_video
        source_metadata = source_video.ffmpeg_metadata

        color_arguments = self._validate_color_arguments(source_metadata)

        x265_params = [
            f'crf={crf}',
            f'pools={threads_count}',
            'ssim-rd=1',  # better results for VMAF evaluation
            'aq-mode=3',  # better compression for complex scenes
        ]

        command = [
            'ffmpeg',
            '-i', str(job_context.source_file_path),

            '-c:v', 'libx265',
            '-x265-params', ':'.join(x265_params),
            '-preset', app_config.encoder_preset,

            '-fps_mode', 'passthrough',

            *color_arguments,

            '-tag:v', 'hvc1',

            '-c:a', 'copy',
            '-map', '0:v:0',
            '-map', '0:a?',
            '-map_metadata', '0',
            '-map_chapters', '0',
            '-movflags', '+faststart',

            str(output_file_path),

            '-progress', 'pipe:2',
            '-loglevel', 'info',
            '-hide_banner'
        ]

        return command

    def _validate_color_arguments(self, source_metadata: Any) -> list[str]:
        if not source_metadata:
            return []

        color_arguments = []

        if (source_metadata.color_primaries is not None
                and source_metadata.color_trc is not None
                and source_metadata.colorspace is not None):
            color_arguments += [
                '-color_primaries', source_metadata.color_primaries,
                '-color_trc', source_metadata.color_trc,
                '-colorspace', source_metadata.colorspace,
            ]
        else:
            log.warning("Source video is missing color metadata, encoding without explicit color settings.")

        return color_arguments