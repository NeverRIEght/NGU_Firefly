from datetime import datetime, timedelta, timezone
from fractions import Fraction
from pathlib import Path
from typing import Optional

from app.model.dto import (
    Color,
    ColorRange,
    ColorStandard,
    Cpu,
    Display,
    EmbeddedMetadata,
    Encoding,
    Environment,
    Evaluation,
    EvaluationMetric,
    ExecutionData,
    File,
    HdrFormat,
    Iteration as IterationDto,
    IterationStage,
    Job,
    JobStage,
    Playback,
    Segment,
    SegmentStatus,
    Video,
)
from app.model.json.encoder_settings import EncoderSettings
from app.model.json.encoding_stage import EncodingStageNamesEnum
from app.model.json.ffmpeg_metadata import FfmpegMetadata, HdrType
from app.model.json.iteration import Iteration as JsonIteration
from app.model.json.job_data import JobData
from app.model.json.source_video import SourceVideo
from app.model.json.video_attributes import VideoAttributes
from app.model.json.video_embedded_metadata import VideoEmbeddedMetadata


class LegacyJsonJobMapper:
    @staticmethod
    def to_dto(
            job_data: JobData,
            input_dir: Path,
            output_dir: Path,
            job_file_path: Optional[Path] = None,
    ) -> Job:
        source_video_dto = LegacyJsonJobMapper._map_source_video(job_data.source_video, input_dir)

        iteration_dtos: list[IterationDto] = []
        for it in job_data.iterations:
            iteration_dtos.append(
                LegacyJsonJobMapper._map_iteration(it, job_data.source_video, output_dir)
            )

        stage_dto = LegacyJsonJobMapper._map_job_stage(job_data.encoding_stage.stage_name)
        created_dt = LegacyJsonJobMapper._calculate_created_datetime(job_data, job_file_path)
        output_video_dto = LegacyJsonJobMapper._resolve_output_video(stage_dto, iteration_dtos, job_data)
        segment_dto = LegacyJsonJobMapper._create_segment(
            job_data.source_video,
            stage_dto,
            iteration_dtos,
            job_data.encoding_stage.job_total_time_seconds,
        )

        return Job(
            id=None,
            source_video=source_video_dto,
            output_video=output_video_dto,
            stage=stage_dto,
            created_datetime_utc=created_dt,
            is_legacy_import=True,
            total_time_seconds=job_data.encoding_stage.job_total_time_seconds,
            segments=[segment_dto],
        )

    @staticmethod
    def _calculate_created_datetime(
            job_data: JobData,
            job_file_path: Optional[Path] = None,
    ) -> datetime:
        earliest_dt: Optional[datetime] = None
        for it in job_data.iterations:
            if it.execution_data and it.execution_data.encoding_finished_datetime:
                finished_dt = LegacyJsonJobMapper._parse_iso_datetime(
                    it.execution_data.encoding_finished_datetime
                )
                if finished_dt:
                    wall_time = (
                            it.execution_data.iteration_time_seconds
                            or it.execution_data.encoding_time_seconds
                            or 0.0
                    )
                    started_dt = finished_dt - timedelta(seconds=wall_time)
                    if earliest_dt is None or started_dt < earliest_dt:
                        earliest_dt = started_dt

        if earliest_dt is not None:
            return earliest_dt

        if job_file_path and job_file_path.is_file():
            return datetime.fromtimestamp(job_file_path.stat().st_mtime, tz=timezone.utc)

        return datetime.now(timezone.utc)

    @staticmethod
    def _create_segment(
            source_video: SourceVideo,
            stage: JobStage,
            iteration_dtos: list[IterationDto],
            total_time_seconds: Optional[float],
    ) -> Segment:
        to_frame = 0
        if source_video.video_attributes:
            duration = source_video.video_attributes.duration_seconds
            fps = source_video.video_attributes.fps
            if duration and fps and duration > 0 and fps > 0:
                to_frame = int(duration * fps)

        status = LegacyJsonJobMapper._map_segment_status(stage)

        return Segment(
            id=None,
            from_frame=0,
            to_frame=to_frame,
            status=status,
            total_time_seconds=total_time_seconds,
            iterations=iteration_dtos,
        )

    @staticmethod
    def _fps_to_avg_frame_rate(fps: float) -> str:
        if fps <= 0:
            return "0/1"

        if abs(fps - (24000 / 1001)) < 0.01 or abs(fps - 23.976) < 0.001:
            return "24000/1001"
        if abs(fps - (30000 / 1001)) < 0.01 or abs(fps - 29.97) < 0.001:
            return "30000/1001"
        if abs(fps - (60000 / 1001)) < 0.01 or abs(fps - 59.94) < 0.001:
            return "60000/1001"

        rounded_int = round(fps)
        if abs(fps - rounded_int) < 0.001:
            return f"{rounded_int}/1"

        frac = Fraction(fps).limit_denominator(1001)
        return f"{frac.numerator}/{frac.denominator}"

    @staticmethod
    def _map_color(ffmpeg_meta: Optional[FfmpegMetadata]) -> Color:
        hdr_types = ffmpeg_meta.hdr_types if ffmpeg_meta and ffmpeg_meta.hdr_types else set()
        hdr_format = LegacyJsonJobMapper._map_hdr_format(hdr_types)

        primaries = (
            LegacyJsonJobMapper._map_color_standard(ffmpeg_meta.color_primaries)
            if ffmpeg_meta
            else None
        )
        trc = (
            LegacyJsonJobMapper._map_color_standard(ffmpeg_meta.color_trc)
            if ffmpeg_meta
            else None
        )
        colorspace = (
            LegacyJsonJobMapper._map_color_standard(ffmpeg_meta.colorspace)
            if ffmpeg_meta
            else None
        )

        return Color(
            id=None,
            hdr_format=hdr_format,
            color_primaries=primaries,
            color_trc=trc,
            colorspace=colorspace,
            color_range=ColorRange.UNKNOWN,
            max_cll=None,
            master_display=None,
            dovi_profile=None,
        )

    @staticmethod
    def _map_color_standard(value: Optional[str]) -> Optional[ColorStandard]:
        if not value:
            return None
        cleaned = value.strip().lower().replace(".", "").replace("_", "-")
        for standard in ColorStandard:
            if standard.value == cleaned:
                return standard
            if standard.value == value.strip().lower():
                return standard
        return ColorStandard.UNKNOWN

    @staticmethod
    def _map_display(
            video_attrs: Optional[VideoAttributes],
            ffmpeg_meta: Optional[FfmpegMetadata],
    ) -> Display:
        width_px = video_attrs.width_px if video_attrs and video_attrs.width_px else 0
        height_px = video_attrs.height_px if video_attrs and video_attrs.height_px else 0

        if width_px > 0 and height_px > 0:
            display_ar = f"{width_px}:{height_px}"
        else:
            display_ar = "16:9"

        pixel_ar = (
            ffmpeg_meta.pixel_aspect_ratio
            if ffmpeg_meta and ffmpeg_meta.pixel_aspect_ratio
            else "1:1"
        )
        pixel_fmt = (
            ffmpeg_meta.pixel_format
            if ffmpeg_meta and ffmpeg_meta.pixel_format
            else "unknown"
        )
        chroma_loc = (
            ffmpeg_meta.chroma_sample_location
            if ffmpeg_meta and ffmpeg_meta.chroma_sample_location
            else "unspecified"
        )

        return Display(
            id=None,
            width_px=width_px,
            height_px=height_px,
            display_aspect_ratio=display_ar,
            pixel_aspect_ratio=pixel_ar,
            pixel_format=pixel_fmt,
            chroma_sample_location=chroma_loc,
        )

    @staticmethod
    def _map_embedded_metadata(
            vem: Optional[VideoEmbeddedMetadata],
    ) -> Optional[EmbeddedMetadata]:
        if not vem:
            return None

        last_encode_dt = LegacyJsonJobMapper._parse_iso_datetime(
            vem.encoding_finished_datetime
        )
        if last_encode_dt is None:
            last_encode_dt = datetime.now(timezone.utc)

        env = Environment(
            id=None,
            firefly_version=vem.encoding_software_version,
            ffmpeg_version=vem.ffmpeg_version,
            compression_engine_version=vem.compression_engine_version,
        )

        return EmbeddedMetadata(
            id=None,
            encodes_count=vem.encodes_count,
            last_encode_datetime=last_encode_dt,
            source_video_sha256_hash=vem.source_video_sha256_hash,
            environment=env,
        )

    @staticmethod
    def _map_hdr_format(hdr_types: set[HdrType]) -> HdrFormat:
        type_values = {t.value if isinstance(t, HdrType) else str(t) for t in hdr_types}
        if HdrType.DOLBY_VISION.value in type_values:
            return HdrFormat.DOLBY_VISION
        if HdrType.HDR10_PLUS.value in type_values:
            return HdrFormat.HDR10_PLUS
        if HdrType.HDR10.value in type_values or HdrType.PQ.value in type_values:
            return HdrFormat.HDR10
        if HdrType.HLG.value in type_values:
            return HdrFormat.HLG
        return HdrFormat.SDR

    @staticmethod
    def _map_iteration(
            iteration: JsonIteration,
            source_video: SourceVideo,
            output_dir: Path,
    ) -> IterationDto:
        video_dto = LegacyJsonJobMapper._map_iteration_video(iteration, source_video, output_dir)

        if iteration.environment:
            env_dto = Environment(
                id=None,
                firefly_version=iteration.environment.script_version,
                ffmpeg_version=iteration.environment.ffmpeg_version,
                compression_engine_version=iteration.environment.compression_engine_version,
            )
            cpu_dto = Cpu(
                id=None,
                name=iteration.environment.cpu_name,
                threads=iteration.environment.cpu_threads,
            )
        else:
            env_dto = Environment(
                id=None,
                firefly_version="unknown",
                ffmpeg_version="unknown",
                compression_engine_version=1,
            )
            cpu_dto = Cpu(
                id=None,
                name="Unknown CPU",
                threads=1,
            )

        if iteration.execution_data:
            finished_dt = LegacyJsonJobMapper._parse_iso_datetime(
                iteration.execution_data.encoding_finished_datetime
            )
            if finished_dt is None:
                finished_dt = datetime.now(timezone.utc)

            cpu_threads_used = (
                iteration.encoder_settings.cpu_threads_to_use
                if iteration.encoder_settings and iteration.encoder_settings.cpu_threads_to_use
                else (cpu_dto.threads or 1)
            )

            exec_data_dto = ExecutionData(
                id=None,
                ffmpeg_command_used=iteration.execution_data.ffmpeg_command_used,
                finished_datetime_utc=finished_dt,
                encoding_wall_time_seconds=iteration.execution_data.encoding_time_seconds,
                evaluation_wall_time_seconds=iteration.execution_data.calculating_vmaf_time_seconds,
                encoding_cpu_time_seconds=None,
                evaluation_cpu_time_seconds=None,
                total_wall_time_seconds=iteration.execution_data.iteration_time_seconds,
                total_cpu_time_seconds=None,
                encoding_cpu_threads_used=cpu_threads_used,
                evaluation_cpu_threads_used=iteration.execution_data.vmaf_cpu_threads_used,
                encoding_cpu=cpu_dto,
                evaluation_cpu=None,
                is_legacy_import=True,
            )

            vmaf_score = iteration.execution_data.source_to_encoded_vmaf_percent
            evaluations = [
                Evaluation(
                    id=None,
                    metric=EvaluationMetric(id=None, name="VMAF", version="vmaf_v0.6.1"),
                    score=vmaf_score,
                )
            ]
        else:
            exec_data_dto = ExecutionData(
                id=None,
                ffmpeg_command_used="unknown",
                finished_datetime_utc=datetime.now(timezone.utc),
                encoding_wall_time_seconds=0.0,
                evaluation_wall_time_seconds=None,
                encoding_cpu_time_seconds=None,
                evaluation_cpu_time_seconds=None,
                total_wall_time_seconds=None,
                total_cpu_time_seconds=None,
                encoding_cpu_threads_used=cpu_dto.threads or 1,
                evaluation_cpu_threads_used=None,
                encoding_cpu=cpu_dto,
                evaluation_cpu=None,
                is_legacy_import=True,
            )
            evaluations = []

        crf = iteration.encoder_settings.crf if iteration.encoder_settings else 0

        return IterationDto(
            id=None,
            stage=IterationStage.COMPLETED,
            video=video_dto,
            environment=env_dto,
            execution_data=exec_data_dto,
            crf=crf,
            evaluations=evaluations,
        )

    @staticmethod
    def _map_iteration_encoding(
        video_attrs: Optional[VideoAttributes],
        encoder_settings: Optional[EncoderSettings],
    ) -> Encoding:
        codec = "unknown"
        if video_attrs and video_attrs.codec:
            codec = video_attrs.codec
        elif encoder_settings and encoder_settings.encoder:
            codec = encoder_settings.encoder
        preset = encoder_settings.preset if encoder_settings else None
        encoder = encoder_settings.encoder if encoder_settings else None
        avg_bitrate = (
            video_attrs.average_bitrate_kilobits_per_second
            if video_attrs
            else None
        )
        return Encoding(
            id=None,
            codec=codec,
            preset=preset,
            encoder=encoder,
            average_bitrate_kilobits_per_second=avg_bitrate,
        )

    @staticmethod
    def _map_iteration_video(
            iteration: JsonIteration,
            source_video: SourceVideo,
            output_dir: Path,
    ) -> Video:
        file_attrs = iteration.file_attributes
        video_attrs = iteration.video_attributes or source_video.video_attributes
        encoder_settings = iteration.encoder_settings
        ffmpeg_meta = iteration.ffmpeg_metadata or source_video.ffmpeg_metadata

        file_dto = File(
            id=None,
            file_name=file_attrs.file_name,
            absolute_path=output_dir / file_attrs.file_name,
            file_size_bytes=file_attrs.file_size_bytes,
            sha256_hash=iteration.sha256_hash,
        )

        display_dto = LegacyJsonJobMapper._map_display(video_attrs, ffmpeg_meta)
        playback_dto = LegacyJsonJobMapper._map_playback(video_attrs)
        encoding_dto = LegacyJsonJobMapper._map_iteration_encoding(video_attrs, encoder_settings)
        color_dto = LegacyJsonJobMapper._map_color(ffmpeg_meta)
        embedded_dto = (
            LegacyJsonJobMapper._map_embedded_metadata(ffmpeg_meta.video_embedded_metadata)
            if ffmpeg_meta and ffmpeg_meta.video_embedded_metadata
            else None
        )

        return Video(
            id=None,
            file=file_dto,
            display=display_dto,
            playback=playback_dto,
            encoding=encoding_dto,
            color=color_dto,
            embedded_metadata=embedded_dto,
        )

    @staticmethod
    def _map_job_stage(stage_name: EncodingStageNamesEnum) -> JobStage:
        if stage_name == EncodingStageNamesEnum.PREPARED:
            return JobStage.CREATED
        elif stage_name == EncodingStageNamesEnum.METADATA_EXTRACTED:
            return JobStage.METADATA_EXTRACTION
        elif stage_name == EncodingStageNamesEnum.SEARCHING_CRF:
            return JobStage.IN_PROGRESS
        elif stage_name in (
                EncodingStageNamesEnum.CRF_FOUND,
                EncodingStageNamesEnum.COMPLETED,
        ):
            return JobStage.COMPLETED
        elif stage_name in (
                EncodingStageNamesEnum.ALREADY_ENCODED,
                EncodingStageNamesEnum.SKIPPED_IS_HDR_VIDEO,
        ):
            return JobStage.FILTERED_OUT
        elif stage_name in (
                EncodingStageNamesEnum.FAILED,
                EncodingStageNamesEnum.STOPPED_VMAF_DELTA,
                EncodingStageNamesEnum.UNREACHABLE_VMAF,
        ):
            return JobStage.FAILED
        return JobStage.IN_PROGRESS

    @staticmethod
    def _map_playback(video_attrs: Optional[VideoAttributes]) -> Playback:
        duration = video_attrs.duration_seconds if video_attrs and video_attrs.duration_seconds else 0.0
        fps = video_attrs.fps if video_attrs and video_attrs.fps else 0.0
        avg_rate = LegacyJsonJobMapper._fps_to_avg_frame_rate(fps)

        return Playback(
            id=None,
            duration_seconds=duration,
            frames_counted=None,
            avg_frame_rate=avg_rate,
            r_frame_rate=None,
        )

    @staticmethod
    def _map_segment_status(stage: JobStage) -> SegmentStatus:
        if stage == JobStage.COMPLETED:
            return SegmentStatus.COMPLETED
        elif stage == JobStage.FAILED:
            return SegmentStatus.FAILED
        elif stage == JobStage.FILTERED_OUT:
            return SegmentStatus.COMPLETED
        return SegmentStatus.IN_PROGRESS

    @staticmethod
    def _map_source_encoding(video_attrs: Optional[VideoAttributes]) -> Encoding:
        codec = video_attrs.codec if video_attrs and video_attrs.codec else "unknown"
        avg_bitrate = (
            video_attrs.average_bitrate_kilobits_per_second
            if video_attrs
            else None
        )
        return Encoding(
            id=None,
            codec=codec,
            preset=None,
            encoder=None,
            average_bitrate_kilobits_per_second=avg_bitrate,
        )

    @staticmethod
    def _map_source_video(source_video: SourceVideo, input_dir: Path) -> Video:
        file_attrs = source_video.file_attributes
        video_attrs = source_video.video_attributes
        ffmpeg_meta = source_video.ffmpeg_metadata

        file_dto = File(
            id=None,
            file_name=file_attrs.file_name,
            absolute_path=input_dir / file_attrs.file_name,
            file_size_bytes=file_attrs.file_size_bytes,
            sha256_hash=source_video.sha256_hash,
        )

        display_dto = LegacyJsonJobMapper._map_display(video_attrs, ffmpeg_meta)
        playback_dto = LegacyJsonJobMapper._map_playback(video_attrs)
        encoding_dto = LegacyJsonJobMapper._map_source_encoding(video_attrs)
        color_dto = LegacyJsonJobMapper._map_color(ffmpeg_meta)
        embedded_dto = (
            LegacyJsonJobMapper._map_embedded_metadata(ffmpeg_meta.video_embedded_metadata)
            if ffmpeg_meta and ffmpeg_meta.video_embedded_metadata
            else None
        )

        return Video(
            id=None,
            file=file_dto,
            display=display_dto,
            playback=playback_dto,
            encoding=encoding_dto,
            color=color_dto,
            embedded_metadata=embedded_dto,
        )

    @staticmethod
    def _parse_iso_datetime(dt_str: Optional[str]) -> Optional[datetime]:
        if not dt_str:
            return None
        try:
            dt = datetime.fromisoformat(dt_str)
            if dt.tzinfo is None:
                return dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except (ValueError, TypeError):
            return datetime.now(timezone.utc)

    @staticmethod
    def _resolve_output_video(
            stage: JobStage,
            iteration_dtos: list[IterationDto],
            job_data: JobData,
    ) -> Optional[Video]:
        if stage != JobStage.COMPLETED or not iteration_dtos:
            return None

        target_crf = job_data.encoding_stage.last_crf
        if target_crf is not None:
            target_crf_int = int(target_crf)
            for it_dto in iteration_dtos:
                if it_dto.crf == target_crf_int and it_dto.video:
                    return it_dto.video

        for it_dto in reversed(iteration_dtos):
            if it_dto.video:
                return it_dto.video

        return None
