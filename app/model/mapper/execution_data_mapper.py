from app.model.dto import ExecutionData
from app.model.entity.entities import ExecutionDataEntity
from app.model.mapper.abstract_mapper import AbstractMapper
from app.model.mapper.cpu_mapper import CpuMapper


class ExecutionDataMapper(AbstractMapper[ExecutionData, ExecutionDataEntity]):
    @staticmethod
    def to_entity(dto: ExecutionData) -> ExecutionDataEntity:
        encoding_cpu = CpuMapper.to_entity(
            ExecutionDataMapper._get_required(dto.encoding_cpu, "encoding_cpu")
        )
        evaluation_cpu = CpuMapper.to_entity(dto.evaluation_cpu) if dto.evaluation_cpu else None

        return ExecutionDataEntity(
            id=dto.id,
            ffmpeg_command_used=ExecutionDataMapper._get_required(
                dto.ffmpeg_command_used,
                "ffmpeg_command_used"
            ),
            finished_datetime_utc=ExecutionDataMapper._get_required(
                dto.finished_datetime_utc,
                "finished_datetime_utc"
            ),
            encoding_wall_time_seconds=ExecutionDataMapper._get_required(
                dto.encoding_wall_time_seconds,
                "encoding_wall_time_seconds"
            ),
            evaluation_wall_time_seconds=dto.evaluation_wall_time_seconds,
            encoding_cpu_time_seconds=ExecutionDataMapper._get_required(
                dto.encoding_cpu_time_seconds,
                "encoding_cpu_time_seconds"
            ),
            evaluation_cpu_time_seconds=dto.evaluation_cpu_time_seconds,
            total_wall_time_seconds=dto.total_wall_time_seconds,
            total_cpu_time_seconds=dto.total_cpu_time_seconds,
            encoding_cpu_threads_used=ExecutionDataMapper._get_required(
                dto.encoding_cpu_threads_used,
                "encoding_cpu_threads_used"
            ),
            evaluation_cpu_threads_used=dto.evaluation_cpu_threads_used,
            encoding_cpu=encoding_cpu,
            evaluation_cpu=evaluation_cpu
        )

    @staticmethod
    def to_dto(entity: ExecutionDataEntity) -> ExecutionData:
        return ExecutionData(
            id=entity.id,
            ffmpeg_command_used=entity.ffmpeg_command_used,
            finished_datetime_utc=entity.finished_datetime_utc,
            encoding_wall_time_seconds=entity.encoding_wall_time_seconds,
            evaluation_wall_time_seconds=entity.evaluation_wall_time_seconds,
            encoding_cpu_time_seconds=entity.encoding_cpu_time_seconds,
            evaluation_cpu_time_seconds=entity.evaluation_cpu_time_seconds,
            total_wall_time_seconds=entity.total_wall_time_seconds,
            total_cpu_time_seconds=entity.total_cpu_time_seconds,
            encoding_cpu_threads_used=entity.encoding_cpu_threads_used,
            evaluation_cpu_threads_used=entity.evaluation_cpu_threads_used,
            encoding_cpu=CpuMapper.to_dto(entity.encoding_cpu) if entity.encoding_cpu else None,
            evaluation_cpu=CpuMapper.to_dto(entity.evaluation_cpu) if entity.evaluation_cpu else None
        )
