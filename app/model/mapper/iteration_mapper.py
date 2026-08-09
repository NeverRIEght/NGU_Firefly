from app.model.dto import Iteration
from app.model.entity.entities import IterationEntity
from app.model.mapper.abstract_mapper import AbstractMapper
from app.model.mapper.environment_mapper import EnvironmentMapper
from app.model.mapper.execution_data_mapper import ExecutionDataMapper


class IterationMapper(AbstractMapper[Iteration, IterationEntity]):
    @staticmethod
    def to_entity(dto: Iteration) -> IterationEntity:
        return IterationEntity(
            id=dto.id,
            video=dto.video,
            environment=dto.environment,
            execution_data=dto.execution_data,
            crf=dto.crf,
        )

    @staticmethod
    def to_dto(entity: IterationEntity) -> Iteration:
        environment = EnvironmentMapper.to_dto(entity.environment) if entity.environment else None
        execution_data = ExecutionDataMapper.to_dto(entity.execution_data) if entity.execution_data else None

        return Iteration(
            id=entity.id,
            stage=None,
            video=None,
            environment=environment,
            execution_data=execution_data,
            crf=entity.crf,
        )
