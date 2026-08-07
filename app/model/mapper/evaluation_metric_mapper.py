from app.model.dto import EvaluationMetric
from app.model.entity.entities import EvaluationMetricsEntity
from app.model.mapper.abstract_mapper import AbstractMapper


class EvaluationMetricMapper(AbstractMapper[EvaluationMetric, EvaluationMetricsEntity]):
    @staticmethod
    def to_entity(dto: EvaluationMetric) -> EvaluationMetricsEntity:
        return EvaluationMetricsEntity(
            id=dto.id,
            name=EvaluationMetricMapper._get_required(dto.name, "name"),
            version=dto.version
        )

    @staticmethod
    def to_dto(entity: EvaluationMetricsEntity) -> EvaluationMetric:
        return EvaluationMetric(
            id=entity.id,
            name=entity.name,
            version=entity.version
        )
