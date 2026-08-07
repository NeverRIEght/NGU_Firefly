from app.model.dto import Evaluation
from app.model.entity.entities import EvaluationEntity
from app.model.mapper.abstract_mapper import AbstractMapper
from app.model.mapper.evaluation_metric_mapper import EvaluationMetricMapper


class EvaluationMapper(AbstractMapper[Evaluation, EvaluationEntity]):
    @staticmethod
    def to_entity(dto: Evaluation) -> EvaluationEntity:
        metric = EvaluationMetricMapper.to_entity(dto.metric) if dto.metric else None

        return EvaluationEntity(
            id=dto.id,
            metric=EvaluationMapper._get_required(metric, "metric"),
            score=EvaluationMapper._get_required(dto.score, "score"),
        )

    @staticmethod
    def to_dto(entity: EvaluationEntity) -> Evaluation:
        return Evaluation(
            id=entity.id,
            metric=EvaluationMetricMapper.to_dto(entity.metric) if entity.metric else None,
            score=entity.score,
        )
