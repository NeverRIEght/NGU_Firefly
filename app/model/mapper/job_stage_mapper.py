from app.db.cache import JobStagesCache
from app.model.dto import JobStage
from app.model.entity.entities import JobStagesEntity
from app.model.mapper.abstract_mapper import AbstractMapper


class JobStageMapper(AbstractMapper[JobStage, JobStagesEntity]):
    @staticmethod
    def to_entity(dto: JobStage) -> JobStagesEntity:
        stage_id = JobStagesCache.get_instance().get_id(dto)
        return JobStagesEntity(id=stage_id, name=dto.name)

    @staticmethod
    def to_dto(entity: JobStagesEntity) -> JobStage:
        cache: JobStagesCache = JobStagesCache.get_instance()
        stage_entity_id = JobStageMapper._get_required(entity.id, "stage_entity.id")
        return cache.get_stage(stage_entity_id)
