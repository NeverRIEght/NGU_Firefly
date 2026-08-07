from app.model.dto import Cpu
from app.model.entity.entities import CpuEntity
from app.model.mapper.abstract_mapper import AbstractMapper


class CpuMapper(AbstractMapper[Cpu, CpuEntity]):
    @staticmethod
    def to_entity(dto: Cpu) -> CpuEntity:
        return CpuEntity(
            id=dto.id,
            cpu_name=CpuMapper._get_required(dto.name, "cpu_name"),
            cpu_threads=CpuMapper._get_required(dto.threads, "cpu_threads"),
        )

    @staticmethod
    def to_dto(entity: CpuEntity) -> Cpu:
        return Cpu(
            id=entity.id,
            name=entity.cpu_name,
            threads=entity.cpu_threads,
        )
