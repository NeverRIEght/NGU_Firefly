import threading
from typing import Optional

from app.model.dto import JobStage


class JobStagesCache:
    _instance: Optional[JobStagesCache] = None
    _lock = threading.Lock()

    _name_to_id: dict[JobStage, int] = {}
    _id_to_name: dict[int, JobStage] = {}

    @classmethod
    def get_instance(cls) -> JobStagesCache:
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    @classmethod
    def init(cls):
        # request data from table
        # if table is empty, populate it with values from the in-code enum
        # if not - just use values from table
        # use the values to populate _name_to_id and _id_to_name
        pass

    @classmethod
    def get_id(cls, stage: JobStage) -> int:
        return cls._name_to_id[stage]

    @classmethod
    def get_stage(cls, stage_id: int) -> JobStage:
        return cls._id_to_name[stage_id]