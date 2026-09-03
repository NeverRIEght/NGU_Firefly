import threading
from typing import Optional

from sqlalchemy.orm import Session

from app.db.cache.enum_lookup_cache import EnumLookupCache
from app.model.dto import (
    ColorRange,
    ColorStandard,
    HdrFormat,
    IterationStage,
    JobStage,
    SegmentStatus,
)
from app.model.entity.entities import (
    ColorRangesEntity,
    ColorStandardsEntity,
    HdrFormatEntity,
    IterationStagesEntity,
    JobStagesEntity,
    SegmentStatusesEntity,
)


class LookupCacheRegistry:
    _instance: Optional["LookupCacheRegistry"] = None
    _lock = threading.Lock()

    def __init__(self):
        self.color_ranges = EnumLookupCache[ColorRange, ColorRangesEntity](ColorRange, ColorRangesEntity)
        self.color_standards = EnumLookupCache[ColorStandard, ColorStandardsEntity](ColorStandard, ColorStandardsEntity)
        self.hdr_formats = EnumLookupCache[HdrFormat, HdrFormatEntity](HdrFormat, HdrFormatEntity)
        self.iteration_stages = EnumLookupCache[IterationStage, IterationStagesEntity](IterationStage, IterationStagesEntity)
        self.job_stages = EnumLookupCache[JobStage, JobStagesEntity](JobStage, JobStagesEntity)
        self.segment_statuses = EnumLookupCache[SegmentStatus, SegmentStatusesEntity](SegmentStatus, SegmentStatusesEntity)

    @classmethod
    def get_instance(cls) -> "LookupCacheRegistry":
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    @classmethod
    def init_all(cls, session: Session) -> None:
        registry = cls.get_instance()
        registry.color_ranges.init(session)
        registry.color_standards.init(session)
        registry.hdr_formats.init(session)
        registry.iteration_stages.init(session)
        registry.job_stages.init(session)
        registry.segment_statuses.init(session)
