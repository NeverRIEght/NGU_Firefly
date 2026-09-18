import threading
from typing import Optional

from sqlalchemy.orm import Session

from app.db.cache.enum_lookup_cache import EnumLookupCache
from app.model.dto import (
    ColorPrimaries,
    ColorRange,
    ColorSpace,
    ColorTransfer,
    HdrFormat,
    IterationStage,
    JobStage,
    SegmentStatus,
)
from app.model.entity.entities import (
    ColorPrimariesEntity,
    ColorRangesEntity,
    ColorSpaceEntity,
    ColorTransferEntity,
    HdrFormatEntity,
    IterationStagesEntity,
    JobStagesEntity,
    SegmentStatusesEntity,
)


class LookupCacheRegistry:
    _instance: Optional["LookupCacheRegistry"] = None
    _lock = threading.Lock()

    def __init__(self):
        self.color_primaries = EnumLookupCache[ColorPrimaries, ColorPrimariesEntity](
            ColorPrimaries, ColorPrimariesEntity
        )
        self.color_ranges = EnumLookupCache[ColorRange, ColorRangesEntity](
            ColorRange, ColorRangesEntity
        )
        self.color_spaces = EnumLookupCache[ColorSpace, ColorSpaceEntity](
            ColorSpace, ColorSpaceEntity
        )
        self.color_transfers = EnumLookupCache[ColorTransfer, ColorTransferEntity](
            ColorTransfer, ColorTransferEntity
        )
        self.hdr_formats = EnumLookupCache[HdrFormat, HdrFormatEntity](
            HdrFormat, HdrFormatEntity
        )
        self.iteration_stages = EnumLookupCache[IterationStage, IterationStagesEntity](
            IterationStage, IterationStagesEntity
        )
        self.job_stages = EnumLookupCache[JobStage, JobStagesEntity](
            JobStage, JobStagesEntity
        )
        self.segment_statuses = EnumLookupCache[SegmentStatus, SegmentStatusesEntity](
            SegmentStatus, SegmentStatusesEntity
        )

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
        registry.color_primaries.init(session)
        registry.color_ranges.init(session)
        registry.color_spaces.init(session)
        registry.color_transfers.init(session)
        registry.hdr_formats.init(session)
        registry.iteration_stages.init(session)
        registry.job_stages.init(session)
        registry.segment_statuses.init(session)
