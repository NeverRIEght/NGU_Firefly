import threading
from typing import Optional

from cpuinfo import get_cpu_info


class CpuInfoProvider:
    _instance: Optional[CpuInfoProvider] = None
    _lock = threading.Lock()

    def __init__(self):
        raw_info = get_cpu_info()
        self._thread_count: int = raw_info.get("count") or -1
        self._cpu_name: str = raw_info.get("brand_raw") or "unknown"

    @classmethod
    def get_instance(cls) -> CpuInfoProvider:
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    def get_cpu_name(self) -> str:
        return self._cpu_name
    
    def get_thread_count(self) -> int:
        return self._thread_count
