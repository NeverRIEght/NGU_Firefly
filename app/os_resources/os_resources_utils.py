import logging

from app.os_resources.exceptions import LowResourcesException

log = logging.getLogger(__name__)

import psutil

from app.config.app_config import ConfigManager


def offload_if_memory_low(process):
    app_config = ConfigManager.get_config()

    mem = psutil.virtual_memory()
    if mem.percent > app_config.ram_percent_hard_limit or mem.available < (app_config.ram_hard_limit_bytes):
        log.debug("System RAM is low (%.1f%% used). Stopping to prevent swap", mem.percent)
        process.kill()
        raise LowResourcesException(f"Process killed due to low memory: {mem.percent}% used")


def set_process_priority(process, priority_str):
    p_str = priority_str.lower()

    try:
        if hasattr(process, 'pid') and not isinstance(process, psutil.Process):
            target_process = psutil.Process(process.pid)
        else:
            target_process = process

        if psutil.WINDOWS:
            priority_map = {
                "idle": psutil.IDLE_PRIORITY_CLASS,
                "below_normal": psutil.BELOW_NORMAL_PRIORITY_CLASS,
                "normal": psutil.NORMAL_PRIORITY_CLASS,
                "above_normal": psutil.ABOVE_NORMAL_PRIORITY_CLASS,
                "high": psutil.HIGH_PRIORITY_CLASS,
                "real_time": psutil.REALTIME_PRIORITY_CLASS
            }

            if p_str not in priority_map:
                log.warning(f"Unknown priority level: {priority_str}. Falling back to \"normal\"")
                val = psutil.NORMAL_PRIORITY_CLASS
            else:
                val = priority_map[p_str]

            target_process.nice(val)

        else:
            priority_map = {
                "idle": 19,
                "below_normal": 10,
                "normal": 0,
                "above_normal": -5,
                "high": -15,
                "real_time": -20
            }
            target_nice = priority_map.get(p_str)

            if target_nice is None:
                log.warning(f"Unknown priority level: {priority_str}. Falling back to 'normal' (nice 0)")
                target_nice = 0

            try:
                target_process.nice(target_nice)
            except psutil.AccessDenied:
                if target_nice < 0:
                    log.warning(f"Sudo/Root required for '{p_str}' priority. Falling back to 'normal' (nice 0)")
                    target_process.nice(0)
                else:
                    raise

        log.debug(f"Set process PID {process.pid} priority to {p_str}")

    except psutil.NoSuchProcess:
        pid = getattr(process, 'pid', 'unknown')
        log.warning(f"Failed to set priority: Process {pid} already terminated")
    except Exception as e:
        pid = getattr(process, 'pid', 'unknown')
        log.error(f"Failed to set priority for PID {pid}: {e}")
