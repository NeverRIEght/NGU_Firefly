import platform
import subprocess

def get_environment_info():
    return {
        "os": platform.system(),
        "os_release": platform.release(),
        "cpu_name": platform.processor(), # Может вернуть 'Intel64 Family...' на Win
        "python_version": platform.python_version()
    }

def get_ffmpeg_version():
    try:
        # Быстро вытаскиваем первую строку из ffmpeg -version
        res = subprocess.check_output(['ffmpeg', '-version'], stderr=subprocess.STDOUT)
        return res.decode().split('\n')[0].split('version ')[1].split(' ')[0]
    except:
        return "unknown"