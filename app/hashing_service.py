import hashlib
from pathlib import Path
import logging

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

CHUNK_SIZE = 65536


def calculate_sha256_hash(file_path: Path) -> str:
    if not file_path.is_file():
        log.error(f"Source file not found at {file_path.name}")
        raise FileNotFoundError(f"Source file not found at {file_path.resolve()}")

    log.info(f"Calculating SHA256 for the file: {file_path.name}")

    sha256_hash = hashlib.sha256()

    try:
        with open(file_path, "rb") as f:

            while True:
                chunk = f.read(CHUNK_SIZE)
                if not chunk:
                    break

                sha256_hash.update(chunk)

    except IOError as e:
        log.error(f"Error while reading file: {file_path.name}: {e}")
        raise RuntimeError(f"Could not read file for hashing: {e}")

    final_hash = sha256_hash.hexdigest()
    log.info(f"SHA256 calculated. Hash: {final_hash[:10]}...")

    return final_hash
