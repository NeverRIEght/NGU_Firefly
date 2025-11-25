import hashlib
import shutil
import os
from pathlib import Path
from typing import Set


def get_file_hash(file_path: Path, block_size: int = 65536) -> str:
    """
    Calculates the MD5 hash of a file to uniquely identify its content.

    Args:
        file_path: The path to the file.
        block_size: The size of the chunks to read from the file.

    Returns:
        A hexadecimal string representing the MD5 hash of the file.
    """
    md5_hash = hashlib.md5()
    try:
        with open(file_path, "rb") as f:
            for block in iter(lambda: f.read(block_size), b""):
                md5_hash.update(block)
    except OSError as e:
        print(f"Error reading file {file_path}: {e}")
        return ""
    return md5_hash.hexdigest()


def smart_copy_folder(source_folder: Path, destination_folder: Path):
    """
    Copies files from the source folder to the destination folder.
    It compares files by hash if a file with the same name already exists in the destination.
    If the content is different, the new file is renamed using its hash to prevent overwriting.

    Args:
        source_folder: The path to the folder to be copied.
        destination_folder: The path to the destination folder.
    """
    if not source_folder.is_dir():
        print(f"Error: Source folder not found: {source_folder}")
        return

    # Ensure the destination folder exists
    destination_folder.mkdir(parents=True, exist_ok=True)
    print(f"Starting smart copy from '{source_folder}' to '{destination_folder}'...")

    # Get a list of all files to process from the source folder
    source_files_to_process = {f for f in source_folder.rglob('*') if f.is_file()}

    # Store hashes of files in destination for quick lookup
    destination_hashes = {}
    print("Pre-calculating hashes for existing files in destination...")
    existing_destination_files = {f for f in destination_folder.rglob('*') if f.is_file()}
    for file_path in existing_destination_files:
        destination_hashes[file_path.name] = get_file_hash(file_path)

    # Copy files and handle duplicates
    for source_file_path in source_files_to_process:
        # Construct the relative path and new file path
        relative_path = source_file_path.relative_to(source_folder)
        destination_file_path = destination_folder / relative_path

        # Create subdirectories if they don't exist
        destination_file_path.parent.mkdir(parents=True, exist_ok=True)

        # Check if the file already exists in the destination
        if destination_file_path.is_file():
            print(f"Conflict detected for '{destination_file_path.name}'. Comparing hashes...")

            # Calculate hash for the source file
            source_hash = get_file_hash(source_file_path)
            destination_hash = destination_hashes.get(destination_file_path.name)

            if source_hash == destination_hash:
                print(f"  - File '{destination_file_path.name}' is a duplicate. Skipping.")
                continue
            else:
                # Hashes are different, so it's a file with the same name but different content
                # We need to rename the new file to avoid overwriting.
                original_stem = destination_file_path.stem
                original_suffix = destination_file_path.suffix

                # New filename with the hash appended
                new_filename = f"{original_stem}_{source_hash}{original_suffix}"
                new_destination_path = destination_file_path.parent / new_filename

                print(f"  - Content is different. Copying as '{new_filename}'.")
                shutil.copy2(source_file_path, new_destination_path)
        else:
            # File does not exist, simply copy it
            print(f"Copying new file: '{destination_file_path.name}'...")
            shutil.copy2(source_file_path, destination_file_path)

    print("\nSmart copy completed.")


# --- Пример использования ---
if __name__ == "__main__":
    source_path = Path("/Users/michaelkomarov/Documents/encode/mp4s")
    destination_path = Path("/Users/michaelkomarov/Documents/encode/input_mp4s")

    smart_copy_folder(source_path, destination_path)