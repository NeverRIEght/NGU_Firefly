from pydantic import BaseModel


class FileAttributes(BaseModel):
    file_name: str
    file_size_bytes: int
