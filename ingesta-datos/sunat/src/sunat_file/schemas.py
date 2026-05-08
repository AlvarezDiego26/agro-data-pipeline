from pathlib import Path

from pydantic import BaseModel


class SunatFile(BaseModel):
    path: Path
    extension: str
    source_name: str


class SunatRemoteFile(BaseModel):
    file_name: str
    url: str
    extension: str
    source_page_url: str
