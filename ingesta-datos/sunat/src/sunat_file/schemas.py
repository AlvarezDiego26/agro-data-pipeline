from pathlib import Path

from pydantic import BaseModel


class SunatFile(BaseModel):
    path: Path
    extension: str
    source_name: str
