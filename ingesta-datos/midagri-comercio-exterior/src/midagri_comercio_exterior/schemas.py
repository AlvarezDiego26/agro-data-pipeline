from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(slots=True)
class MidagriRemoteFile:
    file_name: str
    url: str
    extension: str
    source_page_url: str
    title: str
    publication_year: int | None
    content_length: int | None = None
    last_modified: str | None = None
    remote_signature: str = ""


@dataclass(slots=True)
class MidagriFile:
    path: Path
    extension: str
    source_name: str
