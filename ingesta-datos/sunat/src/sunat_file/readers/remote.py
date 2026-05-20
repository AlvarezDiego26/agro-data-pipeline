from __future__ import annotations

from html import unescape
import re
import time
from pathlib import Path
from urllib.parse import urljoin
from urllib.request import Request, urlopen

from sunat_file.config import get_settings
from sunat_file.schemas import SunatRemoteFile

REMOTE_FILE_PATTERN = re.compile(
    r'(?P<name>x\d{8}\.zip)',
    re.IGNORECASE,
)
HREF_PATTERN = re.compile(
    r'href\s*=\s*["\'](?P<href>[^"\']+)["\']',
    re.IGNORECASE,
)
USER_AGENT = 'Mozilla/5.0 (compatible; AgroProyectoSUNAT/1.0)'


def _http_get_text(url: str) -> str:
    settings = get_settings()
    last_error: Exception | None = None
    for attempt in range(1, settings.sunat_download_retry_intentos + 1):
        try:
            request = Request(url, headers={'User-Agent': USER_AGENT})
            with urlopen(request, timeout=settings.sunat_download_timeout_seconds) as response:
                encoding = response.headers.get_content_charset() or 'latin-1'
                return response.read().decode(encoding, errors='replace')
        except Exception as exc:  # pragma: no cover - red/remote dependent
            last_error = exc
            if attempt >= settings.sunat_download_retry_intentos:
                break
            time.sleep(settings.sunat_download_retry_espera_segundos)
    assert last_error is not None
    raise last_error


def _http_download_file(url: str, destination: Path) -> Path:
    settings = get_settings()
    destination.parent.mkdir(parents=True, exist_ok=True)
    temp_path = destination.with_suffix(destination.suffix + '.part')
    last_error: Exception | None = None

    for attempt in range(1, settings.sunat_download_retry_intentos + 1):
        try:
            request = Request(url, headers={'User-Agent': USER_AGENT})
            with urlopen(request, timeout=settings.sunat_download_timeout_seconds) as response:
                with temp_path.open('wb') as output:
                    while True:
                        chunk = response.read(65536)
                        if not chunk:
                            break
                        output.write(chunk)
            temp_path.replace(destination)
            return destination
        except Exception as exc:  
            last_error = exc
            if temp_path.exists():
                temp_path.unlink()
            if attempt >= settings.sunat_download_retry_intentos:
                break
            time.sleep(settings.sunat_download_retry_espera_segundos)

    assert last_error is not None
    raise last_error


def fetch_remote_listing() -> list[SunatRemoteFile]:
    settings = get_settings()
    page_url = settings.sunat_source_page_url
    html = unescape(_http_get_text(page_url))

    matches: dict[str, SunatRemoteFile] = {}
    for href_match in HREF_PATTERN.finditer(html):
        href = href_match.group('href').strip().replace('\\', '/')
        file_match = REMOTE_FILE_PATTERN.search(href)
        if not file_match:
            continue
        file_name = file_match.group('name').upper()
        matches[file_name] = SunatRemoteFile(
            file_name=file_name,
            url=urljoin(page_url, href),
            extension=Path(file_name).suffix.lower(),
            source_page_url=page_url,
        )

    if matches:
        return sorted(matches.values(), key=lambda item: item.file_name)

    for file_match in REMOTE_FILE_PATTERN.finditer(html):
        file_name = file_match.group('name').upper()
        matches[file_name] = SunatRemoteFile(
            file_name=file_name,
            url=urljoin(page_url, file_name),
            extension=Path(file_name).suffix.lower(),
            source_page_url=page_url,
        )

    return sorted(matches.values(), key=lambda item: item.file_name)


def download_remote_file(file: SunatRemoteFile, destination_dir: Path) -> Path:
    destination = destination_dir / file.file_name
    return _http_download_file(file.url, destination)
