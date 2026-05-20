from __future__ import annotations

import hashlib
from html import unescape
import re
import time
from pathlib import Path
from urllib.parse import unquote, urljoin
from urllib.request import Request, urlopen

from midagri_comercio_exterior.config import get_settings
from midagri_comercio_exterior.schemas import MidagriRemoteFile

HREF_PATTERN = re.compile(r'href\s*=\s*["\'](?P<href>[^"\']+)["\']', re.IGNORECASE)
FILE_EXTENSION_PATTERN = re.compile(r"\.(xlsx|xls|zip)(?:\?.*)?$", re.IGNORECASE)
YEAR_PATTERN = re.compile(r"(20\d{2})")
USER_AGENT = "Mozilla/5.0 (compatible; AgroProyectoMIDAGRI/1.0)"
WINDOWS_INVALID_FILE_CHARS = re.compile(r'[<>:"/\\|?*]')


def _decode_remote_file_name(value: str) -> str:
    return unquote(value).strip()


def _sanitize_local_file_name(value: str) -> str:
    cleaned = WINDOWS_INVALID_FILE_CHARS.sub("_", value).strip().rstrip(".")
    return cleaned or "archivo_midagri"


def _http_get_text(url: str) -> str:
    settings = get_settings()
    last_error: Exception | None = None
    for attempt in range(1, settings.midagri_ce_download_retry_intentos + 1):
        try:
            request = Request(url, headers={"User-Agent": USER_AGENT})
            with urlopen(request, timeout=settings.midagri_ce_download_timeout_seconds) as response:
                encoding = response.headers.get_content_charset() or "utf-8"
                return response.read().decode(encoding, errors="replace")
        except Exception as exc:
            last_error = exc
            if attempt >= settings.midagri_ce_download_retry_intentos:
                break
            time.sleep(settings.midagri_ce_download_retry_espera_segundos)
    assert last_error is not None
    raise last_error


def _http_download_file(url: str, destination: Path) -> Path:
    settings = get_settings()
    destination.parent.mkdir(parents=True, exist_ok=True)
    temp_path = destination.with_suffix(destination.suffix + ".part")
    last_error: Exception | None = None

    for attempt in range(1, settings.midagri_ce_download_retry_intentos + 1):
        try:
            request = Request(url, headers={"User-Agent": USER_AGENT})
            with urlopen(request, timeout=settings.midagri_ce_download_timeout_seconds) as response:
                with temp_path.open("wb") as output:
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
            if attempt >= settings.midagri_ce_download_retry_intentos:
                break
            time.sleep(settings.midagri_ce_download_retry_espera_segundos)

    assert last_error is not None
    raise last_error


def _http_head_metadata(url: str) -> tuple[int | None, str | None]:
    settings = get_settings()
    try:
        request = Request(url, method="HEAD", headers={"User-Agent": USER_AGENT})
        with urlopen(request, timeout=settings.midagri_ce_download_timeout_seconds) as response:
            raw_length = response.headers.get("Content-Length", "").strip()
            content_length = int(raw_length) if raw_length.isdigit() else None
            last_modified = response.headers.get("Last-Modified")
            return content_length, (last_modified or "").strip() or None
    except Exception:
        return None, None


def _build_remote_signature(
    *,
    file_name: str,
    url: str,
    content_length: int | None,
    last_modified: str | None,
) -> str:
    payload = "|".join(
        [
            file_name.strip().upper(),
            url.strip(),
            str(content_length or ""),
            (last_modified or "").strip(),
        ]
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def fetch_remote_listing() -> list[MidagriRemoteFile]:
    settings = get_settings()
    page_url = settings.midagri_ce_source_page_url
    html = unescape(_http_get_text(page_url))
    matches: dict[str, MidagriRemoteFile] = {}

    for href_match in HREF_PATTERN.finditer(html):
        href = href_match.group("href").strip().replace("\\", "/")
        if not FILE_EXTENSION_PATTERN.search(href):
            continue
        full_url = urljoin(page_url, href)
        raw_file_name = Path(full_url.split("?", 1)[0]).name
        decoded_file_name = _decode_remote_file_name(raw_file_name)
        file_name = _sanitize_local_file_name(decoded_file_name)
        decoded_href = _decode_remote_file_name(href)
        scope = f"{decoded_href} {decoded_file_name}"
        year_match = YEAR_PATTERN.search(scope)
        publication_year = int(year_match.group(1)) if year_match else None
        content_length, last_modified = _http_head_metadata(full_url)
        key = file_name.upper()
        matches[key] = MidagriRemoteFile(
            file_name=file_name,
            url=full_url,
            extension=Path(file_name).suffix.lower(),
            source_page_url=page_url,
            title=decoded_file_name,
            publication_year=publication_year,
            content_length=content_length,
            last_modified=last_modified,
            remote_signature=_build_remote_signature(
                file_name=file_name,
                url=full_url,
                content_length=content_length,
                last_modified=last_modified,
            ),
        )

    return sorted(matches.values(), key=lambda item: ((item.publication_year or 0), item.file_name))


def download_remote_file(file: MidagriRemoteFile, destination_dir: Path) -> Path:
    destination = destination_dir / file.file_name
    return _http_download_file(file.url, destination)
