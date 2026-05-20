from __future__ import annotations

import re
from datetime import date
from pathlib import Path

from pyarrow import fs

from midagri_boletines.config import get_settings


def _sanitize_file_name(value: str) -> str:
    cleaned = re.sub(r'[<>:"/\\|?*]', "_", value).strip().rstrip(".")
    return cleaned or "archivo_fuente"


def save_raw_binary(
    content: bytes,
    *,
    source_family: str,
    file_name: str,
    publication_date: date | None = None,
    publication_year: int | None = None,
) -> str:
    settings = get_settings()
    safe_name = _sanitize_file_name(file_name)

    if source_family == "gmml_diario":
        date_token = publication_date.isoformat() if publication_date else "sin_fecha"
        relative_path = f"raw/gmml_diario/fecha_publicacion={date_token}/{safe_name}"
    else:
        year_token = str(publication_year or "sin_anio")
        relative_path = f"raw/agro_en_cifras/anio_publicacion={year_token}/{safe_name}"

    if settings.is_minio:
        destination = f"s3://{settings.minio_bucket}/{settings.minio_prefix.strip('/')}/{relative_path}"
        _save_binary_s3(content=content, destination=destination)
        return destination

    destination = settings.midagri_boletines_landing_dir / relative_path
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(content)
    return str(destination)


def _save_binary_s3(*, content: bytes, destination: str) -> None:
    settings = get_settings()
    endpoint = settings.minio_endpoint.replace("http://", "").replace("https://", "")
    filesystem = fs.S3FileSystem(
        access_key=settings.minio_access_key,
        secret_key=settings.minio_secret_key,
        endpoint_override=endpoint,
        region=settings.minio_region,
        scheme="http",
    )
    s3_path = destination.removeprefix("s3://")
    parent = s3_path.rsplit("/", 1)[0]
    filesystem.create_dir(parent, recursive=True)
    with filesystem.open_output_stream(s3_path) as stream:
        stream.write(content)
