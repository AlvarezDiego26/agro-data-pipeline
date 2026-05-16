from __future__ import annotations

from pathlib import Path

from pyarrow import fs

from midagri_comercio_exterior.config import get_settings


def save_raw_binary(content: bytes, *, publication_year: int | None, file_name: str) -> str:
    settings = get_settings()
    year_token = str(publication_year or "sin_anio")
    relative_path = f"raw_archivos/anio_publicacion={year_token}/{file_name}"

    if settings.is_minio:
        destination = f"s3://{settings.minio_bucket}/{settings.midagri_ce_minio_prefix.strip('/')}/{relative_path}"
        _save_binary_s3(content=content, destination=destination)
        return destination

    destination = settings.raw_dir / "midagri_comercio_exterior" / relative_path
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
