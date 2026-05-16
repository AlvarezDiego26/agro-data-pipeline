from __future__ import annotations

import hashlib
import re
import shutil
from datetime import date
from pathlib import Path

import polars as pl
from loguru import logger

from midagri_comercio_exterior.config import get_settings
from midagri_comercio_exterior.jobs.scanner import scan_inbox
from midagri_comercio_exterior.readers.files import extract_supported_zip_members, read_supported_file
from midagri_comercio_exterior.readers.remote import download_remote_file, fetch_remote_listing
from midagri_comercio_exterior.storage.control import (
    append_control_events,
    build_control_event_id,
    build_control_record_timestamp,
    list_scope_values_by_status,
    read_control_table,
    sync_pending_control_events,
    sync_pending_control_state,
    upsert_control_records,
)
from midagri_comercio_exterior.storage.delta import save_delta_table
from midagri_comercio_exterior.storage.merge import deduplicate_dataset, normalize_dataset
from midagri_comercio_exterior.transformers.analytics import (
    ANALYTICS_DATASET,
    INVENTORY_DATASET,
    build_analytics_dataset,
    build_sheet_inventory,
)

BASE_DATASET = "base_comercio_exterior"
REMOTE_LISTING_DATASET = "fuentes_remotas_midagri"
RAW_FILES_DATASET = "archivos_fuente_midagri"
SUPPORTED_DIRECT_EXTENSIONS = {".xlsx", ".xls"}
SUPPORTED_ARCHIVE_EXTENSIONS = {".zip"}
YEAR_PATTERN = re.compile(r"(20\d{2})")


def _build_control_row(
    *,
    modulo: str,
    dataset: str,
    scope_tipo: str,
    scope_valor: str,
    estado: str,
    mensaje_error: str | None = None,
    extra: dict[str, object] | None = None,
) -> dict[str, object]:
    now = build_control_record_timestamp()
    row = {
        "fuente": "midagri_comercio_exterior",
        "modulo": modulo,
        "dataset": dataset,
        "scope_tipo": scope_tipo,
        "scope_valor": scope_valor,
        "modo_carga": get_settings().midagri_ce_modo_carga,
        "estado": estado,
        "mensaje_error": mensaje_error or "",
        "ejecutado_por": "midagri_comercio_exterior",
        "fecha_ejecucion": now,
        "fecha_actualizacion": now,
    }
    if extra:
        row.update(extra)
    return row


def _persist_event(
    modulo: str,
    dataset: str,
    scope_tipo: str,
    scope_valor: str,
    estado: str,
    mensaje_error: str | None = None,
    *,
    extra: dict[str, object] | None = None,
) -> None:
    row = _build_control_row(
        modulo=modulo,
        dataset=dataset,
        scope_tipo=scope_tipo,
        scope_valor=scope_valor,
        estado=estado,
        mensaje_error=mensaje_error,
        extra=extra,
    )
    event_df = pl.DataFrame([{**row, "evento_id": build_control_event_id()}])
    append_control_events(event_df)


def _persist_control(
    modulo: str,
    dataset: str,
    scope_tipo: str,
    scope_valor: str,
    estado: str,
    mensaje_error: str | None = None,
    *,
    extra: dict[str, object] | None = None,
) -> None:
    row = _build_control_row(
        modulo=modulo,
        dataset=dataset,
        scope_tipo=scope_tipo,
        scope_valor=scope_valor,
        estado=estado,
        mensaje_error=mensaje_error,
        extra=extra,
    )
    control_df = pl.DataFrame([row])
    upsert_control_records(control_df)


def _persist_download_source_status(estado: str, mensaje_error: str | None = None) -> None:
    source_page = get_settings().midagri_ce_source_page_url
    _persist_control("remote_scan", REMOTE_LISTING_DATASET, "fuente", source_page, estado, mensaje_error)
    _persist_event("remote_scan", REMOTE_LISTING_DATASET, "fuente", source_page, estado, mensaje_error)


def _persist_download_control(remote_file, estado: str, mensaje_error: str | None = None, *, local_hash: str = "", local_size: int | None = None) -> None:
    extra = {
        "archivo_origen": remote_file.file_name,
        "archivo_url": remote_file.url,
        "archivo_firma_remota": remote_file.remote_signature,
        "archivo_tamano_bytes": local_size if local_size is not None else remote_file.content_length,
        "archivo_hash": local_hash,
        "archivo_ultima_modificacion": remote_file.last_modified or "",
    }
    _persist_control(
        "download",
        REMOTE_LISTING_DATASET,
        "archivo_remoto_version",
        remote_file.remote_signature,
        estado,
        mensaje_error,
        extra=extra,
    )


def _persist_download_event(remote_file, estado: str, mensaje_error: str | None = None, *, local_hash: str = "", local_size: int | None = None) -> None:
    extra = {
        "archivo_origen": remote_file.file_name,
        "archivo_url": remote_file.url,
        "archivo_firma_remota": remote_file.remote_signature,
        "archivo_tamano_bytes": local_size if local_size is not None else remote_file.content_length,
        "archivo_hash": local_hash,
        "archivo_ultima_modificacion": remote_file.last_modified or "",
    }
    _persist_event(
        "download",
        REMOTE_LISTING_DATASET,
        "archivo_remoto_version",
        remote_file.remote_signature,
        estado,
        mensaje_error,
        extra=extra,
    )


def _persist_import_control(scope_tipo: str, scope_valor: str, estado: str, mensaje_error: str | None = None, *, extra: dict[str, object] | None = None) -> None:
    _persist_control("import", RAW_FILES_DATASET, scope_tipo, scope_valor, estado, mensaje_error, extra=extra)


def _persist_import_event(scope_tipo: str, scope_valor: str, estado: str, mensaje_error: str | None = None, *, extra: dict[str, object] | None = None) -> None:
    _persist_event("import", RAW_FILES_DATASET, scope_tipo, scope_valor, estado, mensaje_error, extra=extra)


def _file_sha256(path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _file_size_bytes(path) -> int:
    return path.stat().st_size


def _existing_remote_file_names() -> set[str]:
    settings = get_settings()
    known_names: set[str] = set()
    for folder in (settings.midagri_ce_inbox_dir, settings.midagri_ce_processed_dir):
        if not folder.exists():
            continue
        known_names.update(path.name.upper() for path in folder.glob("*") if path.is_file())
    return known_names


def _successful_remote_signatures() -> set[str]:
    return list_scope_values_by_status(
        fuente="midagri_comercio_exterior",
        modulo="download",
        dataset=REMOTE_LISTING_DATASET,
        scope_tipo="archivo_remoto_version",
        estados={"success"},
    )


def _successful_import_hashes() -> set[str]:
    return list_scope_values_by_status(
        fuente="midagri_comercio_exterior",
        modulo="import",
        dataset=RAW_FILES_DATASET,
        scope_tipo="archivo_version",
        estados={"success"},
    )


def _latest_download_metadata_by_name(source_name: str) -> dict[str, object]:
    control_df = read_control_table()
    if control_df.is_empty() or "archivo_origen" not in control_df.columns:
        return {}
    filtered = control_df.filter(
        (pl.col("fuente") == "midagri_comercio_exterior")
        & (pl.col("modulo") == "download")
        & (pl.col("dataset") == REMOTE_LISTING_DATASET)
        & (pl.col("estado") == "success")
        & (pl.col("archivo_origen") == source_name)
    )
    if filtered.is_empty():
        return {}
    if "fecha_actualizacion" in filtered.columns:
        filtered = filtered.sort("fecha_actualizacion", descending=True)
    return filtered.row(0, named=True)


def _persist_remote_listing_dataset(discovered_files) -> str:
    if not discovered_files:
        return ""
    settings = get_settings()
    if not settings.midagri_ce_save_remote_listing_dataset:
        return ""
    rows = [
        {
            "archivo_origen": remote_file.file_name,
            "archivo_url": remote_file.url,
            "archivo_extension": remote_file.extension,
            "archivo_titulo": remote_file.title,
            "archivo_anio_publicacion": remote_file.publication_year,
            "archivo_tamano_bytes": remote_file.content_length,
            "archivo_ultima_modificacion": remote_file.last_modified,
            "archivo_firma_remota": remote_file.remote_signature,
            "source_page_url": remote_file.source_page_url,
            "fecha_descubrimiento": build_control_record_timestamp(),
        }
        for remote_file in discovered_files
    ]
    listing_df = pl.DataFrame(rows)
    listing_df = normalize_dataset(listing_df, REMOTE_LISTING_DATASET)
    listing_df = deduplicate_dataset(listing_df, REMOTE_LISTING_DATASET)
    return save_delta_table(listing_df, REMOTE_LISTING_DATASET, ["archivo_anio_publicacion"])


def sync_remote_files_to_inbox() -> list[str]:
    settings = get_settings()
    downloaded: list[str] = []
    try:
        discovered_files = fetch_remote_listing()
        _persist_remote_listing_dataset(discovered_files)
        if discovered_files:
            _persist_download_source_status("success", f"archivos_detectados={len(discovered_files)}")
        else:
            _persist_download_source_status("no_data", "sin_archivos_remotos_detectados")
    except Exception as exc:
        _persist_download_source_status("error", str(exc))
        raise

    successful_signatures = _successful_remote_signatures()
    existing_names = _existing_remote_file_names()

    for remote_file in discovered_files:
        file_name = remote_file.file_name.upper()
        remote_signature = remote_file.remote_signature
        if remote_signature in successful_signatures:
            _persist_download_event(remote_file, "skipped", "archivo_remoto_ya_descargado")
            continue
        if file_name in existing_names:
            _persist_event(
                "download",
                REMOTE_LISTING_DATASET,
                "archivo_remoto_nombre",
                file_name,
                "skipped",
                "archivo_en_bandeja_o_procesado",
                extra={
                    "archivo_origen": remote_file.file_name,
                    "archivo_url": remote_file.url,
                    "archivo_firma_remota": remote_signature,
                },
            )
            continue
        try:
            temp_path = download_remote_file(remote_file, settings.midagri_downloads_dir)
            inbox_path = settings.midagri_ce_inbox_dir / remote_file.file_name
            if inbox_path.exists():
                inbox_path.unlink()
            temp_path.replace(inbox_path)
            local_hash = _file_sha256(inbox_path)
            local_size = _file_size_bytes(inbox_path)
            _persist_download_control(remote_file, "success", local_hash=local_hash, local_size=local_size)
            _persist_download_event(remote_file, "success", local_hash=local_hash, local_size=local_size)
            downloaded.append(remote_file.file_name)
        except Exception as exc:
            logger.exception("Fallo descargando archivo remoto MIDAGRI {name}", name=remote_file.file_name)
            _persist_download_control(remote_file, "error", str(exc))
            _persist_download_event(remote_file, "error", str(exc))

    return downloaded


def _extract_publication_year(source_name: str) -> int | None:
    match = YEAR_PATTERN.search(source_name)
    if match is None:
        return None
    return int(match.group(1))


def _with_lineage(
    raw_df: pl.DataFrame,
    source_file,
    member_file=None,
    *,
    file_hash: str = "",
    file_size_bytes: int | None = None,
    remote_signature: str = "",
) -> pl.DataFrame:
    member_name = member_file.name if member_file is not None else None
    publication_year = _extract_publication_year(source_file.name)
    lineage_df = raw_df.with_columns(
        pl.lit(source_file.name).alias("archivo_origen"),
        pl.lit(member_name).alias("archivo_miembro"),
        pl.lit(source_file.suffix.lower()).alias("tipo_archivo_origen"),
        pl.lit(publication_year).cast(pl.Int32, strict=False).alias("archivo_anio_publicacion"),
        pl.lit(date.today().isoformat()).alias("archivo_fecha_descarga"),
        pl.lit(str(publication_year or "")).alias("anio_publicacion"),
        pl.lit(file_hash).alias("archivo_hash"),
        pl.lit(file_size_bytes).cast(pl.Int64, strict=False).alias("archivo_tamano_bytes"),
        pl.lit(remote_signature).alias("archivo_firma_remota"),
    )
    hash_columns = sorted(lineage_df.columns)
    return lineage_df.with_columns(
        pl.concat_str(
            [pl.col(column).cast(pl.Utf8, strict=False).fill_null("") for column in hash_columns],
            separator="|",
        ).hash().cast(pl.Utf8).alias("registro_hash_fuente")
    )


def _persist_base_dataset(df: pl.DataFrame, *, overwrite: bool = False) -> str:
    settings = get_settings()
    if not settings.midagri_ce_save_base_dataset:
        return ""
    raw_df = normalize_dataset(df, BASE_DATASET)
    raw_df = deduplicate_dataset(raw_df, BASE_DATASET)
    return save_delta_table(raw_df, BASE_DATASET, ["anio_publicacion"], overwrite=overwrite)


def _persist_inventory_dataset(df: pl.DataFrame, *, overwrite: bool = False) -> str:
    settings = get_settings()
    if not settings.midagri_ce_save_inventory_dataset:
        return ""
    inventory_df = build_sheet_inventory(df)
    if inventory_df.is_empty():
        return ""
    inventory_df = normalize_dataset(inventory_df, INVENTORY_DATASET)
    inventory_df = deduplicate_dataset(inventory_df, INVENTORY_DATASET)
    return save_delta_table(
        inventory_df,
        INVENTORY_DATASET,
        ["anio_publicacion", "tipo_hoja"],
        overwrite=overwrite,
    )


def _persist_analytics_dataset(df: pl.DataFrame, *, overwrite: bool = False) -> str:
    analytics_df = build_analytics_dataset(df)
    if analytics_df.is_empty():
        return ""
    analytics_df = normalize_dataset(analytics_df, ANALYTICS_DATASET)
    analytics_df = deduplicate_dataset(analytics_df, ANALYTICS_DATASET)
    analytics_df = analytics_df.with_columns(
        pl.col("fecha_particion").cast(pl.Date, strict=False),
        pl.col("fecha_referencia_inicio").cast(pl.Date, strict=False),
        pl.col("fecha_referencia_fin").cast(pl.Date, strict=False),
    )
    return save_delta_table(analytics_df, ANALYTICS_DATASET, ["fecha_particion"], overwrite=overwrite)


def _persist_source_dataset(frames: list[pl.DataFrame], *, overwrite: bool = False) -> list[str]:
    if not frames:
        return []
    merged_source_df = pl.concat(frames, how="diagonal_relaxed")
    dataset_names: list[str] = []
    base_path = _persist_base_dataset(merged_source_df, overwrite=overwrite)
    if base_path:
        dataset_names.append(BASE_DATASET)
    inventory_path = _persist_inventory_dataset(merged_source_df, overwrite=overwrite)
    if inventory_path:
        dataset_names.append(INVENTORY_DATASET)
    analytics_path = _persist_analytics_dataset(merged_source_df, overwrite=overwrite)
    if analytics_path:
        dataset_names.append(ANALYTICS_DATASET)
    return dataset_names


def _process_excel_file(source_path) -> list[str]:
    settings = get_settings()
    publication_year = _extract_publication_year(source_path.name)
    file_hash = _file_sha256(source_path)
    file_size_bytes = _file_size_bytes(source_path)
    download_metadata = _latest_download_metadata_by_name(source_path.name)
    df = read_supported_file(source_path)
    df = _with_lineage(
        df,
        source_path,
        file_hash=file_hash,
        file_size_bytes=file_size_bytes,
        remote_signature=str(download_metadata.get("archivo_firma_remota", "")),
    )
    dataset_names = _persist_source_dataset([df])
    if settings.midagri_ce_save_raw_binary:
        from midagri_comercio_exterior.storage.raw import save_raw_binary

        save_raw_binary(
            source_path.read_bytes(),
            publication_year=publication_year,
            file_name=source_path.name,
        )
        return [RAW_FILES_DATASET, *dataset_names]
    return dataset_names


def _process_zip_file(source_path) -> list[str]:
    settings = get_settings()
    publication_year = _extract_publication_year(source_path.name)
    file_hash = _file_sha256(source_path)
    file_size_bytes = _file_size_bytes(source_path)
    download_metadata = _latest_download_metadata_by_name(source_path.name)
    temp_dir, members = extract_supported_zip_members(source_path)
    frames: list[pl.DataFrame] = []
    try:
        if not members:
            raise ValueError("El ZIP no contiene archivos .xlsx/.xls soportados.")
        for member in members:
            df = read_supported_file(member)
            df = _with_lineage(
                df,
                source_path,
                member,
                file_hash=file_hash,
                file_size_bytes=file_size_bytes,
                remote_signature=str(download_metadata.get("archivo_firma_remota", "")),
            )
            frames.append(df)
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)
    dataset_names = _persist_source_dataset(frames)
    if settings.midagri_ce_save_raw_binary:
        from midagri_comercio_exterior.storage.raw import save_raw_binary

        save_raw_binary(
            source_path.read_bytes(),
            publication_year=publication_year,
            file_name=source_path.name,
        )
        return [RAW_FILES_DATASET, *dataset_names]
    return dataset_names


def _rebuild_excel_file(source_path: Path) -> list[str]:
    file_hash = _file_sha256(source_path)
    file_size_bytes = _file_size_bytes(source_path)
    download_metadata = _latest_download_metadata_by_name(source_path.name)
    df = read_supported_file(source_path)
    df = _with_lineage(
        df,
        source_path,
        file_hash=file_hash,
        file_size_bytes=file_size_bytes,
        remote_signature=str(download_metadata.get("archivo_firma_remota", "")),
    )
    return _persist_source_dataset([df])


def _rebuild_zip_file(source_path: Path) -> list[str]:
    file_hash = _file_sha256(source_path)
    file_size_bytes = _file_size_bytes(source_path)
    download_metadata = _latest_download_metadata_by_name(source_path.name)
    temp_dir, members = extract_supported_zip_members(source_path)
    frames: list[pl.DataFrame] = []
    try:
        if not members:
            raise ValueError("El ZIP no contiene archivos .xlsx/.xls soportados.")
        for member in members:
            df = read_supported_file(member)
            df = _with_lineage(
                df,
                source_path,
                member,
                file_hash=file_hash,
                file_size_bytes=file_size_bytes,
                remote_signature=str(download_metadata.get("archivo_firma_remota", "")),
            )
            frames.append(df)
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)
    return _persist_source_dataset(frames)


def rebuild_clean_datasets() -> list[str]:
    settings = get_settings()
    results: list[str] = []
    seen_hashes: set[str] = set()
    source_paths: list[Path] = []
    frames: list[pl.DataFrame] = []
    accepted_sources: list[str] = []

    for folder in (settings.midagri_ce_processed_dir, settings.midagri_ce_inbox_dir):
        if not folder.exists():
            continue
        for path in sorted(folder.glob("*")):
            if not path.is_file():
                continue
            if path.suffix.lower() not in SUPPORTED_DIRECT_EXTENSIONS | SUPPORTED_ARCHIVE_EXTENSIONS:
                continue
            file_hash = _file_sha256(path)
            if file_hash in seen_hashes:
                results.append(f"{path.name}: SKIPPED archivo_duplicado_mismo_hash")
                continue
            seen_hashes.add(file_hash)
            source_paths.append(path)

    for source_path in source_paths:
        try:
            logger.info("Reconstruyendo capas limpias desde {name}", name=source_path.name)
            if source_path.suffix.lower() in SUPPORTED_ARCHIVE_EXTENSIONS:
                frame_hash = _file_sha256(source_path)
                file_size_bytes = _file_size_bytes(source_path)
                download_metadata = _latest_download_metadata_by_name(source_path.name)
                temp_dir, members = extract_supported_zip_members(source_path)
                try:
                    if not members:
                        raise ValueError("El ZIP no contiene archivos .xlsx/.xls soportados.")
                    for member in members:
                        df = read_supported_file(member)
                        df = _with_lineage(
                            df,
                            source_path,
                            member,
                            file_hash=frame_hash,
                            file_size_bytes=file_size_bytes,
                            remote_signature=str(download_metadata.get("archivo_firma_remota", "")),
                        )
                        frames.append(df)
                finally:
                    shutil.rmtree(temp_dir, ignore_errors=True)
            else:
                frame_hash = _file_sha256(source_path)
                file_size_bytes = _file_size_bytes(source_path)
                download_metadata = _latest_download_metadata_by_name(source_path.name)
                df = read_supported_file(source_path)
                df = _with_lineage(
                    df,
                    source_path,
                    file_hash=frame_hash,
                    file_size_bytes=file_size_bytes,
                    remote_signature=str(download_metadata.get("archivo_firma_remota", "")),
                )
                frames.append(df)
            accepted_sources.append(source_path.name)
        except Exception as exc:
            logger.exception("Fallo reconstruyendo capas limpias desde {name}", name=source_path.name)
            results.append(f"{source_path.name}: ERROR {exc}")

    dataset_names = _persist_source_dataset(frames, overwrite=True)
    if not dataset_names:
        results.append("rebuild-clean: SIN_DATOS_ANALITICOS")
        return results
    for source_name in accepted_sources:
        for dataset_name in dataset_names:
            results.append(f"{source_name} -> {dataset_name}")
    return results


def run_import() -> list[str]:
    settings = get_settings()
    processed: list[str] = []
    sync_pending_control_state()
    sync_pending_control_events()
    successful_import_hashes = _successful_import_hashes()

    for file in scan_inbox():
        try:
            file_hash = _file_sha256(file.path)
            if file_hash in successful_import_hashes:
                shutil.move(str(file.path), str(settings.midagri_ce_processed_dir / file.path.name))
                _persist_import_event(
                    "archivo_version",
                    file_hash,
                    "skipped",
                    "version_ya_importada",
                    extra={"archivo_origen": file.source_name},
                )
                processed.append(f"{file.source_name}: SKIPPED version_ya_importada")
                continue

            logger.info("Procesando archivo {name}", name=file.source_name)
            if file.extension in SUPPORTED_ARCHIVE_EXTENSIONS:
                dataset_names = _process_zip_file(file.path)
            elif file.extension in SUPPORTED_DIRECT_EXTENSIONS:
                dataset_names = _process_excel_file(file.path)
            else:
                logger.warning("Archivo no soportado: {name}", name=file.source_name)
                _persist_import_control("archivo", file.source_name, "skipped", "archivo_no_soportado")
                _persist_import_event("archivo", file.source_name, "skipped", "archivo_no_soportado")
                continue

            shutil.move(str(file.path), str(settings.midagri_ce_processed_dir / file.path.name))
            import_extra = {"archivo_origen": file.source_name, "archivo_hash": file_hash}
            _persist_import_control("archivo", file.source_name, "success", extra=import_extra)
            _persist_import_event("archivo", file.source_name, "success", extra=import_extra)
            _persist_import_control("archivo_version", file_hash, "success", extra={"archivo_origen": file.source_name})
            _persist_import_event("archivo_version", file_hash, "success", extra={"archivo_origen": file.source_name})
            successful_import_hashes.add(file_hash)
            for dataset_name in dataset_names:
                processed.append(f"{file.source_name} -> {dataset_name}")
        except Exception as exc:
            logger.exception("Fallo procesando {name}", name=file.source_name)
            shutil.move(str(file.path), str(settings.midagri_ce_error_dir / file.path.name))
            _persist_import_control("archivo", file.source_name, "error", str(exc))
            _persist_import_event("archivo", file.source_name, "error", str(exc))
            processed.append(f"{file.source_name}: ERROR {exc}")
    return processed
