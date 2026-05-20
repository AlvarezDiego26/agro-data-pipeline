from deltalake import DeltaTable
import polars as pl
from loguru import logger

from midagri_boletines.config import get_settings
from midagri_boletines.extractors.cifras_extractor import (
    download_monthly_file,
    fetch_monthly_remote_listing,
)
from midagri_boletines.storage.merge import deduplicate_dataset, save_delta_table
from midagri_boletines.storage.raw import save_raw_binary
from midagri_boletines.transformers.cifras_transformer import (
    build_monthly_agrarian_curated,
    process_monthly_file_bytes,
)

BASE_DATASET_NAME = "base/base_agro_en_cifras"
CURATED_DATASET_NAME = "curated/agro_en_cifras_agrario"


def get_ingested_signatures(dataset_name: str) -> set[str]:
    """Escanea la tabla Delta para obtener las firmas de archivos ya integrados."""
    settings = get_settings()
    uri = settings.build_delta_uri(dataset_name)
    storage_options = settings.delta_storage_options

    try:
        dt = DeltaTable(uri, storage_options=storage_options)
        df = pl.scan_pyarrow_dataset(dt.to_pyarrow_dataset())
        signatures_df = df.select(pl.col("archivo_firma_remota").unique()).collect()
        if not signatures_df.is_empty():
            return set(signatures_df["archivo_firma_remota"].to_list())
    except Exception as exc:
        logger.debug(f"No se pudo leer historico de firmas remotas para {dataset_name}: {exc}")

    return set()


def run_cifras_pipeline(
    fecha_inicio: str | None = None,
    fecha_fin: str | None = None,
    modo_carga: str | None = None,
    rebuild_clean: bool = False,
) -> None:
    """Ejecuta el pipeline mensual de 'El Agro en Cifras'."""
    settings = get_settings()

    if fecha_inicio:
        settings.midagri_boletines_fecha_inicio = fecha_inicio
    if fecha_fin:
        settings.midagri_boletines_fecha_fin = fecha_fin
    if modo_carga:
        settings.midagri_boletines_modo_carga = modo_carga

    logger.info("Iniciando pipeline mensual de boletines 'El Agro en Cifras'...")

    start_year = settings.fecha_inicio_resuelta.year
    end_year = settings.fecha_fin_resuelta.year
    logger.info(f"Rango de anos solicitado para mensual: {start_year} a {end_year}")

    ingested_signatures = set()
    if not rebuild_clean and settings.is_incremental:
        ingested_signatures = get_ingested_signatures(CURATED_DATASET_NAME)
        logger.info(f"Carga incremental activa. Firmas registradas anteriormente: {len(ingested_signatures)}")
    else:
        logger.info("Modo backfill o rebuild activo. Se reprocesaran todos los archivos descubiertos.")

    remote_files = fetch_monthly_remote_listing()

    to_process = []
    for file in remote_files:
        if file.publication_year is None:
            logger.warning(f"Ignorando {file.file_name} porque no se pudo deducir el ano de publicacion.")
            continue
        if not (start_year <= file.publication_year <= end_year):
            continue
        if file.remote_signature in ingested_signatures:
            logger.debug(f"Saltando {file.file_name} (ya integrado).")
            continue
        to_process.append(file)

    if not to_process:
        logger.success("No hay nuevos archivos mensuales por procesar.")
        return

    logger.info(f"Se procesaran {len(to_process)} archivos consolidados mensuales...")

    processed_count = 0
    failures_count = 0
    is_first_write = True

    for idx, file in enumerate(to_process, 1):
        logger.info(f"[{idx}/{len(to_process)}] Procesando archivo mensual: {file.file_name} (Ano {file.publication_year})")

        try:
            content_bytes = download_monthly_file(file, timeout=settings.midagri_boletines_timeout_seconds)

            raw_path = None
            if settings.midagri_boletines_save_raw_binary:
                raw_path = save_raw_binary(
                    content_bytes,
                    source_family="agro_en_cifras",
                    file_name=file.file_name,
                    publication_year=file.publication_year,
                )

            base_df = process_monthly_file_bytes(
                content_bytes,
                file_name=file.file_name,
                publication_year=file.publication_year,
                remote_signature=file.remote_signature,
            )

            if base_df.is_empty():
                logger.warning(f"El archivo {file.file_name} no contenia registros de celdas validos.")
                continue

            base_df = base_df.with_columns(pl.lit(raw_path).alias("ruta_raw_origen"))
            curated_df = build_monthly_agrarian_curated(base_df)

            overwrite_mode = rebuild_clean and is_first_write

            if settings.midagri_boletines_save_base_dataset:
                save_delta_table(
                    deduplicate_dataset(base_df, BASE_DATASET_NAME),
                    BASE_DATASET_NAME,
                    partition_by=["anio_publicacion"],
                    overwrite=overwrite_mode,
                )

            if settings.midagri_boletines_save_curated_dataset:
                save_delta_table(
                    deduplicate_dataset(curated_df, CURATED_DATASET_NAME),
                    CURATED_DATASET_NAME,
                    partition_by=["anio_publicacion", "categoria_agraria"],
                    overwrite=overwrite_mode,
                )

            processed_count += 1
            is_first_write = False
        except Exception as exc:
            logger.error(f"Error procesando el consolidado mensual {file.file_name}: {exc}")
            failures_count += 1

    logger.success(
        f"Pipeline mensual completado. Procesados con exito: {processed_count} archivos. Fallidos: {failures_count} archivos."
    )
