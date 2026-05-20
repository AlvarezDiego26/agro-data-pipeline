from datetime import date, timedelta

import polars as pl
from deltalake import DeltaTable
from loguru import logger

from midagri_boletines.config import get_settings
from midagri_boletines.extractors.siea_extractor import download_daily_pdf
from midagri_boletines.storage.merge import deduplicate_dataset, save_delta_table
from midagri_boletines.storage.raw import save_raw_binary
from midagri_boletines.transformers.pdf_transformer import (
    normalize_daily_gmml_dataframe,
    parse_daily_gmml_pdf,
)

BASE_DATASET_NAME = "base/base_gmml_diario"
CURATED_DATASET_NAME = "curated/gmml_diario_agrario"


def get_last_ingested_date(dataset_name: str) -> date | None:
    """Escanea la metadata de la tabla Delta para obtener la ultima fecha materializada."""
    settings = get_settings()
    uri = settings.build_delta_uri(dataset_name)
    storage_options = settings.delta_storage_options

    try:
        dt = DeltaTable(uri, storage_options=storage_options)
        df = pl.scan_pyarrow_dataset(dt.to_pyarrow_dataset())
        max_date_df = df.select(pl.col("fecha").max()).collect()
        if not max_date_df.is_empty() and max_date_df["fecha"][0] is not None:
            return max_date_df["fecha"][0]
    except Exception as exc:
        logger.debug(f"No se pudo leer la ultima fecha de la tabla Delta (puede no existir aun): {exc}")

    return None


def run_pipeline(
    fecha_inicio: str | None = None,
    fecha_fin: str | None = None,
    modo_carga: str | None = None,
    rebuild_clean: bool = False,
) -> None:
    """Ejecuta el pipeline de ingesta diaria GMML."""
    settings = get_settings()

    if fecha_inicio:
        settings.midagri_boletines_fecha_inicio = fecha_inicio
    if fecha_fin:
        settings.midagri_boletines_fecha_fin = fecha_fin
    if modo_carga:
        settings.midagri_boletines_modo_carga = modo_carga

    logger.info("Iniciando pipeline de ingesta de boletines diarios GMML (SIEA)...")

    start_date = settings.fecha_inicio_resuelta
    end_date = settings.fecha_fin_resuelta

    if not rebuild_clean and settings.is_incremental:
        last_date = get_last_ingested_date(CURATED_DATASET_NAME)
        if last_date:
            start_date = last_date + timedelta(days=1)
            logger.info(f"Carga incremental activa. Ultima fecha registrada: {last_date}. Proxima fecha: {start_date}")
        else:
            logger.info(f"No se encontro historico previo materializado. Iniciando backfill desde: {start_date}")

    if start_date > end_date:
        logger.success(f"La base diaria ya esta al dia. No hay nuevas fechas por procesar ({start_date} a {end_date}).")
        return

    logger.info(f"Procesando rango diario: {start_date} al {end_date}")

    current_date = start_date
    processed_count = 0
    failures_count = 0

    while current_date <= end_date:
        logger.info(f"--- Procesando dia: {current_date} ---")

        try:
            pdf_bytes = download_daily_pdf(current_date, timeout=settings.midagri_boletines_timeout_seconds)
            if not pdf_bytes:
                logger.info(f"No se encontro boletin diario publicado para la fecha: {current_date}")
                current_date += timedelta(days=1)
                continue

            raw_path = None
            if settings.midagri_boletines_save_raw_binary:
                raw_path = save_raw_binary(
                    pdf_bytes,
                    source_family="gmml_diario",
                    file_name=f"boletin_gmml_{current_date.isoformat()}.pdf",
                    publication_date=current_date,
                )

            base_df = parse_daily_gmml_pdf(pdf_bytes, current_date)
            if base_df.is_empty():
                logger.warning(f"El reporte del dia {current_date} no contenia registros diarios parseables.")
                current_date += timedelta(days=1)
                continue

            base_df = base_df.with_columns(
                pl.col("fecha").alias("fecha_particion"),
                pl.lit(raw_path).alias("ruta_raw_origen"),
            )
            curated_df = normalize_daily_gmml_dataframe(base_df)

            if settings.midagri_boletines_save_base_dataset:
                save_delta_table(
                    deduplicate_dataset(base_df, BASE_DATASET_NAME),
                    BASE_DATASET_NAME,
                    partition_by=["fecha_particion"],
                    overwrite=rebuild_clean and processed_count == 0,
                )

            if settings.midagri_boletines_save_curated_dataset:
                save_delta_table(
                    deduplicate_dataset(curated_df, CURATED_DATASET_NAME),
                    CURATED_DATASET_NAME,
                    partition_by=["fecha_particion"],
                    overwrite=rebuild_clean and processed_count == 0,
                )

            processed_count += 1
        except Exception as exc:
            logger.error(f"Error procesando la fecha {current_date}: {exc}")
            failures_count += 1

        current_date += timedelta(days=1)

    logger.success(
        f"Pipeline diario completado. Procesados con exito: {processed_count} dias. Fallidos: {failures_count} dias."
    )
