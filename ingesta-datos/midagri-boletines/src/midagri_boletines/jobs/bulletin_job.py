from datetime import date, timedelta
import polars as pl
from deltalake import DeltaTable
from loguru import logger

from midagri_boletines.config import get_settings
from midagri_boletines.extractors.siea_extractor import download_daily_pdf
from midagri_boletines.transformers.pdf_transformer import parse_daily_gmml_pdf
from midagri_boletines.storage.merge import deduplicate_dataset, save_delta_table

DATASET_NAME = "base_gmml_diario"


def get_last_ingested_date(dataset_name: str) -> date | None:
    """Escanea la metadata de la tabla Delta para obtener la última fecha materializada."""
    settings = get_settings()
    uri = settings.build_delta_uri(dataset_name)
    storage_options = settings.delta_storage_options
    
    try:
        dt = DeltaTable(uri, storage_options=storage_options)
        df = pl.scan_pyarrow_dataset(dt.to_pyarrow_dataset())
        max_date_df = df.select(pl.col("fecha").max()).collect()
        if not max_date_df.is_empty() and max_date_df["fecha"][0] is not None:
            return max_date_df["fecha"][0]
    except Exception as e:
        logger.debug(f"No se pudo leer la última fecha de la tabla Delta (puede no existir aún): {e}")
    
    return None


def run_pipeline(
    fecha_inicio: str | None = None,
    fecha_fin: str | None = None,
    modo_carga: str | None = None,
    rebuild_clean: bool = False
) -> None:
    """
    Ejecuta el pipeline de ingesta de boletines diarios de precios y abastecimiento del GMML.
    """
    settings = get_settings()
    
    # Sobreescribir configuraciones si vienen como argumentos de Prefect
    if fecha_inicio:
        settings.midagri_boletines_fecha_inicio = fecha_inicio
    if fecha_fin:
        settings.midagri_boletines_fecha_fin = fecha_fin
    if modo_carga:
        settings.midagri_boletines_modo_carga = modo_carga

    logger.info("Iniciando Pipeline de Ingesta de Boletines Diarios GMML (SIEA)...")

    # Resolver rango de fechas
    start_date = settings.fecha_inicio_resuelta
    end_date = settings.fecha_fin_resuelta

    if not rebuild_clean and settings.is_incremental:
        last_date = get_last_ingested_date(DATASET_NAME)
        if last_date:
            start_date = last_date + timedelta(days=1)
            logger.info(f"Carga Incremental activa. Última fecha registrada: {last_date}. Próxima fecha: {start_date}")
        else:
            logger.info(f"No se encontró histórico previo materializado. Iniciando backfill desde: {start_date}")

    if start_date > end_date:
        logger.success(f"La base de datos ya está al día. No hay nuevas fechas por procesar (Rango solicitado: {start_date} a {end_date}).")
        return

    logger.info(f"Procesando rango de fechas: {start_date} al {end_date}")

    current_date = start_date
    processed_count = 0
    failures_count = 0

    while current_date <= end_date:
        # Los fines de semana (Sábado y Domingo) no suelen publicarse reportes diarios separados,
        # pero probamos de igual manera por si se registran retroactivamente
        logger.info(f"--- Procesando Día: {current_date} ---")
        
        try:
            pdf_bytes = download_daily_pdf(current_date, timeout=settings.midagri_boletines_timeout_seconds)
            
            if pdf_bytes:
                # Transformar PDF en DataFrame Polars
                df = parse_daily_gmml_pdf(pdf_bytes, current_date)
                
                if not df.is_empty():
                    # Crear columna física fecha_particion igual a fecha
                    df = df.with_columns(
                        pl.col("fecha").alias("fecha_particion")
                    )
                    
                    # Deduplicación y Persistencia Delta Lake
                    df = deduplicate_dataset(df, DATASET_NAME)
                    save_delta_table(
                        df, 
                        DATASET_NAME, 
                        partition_by=["fecha_particion"], 
                        overwrite=rebuild_clean and processed_count == 0
                    )
                    processed_count += 1
                else:
                    logger.warning(f"El reporte del día {current_date} no contenía registros de precios parseables.")
            else:
                logger.info(f"No se encontró boletín diario publicado para la fecha: {current_date}")
        
        except Exception as e:
            logger.error(f"Error procesando la fecha {current_date}: {e}")
            failures_count += 1

        current_date += timedelta(days=1)

    logger.success(f"Pipeline completado. Procesados con éxito: {processed_count} días. Fallidos: {failures_count} días.")
