from deltalake import DeltaTable
import polars as pl
from loguru import logger

from midagri_boletines.config import get_settings
from midagri_boletines.extractors.cifras_extractor import fetch_monthly_remote_listing, download_monthly_file
from midagri_boletines.transformers.cifras_transformer import process_monthly_file_bytes
from midagri_boletines.storage.merge import deduplicate_dataset, save_delta_table

DATASET_NAME = "base_agro_en_cifras"


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
    except Exception as e:
        logger.debug(f"No se pudo leer histórico de firmas remota para {dataset_name}: {e}")
        
    return set()


def run_cifras_pipeline(
    fecha_inicio: str | None = None,
    fecha_fin: str | None = None,
    modo_carga: str | None = None,
    rebuild_clean: bool = False
) -> None:
    """
    Ejecuta el pipeline de ingesta para los consolidados mensuales de 'El Agro en Cifras'.
    """
    settings = get_settings()
    
    # Sobreescribir configuraciones si vienen como argumentos
    if fecha_inicio:
        settings.midagri_boletines_fecha_inicio = fecha_inicio
    if fecha_fin:
        settings.midagri_boletines_fecha_fin = fecha_fin
    if modo_carga:
        settings.midagri_boletines_modo_carga = modo_carga

    logger.info("Iniciando Pipeline de Boletines Mensuales (El Agro en Cifras)...")

    # Resolver rango de años a procesar
    start_year = settings.fecha_inicio_resuelta.year
    end_year = settings.fecha_fin_resuelta.year
    logger.info(f"Rango de años solicitado para mensual: {start_year} a {end_year}")

    # Obtener firmas ya procesadas si es carga incremental
    ingested_signatures = set()
    if not rebuild_clean and settings.is_incremental:
        ingested_signatures = get_ingested_signatures(DATASET_NAME)
        logger.info(f"Carga Incremental activa. Firmas registradas anteriormente: {len(ingested_signatures)}")
    else:
        logger.info("Modo Backfill o Rebuild Activo. Se reprocesarán todos los archivos descubiertos.")

    # 1. Obtener listado de archivos remotos
    remote_files = fetch_monthly_remote_listing()
    
    # 2. Filtrar archivos por año e incrementalidad
    to_process = []
    for file in remote_files:
        if file.publication_year is None:
            logger.warning(f"Ignorando archivo remoto {file.file_name} porque no se pudo deducir el año de publicación.")
            continue
            
        if not (start_year <= file.publication_year <= end_year):
            continue
            
        if file.remote_signature in ingested_signatures:
            logger.debug(f"Saltando {file.file_name} (ya integrado).")
            continue
            
        to_process.append(file)

    if not to_process:
        logger.success("No hay nuevos archivos consolidados mensuales por procesar.")
        return

    logger.info(f"Se procesarán {len(to_process)} archivos consolidados mensuales...")

    processed_count = 0
    failures_count = 0
    is_first_write = True

    for idx, file in enumerate(to_process, 1):
        logger.info(f"[{idx}/{len(to_process)}] Procesando Archivo Mensual: {file.file_name} (Año {file.publication_year})")
        
        try:
            # A) Descargar
            content_bytes = download_monthly_file(file, timeout=settings.midagri_boletines_timeout_seconds)
            
            # B) Parsear Excel y normalizar a celdas des-pivotadas en memoria
            df = process_monthly_file_bytes(
                content_bytes,
                file_name=file.file_name,
                publication_year=file.publication_year,
                remote_signature=file.remote_signature
            )
            
            if not df.is_empty():
                # C) Deduplicación en memoria
                df = deduplicate_dataset(df, DATASET_NAME)
                
                # D) Guardar en Delta Lake
                overwrite_mode = rebuild_clean and is_first_write
                save_delta_table(
                    df,
                    DATASET_NAME,
                    partition_by=["anio_publicacion"],
                    overwrite=overwrite_mode
                )
                
                processed_count += 1
                is_first_write = False
            else:
                logger.warning(f"El archivo {file.file_name} no contenía registros de celdas válidos.")
                
        except Exception as e:
            logger.error(f"Error procesando el consolidado mensual {file.file_name}: {e}")
            failures_count += 1

    logger.success(f"Pipeline mensual completado. Procesados con éxito: {processed_count} archivos. Fallidos: {failures_count} archivos.")
