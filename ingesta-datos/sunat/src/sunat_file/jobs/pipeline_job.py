from __future__ import annotations

from loguru import logger

from sunat_file.config import get_settings
from sunat_file.jobs.agro_filter_job import run_filter_agro
from sunat_file.jobs.import_job import run_import, sync_remote_files_to_inbox
from sunat_file.jobs.scanner import scan_inbox
from sunat_file.storage.control import (
    get_control_sync_status,
    sync_pending_control_events,
    sync_pending_control_state,
)


def run_pipeline_main() -> dict[str, object]:
    settings = get_settings()
    logger.info('Iniciando pipeline SUNAT')
    logger.info(f'Ruta de control: {settings.control_dir}')
    sync_pending_control_state()
    sync_pending_control_events()
    downloaded: list[str] = []
    try:
        downloaded = sync_remote_files_to_inbox()
    except Exception:
        if scan_inbox():
            logger.exception('Fallo sincronizando archivos remotos SUNAT; se continua con archivos ya presentes en inbox')
        else:
            raise
    pending_files = scan_inbox()
    imported = run_import() if pending_files else []
    logger.info(f'Archivos ZIP importados: {len(imported)}')
    if not imported and settings.is_minio:
        from sunat_file.jobs.import_job import ZIP_CONSOLIDATED_DATASET
        import polars as pl

        source_uri = settings.build_delta_uri(ZIP_CONSOLIDATED_DATASET)
        try:
            pl.read_delta(source_uri, storage_options=settings.delta_storage_options).head(1)
        except Exception:
            logger.warning(
                'No existe la base consolidada SUNAT en MinIO y tampoco se importaron ZIPs en esta corrida. '
                'Verifica inbox/control local si buscas reconstruir desde cero.'
            )
            final_control_sync = sync_pending_control_state()
            final_events_sync = sync_pending_control_events()
            control_status = get_control_sync_status()
            return {
                'downloaded': downloaded,
                'pending_files': len(pending_files),
                'imported': imported,
                'fresh': {
                    'rows': 0,
                    'base_rows': 0,
                    'raw_path': '',
                    'preview_path': '',
                    'resumen_path': '',
                    'resumen_subpartidas_path': '',
                    'catalog_path': '',
                    'territory_path': '',
                    'region_summary_path': '',
                    'ubigeo_quality_path': '',
                    'diccionario_path': '',
                    'clean_path': settings.build_delta_uri('exportaciones_filtradas/tablon_maestro_agrario'),
                },
                'control_sync': final_control_sync,
                'control_events_sync': final_events_sync,
                'control_status': control_status,
            }
    fresh = run_filter_agro()
    logger.info(f'Registros base: {fresh.get("base_rows", 0)}')
    logger.info(f'Registros filtrados agrícolas: {fresh.get("rows", 0)}')
    logger.info(f'Ruta de extracción SUNAT: {settings.extraccion_dir}')
    logger.info(f'Ruta de consolidación agrícola: {settings.consolidacion_agricola_dir}')
    final_control_sync = sync_pending_control_state()
    final_events_sync = sync_pending_control_events()
    control_status = get_control_sync_status()
    return {
        'downloaded': downloaded,
        'pending_files': len(pending_files),
        'imported': imported,
        'fresh': fresh,
        'control_sync': final_control_sync,
        'control_events_sync': final_events_sync,
        'control_status': control_status,
    }


run_pipeline_agro = run_pipeline_main
