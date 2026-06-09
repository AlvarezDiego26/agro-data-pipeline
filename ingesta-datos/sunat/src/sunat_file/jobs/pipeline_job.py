from __future__ import annotations

from contextlib import contextmanager
import os
import time

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


def _empty_fresh_result(clean_path: str, *, skipped_reason: str | None = None) -> dict[str, object]:
    result: dict[str, object] = {
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
        'clean_path': clean_path,
    }
    if skipped_reason:
        result['skipped_reason'] = skipped_reason
    return result


@contextmanager
def _sunat_pipeline_lock():
    settings = get_settings()
    lock_path = settings.control_dir / 'sunat_pipeline.lock'
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    fd: int | None = None
    acquired = False

    try:
        try:
            fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            lock_age = time.time() - lock_path.stat().st_mtime if lock_path.exists() else 0
            if lock_age <= settings.sunat_pipeline_lock_ttl_seconds:
                yield False
                return
            logger.warning(
                'Se encontro un lock SUNAT vencido en {} con edad {:.0f}s; se intentara reemplazar.',
                lock_path,
                lock_age,
            )
            try:
                lock_path.unlink(missing_ok=True)
                fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            except FileExistsError:
                yield False
                return

        acquired = True
        assert fd is not None
        os.write(fd, f'pid={os.getpid()} started_at={time.time():.0f}\n'.encode('utf-8'))
        yield True
    finally:
        if fd is not None:
            os.close(fd)
        if acquired:
            try:
                lock_path.unlink(missing_ok=True)
            except Exception:
                logger.warning('No se pudo liberar el lock SUNAT en {}', lock_path)


def run_pipeline_main() -> dict[str, object]:
    settings = get_settings()
    with _sunat_pipeline_lock() as lock_acquired:
        if not lock_acquired:
            logger.warning('Se omite pipeline SUNAT porque ya hay otra ejecucion en curso.')
            control_status = get_control_sync_status()
            return {
                'downloaded': [],
                'pending_files': 0,
                'imported': [],
                'fresh': _empty_fresh_result(
                    settings.build_delta_uri('exportaciones_filtradas'),
                    skipped_reason='sunat_pipeline_already_running',
                ),
                'control_sync': {'synced': True, 'pending_records': control_status['pending_records'], 'target': 'skipped-lock-active'},
                'control_events_sync': {
                    'synced': True,
                    'pending_records': control_status['pending_event_records'],
                    'target': 'skipped-lock-active',
                },
                'control_status': control_status,
            }
        return _run_pipeline_main_locked(settings)


def _run_pipeline_main_locked(settings) -> dict[str, object]:
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
                'fresh': _empty_fresh_result(settings.build_delta_uri('exportaciones_filtradas')),
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
