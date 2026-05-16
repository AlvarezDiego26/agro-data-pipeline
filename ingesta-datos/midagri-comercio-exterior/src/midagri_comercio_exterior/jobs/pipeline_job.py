from __future__ import annotations

from loguru import logger

from midagri_comercio_exterior.config import get_settings
from midagri_comercio_exterior.jobs.import_job import run_import, sync_remote_files_to_inbox
from midagri_comercio_exterior.jobs.scanner import scan_inbox
from midagri_comercio_exterior.storage.control import (
    get_control_sync_status,
    sync_pending_control_events,
    sync_pending_control_state,
)


def run_pipeline_main() -> dict[str, object]:
    settings = get_settings()
    logger.info("Iniciando pipeline MIDAGRI Comercio Exterior")
    logger.info(f"Ruta de control: {settings.control_dir}")
    sync_pending_control_state()
    sync_pending_control_events()
    downloaded = sync_remote_files_to_inbox()
    pending_files = scan_inbox()
    imported = run_import() if pending_files else []
    logger.info(f"Archivos descargados: {len(downloaded)}")
    logger.info(f"Archivos importados: {len(imported)}")
    final_control_sync = sync_pending_control_state()
    final_events_sync = sync_pending_control_events()
    control_status = get_control_sync_status()
    return {
        "downloaded": downloaded,
        "pending_files": len(pending_files),
        "imported": imported,
        "control_sync": final_control_sync,
        "control_events_sync": final_events_sync,
        "control_status": control_status,
    }
