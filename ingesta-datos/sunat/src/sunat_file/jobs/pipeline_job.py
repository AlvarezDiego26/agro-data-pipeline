from __future__ import annotations

from loguru import logger

from sunat_file.jobs.agro_filter_job import run_filter_agro
from sunat_file.jobs.import_job import run_import, sync_remote_files_to_inbox
from sunat_file.jobs.scanner import scan_inbox
from sunat_file.storage.control import (
    get_control_sync_status,
    sync_pending_control_events,
    sync_pending_control_state,
)


def run_pipeline_main() -> dict[str, object]:
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
    fresh = run_filter_agro()
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
