from __future__ import annotations

from sunat_file.jobs.agro_filter_job import run_filter_agro
from sunat_file.jobs.import_job import run_import
from sunat_file.jobs.scanner import scan_inbox


def run_pipeline_main() -> dict[str, object]:
    pending_files = scan_inbox()
    imported = run_import() if pending_files else []
    fresh = run_filter_agro()
    return {
        'pending_files': len(pending_files),
        'imported': imported,
        'fresh': fresh,
    }


# Alias temporal para no romper llamadas heredadas.
run_pipeline_agro = run_pipeline_main
