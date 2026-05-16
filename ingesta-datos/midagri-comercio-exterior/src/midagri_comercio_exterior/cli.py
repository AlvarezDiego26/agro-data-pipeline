import typer

from midagri_comercio_exterior.config import get_settings
from midagri_comercio_exterior.jobs.import_job import (
    rebuild_clean_datasets,
    run_import,
    sync_remote_files_to_inbox,
)
from midagri_comercio_exterior.jobs.pipeline_job import run_pipeline_main
from midagri_comercio_exterior.jobs.scanner import scan_inbox
from midagri_comercio_exterior.readers.remote import fetch_remote_listing
from midagri_comercio_exterior.storage.control import get_control_sync_status

app = typer.Typer(help="CLI del pipeline MIDAGRI Comercio Exterior Agrario.")


def _apply_runtime_overrides(
    fecha_inicio: str | None,
    fecha_fin: str | None,
    modo_carga: str | None,
) -> None:
    settings = get_settings()
    if fecha_inicio is not None:
        settings.midagri_ce_fecha_corte_inicio = fecha_inicio
    if fecha_fin is not None:
        settings.midagri_ce_fecha_corte_fin = fecha_fin
    if modo_carga is not None:
        settings.midagri_ce_modo_carga = modo_carga


def _echo_control_status(result: dict[str, object]) -> None:
    typer.echo(
        "Control pendiente por sincronizar: "
        f"{result['control_status']['pending_records']}"
    )
    typer.echo(
        "Eventos de control pendientes por sincronizar: "
        f"{result['control_status']['pending_event_records']}"
    )


@app.command("scan-inbox")
def scan_inbox_command() -> None:
    files = scan_inbox()
    typer.echo(f"Archivos detectados: {len(files)}")
    for item in files:
        typer.echo(f"- {item.source_name} ({item.extension})")


@app.command("sync-remote")
def sync_remote_command() -> None:
    downloaded = sync_remote_files_to_inbox()
    typer.echo(f"Archivos remotos descargados: {len(downloaded)}")
    for item in downloaded:
        typer.echo(f"- {item}")


@app.command("list-remote")
def list_remote_command() -> None:
    files = fetch_remote_listing()
    typer.echo(f"Archivos remotos detectados: {len(files)}")
    for item in files:
        typer.echo(
            " | ".join(
                [
                    item.file_name,
                    item.extension,
                    str(item.publication_year or ""),
                    str(item.content_length or ""),
                    item.last_modified or "",
                    item.remote_signature,
                ]
            )
        )


@app.command("run-import")
def run_import_command() -> None:
    results = run_import()
    typer.echo(f"Archivos procesados: {len(results)}")
    for item in results:
        typer.echo(f"- {item}")


@app.command("rebuild-clean")
def rebuild_clean_command() -> None:
    results = rebuild_clean_datasets()
    typer.echo(f"Resultados de reconstruccion: {len(results)}")
    for item in results:
        typer.echo(f"- {item}")


@app.command("run-main")
def run_main_command(
    fecha_inicio: str | None = typer.Option(None, "--fecha-inicio"),
    fecha_fin: str | None = typer.Option(None, "--fecha-fin"),
    modo_carga: str | None = typer.Option(None, "--modo-carga"),
) -> None:
    _apply_runtime_overrides(fecha_inicio, fecha_fin, modo_carga)
    result = run_pipeline_main()
    typer.echo(f"Archivos remotos descargados: {len(result['downloaded'])}")
    for item in result["downloaded"]:
        typer.echo(f"- {item}")
    typer.echo(f"Archivos encontrados en inbox: {result['pending_files']}")
    typer.echo(f"Archivos importados: {len(result['imported'])}")
    for item in result["imported"]:
        typer.echo(f"- {item}")
    _echo_control_status(result)


@app.command("control-status")
def control_status_command() -> None:
    status = get_control_sync_status()
    typer.echo(f"Control pendiente: {status['pending_records']}")
    typer.echo(f"Control local: {status['local_records']}")
    typer.echo(f"Eventos pendientes: {status['pending_event_records']}")
    typer.echo(f"Eventos locales: {status['local_event_records']}")


if __name__ == "__main__":
    app()
