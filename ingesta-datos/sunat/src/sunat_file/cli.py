import typer

from sunat_file.config import get_settings
from sunat_file.jobs.agro_filter_job import run_filter_agro
from sunat_file.jobs.import_job import run_import, sync_remote_files_to_inbox
from sunat_file.jobs.pipeline_job import run_pipeline_main
from sunat_file.jobs.scanner import scan_inbox

app = typer.Typer(help='CLI del pipeline liviano SUNAT.')


def _apply_runtime_overrides(
    fecha_inicio: str | None,
    fecha_fin: str | None,
    modo_carga: str | None,
) -> None:
    settings = get_settings()
    if fecha_inicio is not None:
        settings.sunat_fecha_corte_inicio = fecha_inicio
    if fecha_fin is not None:
        settings.sunat_fecha_corte_fin = fecha_fin
    if modo_carga is not None:
        settings.sunat_modo_carga = modo_carga


def _echo_control_status(result: dict[str, object]) -> None:
    typer.echo(
        'Control pendiente por sincronizar: '
        f"{result['control_status']['pending_records']}"
    )
    typer.echo(
        'Eventos de control pendientes por sincronizar: '
        f"{result['control_status']['pending_event_records']}"
    )
    if result['control_status']['pending_records']:
        typer.echo(f"Cache local de control: {result['control_status']['pending_path']}")
    if result['control_status']['pending_event_records']:
        typer.echo(f"Cache local de eventos de control: {result['control_status']['pending_events_path']}")


@app.command('scan-inbox')
def scan_inbox_command() -> None:
    files = scan_inbox()
    typer.echo(f'Archivos detectados: {len(files)}')
    for item in files:
        typer.echo(f'- {item.source_name} ({item.extension})')


@app.command('sync-remote')
def sync_remote_command() -> None:
    downloaded = sync_remote_files_to_inbox()
    typer.echo(f'Archivos remotos descargados: {len(downloaded)}')
    for item in downloaded:
        typer.echo(f'- {item}')


@app.command('run-import')
def run_import_command() -> None:
    results = run_import()
    typer.echo(f'Archivos procesados: {len(results)}')
    for item in results:
        typer.echo(f'- {item}')


@app.command('run-filter-fresh')
def run_filter_fresh_command(
    fecha_inicio: str | None = typer.Option(None, '--fecha-inicio'),
    fecha_fin: str | None = typer.Option(None, '--fecha-fin'),
    modo_carga: str | None = typer.Option(None, '--modo-carga'),
) -> None:
    _apply_runtime_overrides(fecha_inicio, fecha_fin, modo_carga)
    result = run_filter_agro()
    typer.echo(f"Registros frescos filtrados: {result['rows']}")
    typer.echo(f"Raw: {result['raw_path']}")
    typer.echo(f"Preview: {result['preview_path']}")
    typer.echo(f"Resumen productos: {result['resumen_path']}")
    typer.echo(f"Resumen subpartidas: {result['resumen_subpartidas_path']}")
    typer.echo(f"Catalogo homologado: {result['catalog_path']}")
    typer.echo(f"Catalogo territorial: {result['territory_path']}")
    typer.echo(f"Resumen regiones: {result['region_summary_path']}")
    typer.echo(f"Calidad ubigeo: {result['ubigeo_quality_path']}")
    typer.echo(f"Diccionario: {result['diccionario_path']}")
    typer.echo(f"Clean: {result['clean_path']}")


@app.command('run-main')
def run_main_command(
    fecha_inicio: str | None = typer.Option(None, '--fecha-inicio'),
    fecha_fin: str | None = typer.Option(None, '--fecha-fin'),
    modo_carga: str | None = typer.Option(None, '--modo-carga'),
) -> None:
    _apply_runtime_overrides(fecha_inicio, fecha_fin, modo_carga)
    result = run_pipeline_main()
    typer.echo(f"Archivos remotos descargados: {len(result['downloaded'])}")
    for item in result['downloaded']:
        typer.echo(f'- {item}')
    typer.echo(f"Archivos encontrados en inbox: {result['pending_files']}")
    typer.echo(f"Archivos importados: {len(result['imported'])}")
    for item in result['imported']:
        typer.echo(f'- {item}')
    fresh = result['fresh']
    typer.echo(f"Registros frescos filtrados: {fresh['rows']}")
    typer.echo(f"Raw: {fresh['raw_path']}")
    typer.echo(f"Catalogo homologado: {fresh['catalog_path']}")
    typer.echo(f"Catalogo territorial: {fresh['territory_path']}")
    typer.echo(f"Resumen regiones: {fresh['region_summary_path']}")
    typer.echo(f"Calidad ubigeo: {fresh['ubigeo_quality_path']}")
    typer.echo(f"Clean: {fresh['clean_path']}")
    _echo_control_status(result)


@app.command('run-filter-agro')
def run_filter_agro_legacy_command() -> None:
    run_filter_fresh_command()


@app.command('run-pipeline-agro')
def run_pipeline_agro_legacy_command() -> None:
    run_main_command()


if __name__ == '__main__':
    app()
