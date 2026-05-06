import typer

from sunat_file.jobs.agro_filter_job import run_filter_agro
from sunat_file.jobs.import_job import run_import
from sunat_file.jobs.pipeline_job import run_pipeline_main
from sunat_file.jobs.scanner import scan_inbox

app = typer.Typer(help='CLI del pipeline liviano SUNAT.')


@app.command('scan-inbox')
def scan_inbox_command() -> None:
    files = scan_inbox()
    typer.echo(f'Archivos detectados: {len(files)}')
    for item in files:
        typer.echo(f'- {item.source_name} ({item.extension})')


@app.command('run-import')
def run_import_command() -> None:
    results = run_import()
    typer.echo(f'Archivos procesados: {len(results)}')
    for item in results:
        typer.echo(f'- {item}')


@app.command('run-filter-fresh')
def run_filter_fresh_command() -> None:
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
def run_main_command() -> None:
    result = run_pipeline_main()
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


# Aliases heredados mientras se estabiliza el proyecto.
@app.command('run-filter-agro')
def run_filter_agro_legacy_command() -> None:
    run_filter_fresh_command()


@app.command('run-pipeline-agro')
def run_pipeline_agro_legacy_command() -> None:
    run_main_command()


if __name__ == '__main__':
    app()
