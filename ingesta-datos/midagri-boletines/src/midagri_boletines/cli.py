import typer
from midagri_boletines.config import get_settings
from midagri_boletines.jobs.bulletin_job import run_pipeline
from midagri_boletines.jobs.cifras_job import run_cifras_pipeline

app = typer.Typer(help="CLI del pipeline MIDAGRI Boletines (Diario y Mensual).")


def _apply_runtime_overrides(
    fecha_inicio: str | None,
    fecha_fin: str | None,
    modo_carga: str | None,
) -> None:
    settings = get_settings()
    if fecha_inicio is not None:
        settings.midagri_boletines_fecha_inicio = fecha_inicio
    if fecha_fin is not None:
        settings.midagri_boletines_fecha_fin = fecha_fin
    if modo_carga is not None:
        settings.midagri_boletines_modo_carga = modo_carga


@app.command("run-main")
def run_main_command(
    fecha_inicio: str | None = typer.Option(None, "--fecha-inicio"),
    fecha_fin: str | None = typer.Option(None, "--fecha-fin"),
    modo_carga: str | None = typer.Option(None, "--modo-carga"),
) -> None:
    """Ejecuta ambos pipelines (diario y mensual) en modo normal o incremental."""
    _apply_runtime_overrides(fecha_inicio, fecha_fin, modo_carga)
    
    # 1. Ejecutar Ingesta de Boletines Diarios (GMML)
    run_pipeline(
        fecha_inicio=fecha_inicio,
        fecha_fin=fecha_fin,
        modo_carga=modo_carga,
        rebuild_clean=False,
    )
    
    # 2. Ejecutar Ingesta de Boletines Mensuales (El Agro en Cifras)
    run_cifras_pipeline(
        fecha_inicio=fecha_inicio,
        fecha_fin=fecha_fin,
        modo_carga=modo_carga,
        rebuild_clean=False,
    )


@app.command("rebuild-clean")
def rebuild_clean_command(
    fecha_inicio: str | None = typer.Option(None, "--fecha-inicio"),
    fecha_fin: str | None = typer.Option(None, "--fecha-fin"),
    modo_carga: str | None = typer.Option(None, "--modo-carga"),
) -> None:
    """Ejecuta ambos pipelines (diario y mensual) en modo de recreación limpia (sobreescritura)."""
    _apply_runtime_overrides(fecha_inicio, fecha_fin, modo_carga)
    
    # 1. Re-construir Boletines Diarios (GMML)
    run_pipeline(
        fecha_inicio=fecha_inicio,
        fecha_fin=fecha_fin,
        modo_carga=modo_carga,
        rebuild_clean=True,
    )
    
    # 2. Re-construir Boletines Mensuales (El Agro en Cifras)
    run_cifras_pipeline(
        fecha_inicio=fecha_inicio,
        fecha_fin=fecha_fin,
        modo_carga=modo_carga,
        rebuild_clean=True,
    )


if __name__ == "__main__":
    app()
