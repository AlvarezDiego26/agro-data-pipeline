import typer

from sisap_light.jobs.ciudades_job import (
    build_plan_mayoristas,
    build_plan_minoristas,
    run_full as run_ciudades_full,
    run_sample as run_ciudades_sample,
)
from sisap_light.jobs.master_job import run_pipeline_main
from sisap_light.jobs.precios_job import build_plan as build_precios_plan
from sisap_light.jobs.precios_job import run_full as run_precios_full
from sisap_light.jobs.precios_job import run_sample as run_precios_sample
from sisap_light.jobs.volumen_job import (
    build_plan as build_volumen_plan,
    inspect_home,
    inspect_sample_report,
    run_full as run_volumen_full,
    run_sample as run_volumen_sample,
)
from sisap_light.schemas import ModuloSisap

app = typer.Typer(help='CLI del proyecto SISAP liviano.')


@app.command('run-main')
def run_main_command() -> None:
    result = run_pipeline_main()
    typer.echo(f"Modulos: {', '.join(result['modulos'])}")
    if result['procedencias']:
        typer.echo(f"Procedencias: {', '.join(result['procedencias'])}")
    if result['regiones']:
        typer.echo(f"Regiones: {', '.join(result['regiones'])}")
    typer.echo(f"Bloques ejecutados: {len(result['resultados'])}")
    for item in result['resultados']:
        typer.echo(f'- {item}')


@app.command('inspect-home')
def inspect_home_command() -> None:
    data = inspect_home()
    typer.echo(f"Hidden inputs: {len(data['hidden_inputs'])}")
    typer.echo(f"PostID detectado: {data['post_id']}")
    typer.echo(f"Mercados detectados: {len(data['mercado_options'])}")
    typer.echo(f"Productos detectados: {len(data['producto_options'])}")
    typer.echo(f"Procedencias detectadas: {len(data['procedencia_options'])}")
    typer.echo(f"Variables detectadas: {len(data['variable_options'])}")


@app.command('inspect-sample-report')
def inspect_sample_report_command() -> None:
    data = inspect_sample_report()
    typer.echo(f"Query usada: {data['query']}")
    typer.echo(f"Titulos detectados: {data['titles']}")
    typer.echo(f"Primeras filas parseadas: {len(data['rows'])}")
    if data['rows']:
        typer.echo(str(data['rows'][0]))


@app.command('plan-volumen')
def plan_volumen() -> None:
    plan = build_volumen_plan()
    typer.echo(f"Queries de volumen: {len(plan)}")
    if plan:
        typer.echo(plan[0].model_dump_json(indent=2))


@app.command('run-volumen')
def run_volumen() -> None:
    output = run_volumen_full()
    typer.echo(f"Volumen consolidado guardado en: {output}")


@app.command('plan-precios')
def plan_precios() -> None:
    plan = build_precios_plan()
    typer.echo(f"Queries de precios: {len(plan)}")
    if plan:
        typer.echo(plan[0].model_dump_json(indent=2))


@app.command('run-precios')
def run_precios() -> None:
    output = run_precios_full()
    typer.echo(f"Precios consolidados guardados en: {output}")


@app.command('plan-ciudades-mayoristas')
def plan_ciudades_mayoristas() -> None:
    plan = build_plan_mayoristas()
    typer.echo(f"Queries de ciudades mayoristas: {len(plan)}")
    if plan:
        typer.echo(plan[0].model_dump_json(indent=2))


@app.command('plan-ciudades-minoristas')
def plan_ciudades_minoristas() -> None:
    plan = build_plan_minoristas()
    typer.echo(f"Queries de ciudades minoristas: {len(plan)}")
    if plan:
        typer.echo(plan[0].model_dump_json(indent=2))


@app.command('run-ciudades-mayoristas')
def run_ciudades_mayoristas() -> None:
    output = run_ciudades_full(ModuloSisap.CIUDADES_PRECIOS_MAYORISTAS)
    typer.echo(f"Ciudades mayoristas consolidadas guardadas en: {output}")


@app.command('run-ciudades-minoristas')
def run_ciudades_minoristas() -> None:
    output = run_ciudades_full(ModuloSisap.CIUDADES_PRECIOS_MINORISTAS)
    typer.echo(f"Ciudades minoristas consolidadas guardadas en: {output}")


@app.command('sample-volumen')
def sample_volumen() -> None:
    output = run_volumen_sample()
    typer.echo(f"Sample volumen guardado en: {output}")


@app.command('sample-precios')
def sample_precios() -> None:
    output = run_precios_sample()
    typer.echo(f"Sample precios guardado en: {output}")


@app.command('sample-ciudades-mayoristas')
def sample_ciudades_mayoristas() -> None:
    output = run_ciudades_sample(ModuloSisap.CIUDADES_PRECIOS_MAYORISTAS)
    typer.echo(f"Sample ciudades mayoristas guardado en: {output}")


@app.command('sample-ciudades-minoristas')
def sample_ciudades_minoristas() -> None:
    output = run_ciudades_sample(ModuloSisap.CIUDADES_PRECIOS_MINORISTAS)
    typer.echo(f"Sample ciudades minoristas guardado en: {output}")


if __name__ == '__main__':
    app()
