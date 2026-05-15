import typer

from sisap_light.config import get_settings
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
    inspect_home_mercado,
    inspect_sample_report,
    run_full as run_volumen_full,
    run_sample as run_volumen_sample,
)
from sisap_light.schemas import ModuloSisap

app = typer.Typer(help='CLI del proyecto SISAP liviano.')


def _run_and_report(runner):
    try:
        return runner()
    except Exception as exc:
        typer.echo(f'ERROR: {exc}')
        raise typer.Exit(code=1)


def _apply_runtime_overrides(
    fecha_inicio: str | None,
    fecha_fin: str | None,
    modo_carga: str | None,
    modulos: str | None,
    procedencias: str | None,
    mercados: str | None,
    regiones: str | None,
    producto_codigo: str | None,
    producto_nombre: str | None,
    max_queries: int | None,
    max_productos: int | None,
    max_scopes: int | None,
    scope_workers: int | None,
    shard_workers: int | None,
    product_batch_size: int | None,
) -> None:
    settings = get_settings()
    if fecha_inicio is not None:
        settings.sisap_fecha_inicio = fecha_inicio
    if fecha_fin is not None:
        settings.sisap_fecha_fin = fecha_fin
    if modo_carga is not None:
        settings.sisap_modo_carga = modo_carga
    if modulos is not None:
        settings.sisap_modulos = modulos
    if procedencias is not None:
        settings.sisap_procedencias = procedencias
    if mercados is not None:
        settings.sisap_mercados = mercados
    if regiones is not None:
        settings.sisap_regiones = regiones
    if producto_codigo is not None:
        settings.sisap_producto_codigo = producto_codigo
    if producto_nombre is not None:
        settings.sisap_producto_nombre = producto_nombre
    if max_queries is not None:
        settings.sisap_max_queries = max_queries
    if max_productos is not None:
        settings.sisap_max_productos = max_productos
    if max_scopes is not None:
        settings.sisap_max_scopes = max_scopes
    if scope_workers is not None:
        settings.sisap_scope_max_workers = scope_workers
    if shard_workers is not None:
        settings.sisap_shard_max_workers = shard_workers
    if product_batch_size is not None:
        settings.sisap_product_batch_size = product_batch_size


def _run_command_with_overrides(
    runner,
    fecha_inicio: str | None,
    fecha_fin: str | None,
    modo_carga: str | None,
    max_queries: int | None,
    scope_workers: int | None,
    shard_workers: int | None,
    product_batch_size: int | None,
):
    _apply_runtime_overrides(
        fecha_inicio=fecha_inicio,
        fecha_fin=fecha_fin,
        modo_carga=modo_carga,
        modulos=None,
        procedencias=None,
        mercados=None,
        regiones=None,
        producto_codigo=None,
        producto_nombre=None,
        max_queries=max_queries,
        max_productos=None,
        max_scopes=None,
        scope_workers=scope_workers,
        shard_workers=shard_workers,
        product_batch_size=product_batch_size,
    )
    return runner()


def _echo_scope_summary(label: str, values: list[str]) -> None:
    if values:
        typer.echo(f'{label}: {", ".join(values)}')


def _echo_pipeline_summary(result: dict[str, object]) -> None:
    typer.echo(f"Modulos: {', '.join(result['modulos'])}")
    _echo_scope_summary('Procedencias', result['procedencias'])
    _echo_scope_summary('Mercados', result['mercados'])
    _echo_scope_summary('Regiones', result['regiones'])
    typer.echo(f"Bloques ejecutados: {len(result['resultados'])}")
    for item in result['resultados']:
        typer.echo(f'- {item}')
    typer.echo(
        'Control pendiente por sincronizar: '
        f"{result['control_status']['pending_records']}"
    )
    typer.echo(
        'Eventos de control pendientes por sincronizar: '
        f"{result['control_status']['pending_event_records']}"
    )
    typer.echo(f"Scope workers: {result['scope_workers']}")
    typer.echo(f"Shard workers: {result['shard_workers']}")
    typer.echo(f"Lote productos por shard: {result['product_batch_size']}")
    if result['control_status']['pending_records']:
        typer.echo(
            'Cache local de control: '
            f"{result['control_status']['pending_path']}"
        )
    if result['control_status']['pending_event_records']:
        typer.echo(
            'Cache local de eventos de control: '
            f"{result['control_status']['pending_events_path']}"
        )


@app.command('run-main')
def run_main_command(
    fecha_inicio: str | None = typer.Option(None, '--fecha-inicio'),
    fecha_fin: str | None = typer.Option(None, '--fecha-fin'),
    modo_carga: str | None = typer.Option(None, '--modo-carga'),
    modulos: str | None = typer.Option(None, '--modulos'),
    procedencias: str | None = typer.Option(None, '--procedencias'),
    mercados: str | None = typer.Option(None, '--mercados'),
    regiones: str | None = typer.Option(None, '--regiones'),
    producto_codigo: str | None = typer.Option(None, '--producto-codigo'),
    producto_nombre: str | None = typer.Option(None, '--producto-nombre'),
    max_queries: int | None = typer.Option(None, '--max-queries'),
    max_productos: int | None = typer.Option(None, '--max-productos'),
    max_scopes: int | None = typer.Option(None, '--max-scopes'),
    scope_workers: int | None = typer.Option(None, '--scope-workers'),
    shard_workers: int | None = typer.Option(None, '--shard-workers'),
    product_batch_size: int | None = typer.Option(None, '--product-batch-size'),
) -> None:
    _apply_runtime_overrides(
        fecha_inicio,
        fecha_fin,
        modo_carga,
        modulos,
        procedencias,
        mercados,
        regiones,
        producto_codigo,
        producto_nombre,
        max_queries,
        max_productos,
        max_scopes,
        scope_workers,
        shard_workers,
        product_batch_size,
    )
    result = _run_and_report(run_pipeline_main)
    _echo_pipeline_summary(result)


@app.command('inspect-home')
def inspect_home_command() -> None:
    data = inspect_home()
    typer.echo(f"Hidden inputs: {len(data['hidden_inputs'])}")
    typer.echo(f"PostID detectado: {data['post_id']}")
    typer.echo(f"Mercados detectados: {len(data['mercado_options'])}")
    typer.echo(f"Productos detectados (mercado por defecto de la home): {len(data['producto_options'])}")
    typer.echo(f"Procedencias detectadas: {len(data['procedencia_options'])}")
    typer.echo(f"Variables detectadas: {len(data['variable_options'])}")


@app.command('inspect-home-mercado')
def inspect_home_mercado_command(
    mercado_codigo: str = typer.Option(..., '--mercado-codigo', help='Codigo del select mercado (ej. 15011501)'),
) -> None:
    """Lista productos disponibles para un mercado (filtrarPorMercado), como en la corrida real."""
    data = inspect_home_mercado(mercado_codigo)
    typer.echo(f"Mercado: {data['mercado_codigo']}")
    typer.echo(f"Productos detectados: {len(data['producto_options'])}")
    for opt in data['producto_options'][:25]:
        typer.echo(f"  {opt['value']} — {opt['label']}")
    if len(data['producto_options']) > 25:
        typer.echo(f"  ... y {len(data['producto_options']) - 25} mas")


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
def run_volumen(
    fecha_inicio: str | None = typer.Option(None, '--fecha-inicio'),
    fecha_fin: str | None = typer.Option(None, '--fecha-fin'),
    modo_carga: str | None = typer.Option(None, '--modo-carga'),
    max_queries: int | None = typer.Option(None, '--max-queries'),
    scope_workers: int | None = typer.Option(None, '--scope-workers'),
    shard_workers: int | None = typer.Option(None, '--shard-workers'),
    product_batch_size: int | None = typer.Option(None, '--product-batch-size'),
) -> None:
    output = _run_and_report(
        lambda: _run_command_with_overrides(
            run_volumen_full,
            fecha_inicio,
            fecha_fin,
            modo_carga,
            max_queries,
            scope_workers,
            shard_workers,
            product_batch_size,
        )
    )
    typer.echo(f'Volumen consolidado guardado en: {output}')


@app.command('plan-precios')
def plan_precios() -> None:
    plan = build_precios_plan()
    typer.echo(f"Queries de precios: {len(plan)}")
    if plan:
        typer.echo(plan[0].model_dump_json(indent=2))


@app.command('run-precios')
def run_precios(
    fecha_inicio: str | None = typer.Option(None, '--fecha-inicio'),
    fecha_fin: str | None = typer.Option(None, '--fecha-fin'),
    modo_carga: str | None = typer.Option(None, '--modo-carga'),
    max_queries: int | None = typer.Option(None, '--max-queries'),
    scope_workers: int | None = typer.Option(None, '--scope-workers'),
    shard_workers: int | None = typer.Option(None, '--shard-workers'),
    product_batch_size: int | None = typer.Option(None, '--product-batch-size'),
) -> None:
    output = _run_and_report(
        lambda: _run_command_with_overrides(
            run_precios_full,
            fecha_inicio,
            fecha_fin,
            modo_carga,
            max_queries,
            scope_workers,
            shard_workers,
            product_batch_size,
        )
    )
    typer.echo(f'Precios consolidados guardados en: {output}')


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
def run_ciudades_mayoristas(
    fecha_inicio: str | None = typer.Option(None, '--fecha-inicio'),
    fecha_fin: str | None = typer.Option(None, '--fecha-fin'),
    modo_carga: str | None = typer.Option(None, '--modo-carga'),
    max_queries: int | None = typer.Option(None, '--max-queries'),
    scope_workers: int | None = typer.Option(None, '--scope-workers'),
    shard_workers: int | None = typer.Option(None, '--shard-workers'),
    product_batch_size: int | None = typer.Option(None, '--product-batch-size'),
) -> None:
    output = _run_and_report(
        lambda: _run_command_with_overrides(
            lambda: run_ciudades_full(ModuloSisap.CIUDADES_PRECIOS_MAYORISTAS),
            fecha_inicio,
            fecha_fin,
            modo_carga,
            max_queries,
            scope_workers,
            shard_workers,
            product_batch_size,
        )
    )
    typer.echo(f'Ciudades mayoristas consolidadas guardadas en: {output}')


@app.command('run-ciudades-minoristas')
def run_ciudades_minoristas(
    fecha_inicio: str | None = typer.Option(None, '--fecha-inicio'),
    fecha_fin: str | None = typer.Option(None, '--fecha-fin'),
    modo_carga: str | None = typer.Option(None, '--modo-carga'),
    max_queries: int | None = typer.Option(None, '--max-queries'),
    scope_workers: int | None = typer.Option(None, '--scope-workers'),
    shard_workers: int | None = typer.Option(None, '--shard-workers'),
    product_batch_size: int | None = typer.Option(None, '--product-batch-size'),
) -> None:
    output = _run_and_report(
        lambda: _run_command_with_overrides(
            lambda: run_ciudades_full(ModuloSisap.CIUDADES_PRECIOS_MINORISTAS),
            fecha_inicio,
            fecha_fin,
            modo_carga,
            max_queries,
            scope_workers,
            shard_workers,
            product_batch_size,
        )
    )
    typer.echo(f'Ciudades minoristas consolidadas guardadas en: {output}')


@app.command('sample-volumen')
def sample_volumen() -> None:
    output = run_volumen_sample()
    typer.echo(f'Sample volumen guardado en: {output}')


@app.command('sample-precios')
def sample_precios() -> None:
    output = run_precios_sample()
    typer.echo(f'Sample precios guardado en: {output}')


@app.command('sample-ciudades-mayoristas')
def sample_ciudades_mayoristas() -> None:
    output = run_ciudades_sample(ModuloSisap.CIUDADES_PRECIOS_MAYORISTAS)
    typer.echo(f'Sample ciudades mayoristas guardado en: {output}')


@app.command('sample-ciudades-minoristas')
def sample_ciudades_minoristas() -> None:
    output = run_ciudades_sample(ModuloSisap.CIUDADES_PRECIOS_MINORISTAS)
    typer.echo(f'Sample ciudades minoristas guardado en: {output}')


if __name__ == '__main__':
    app()
