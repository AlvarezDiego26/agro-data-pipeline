from __future__ import annotations

import sys

from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner

from agro_orquestacion.config import get_settings
from agro_orquestacion.planner import SisapWorkUnit, build_sisap_work_units
from agro_orquestacion.runner import run_python_module


def _merge_env(base_env: dict[str, str], overrides: dict[str, str | int | None]) -> dict[str, str]:
    merged = dict(base_env)
    for key, value in overrides.items():
        if value is not None:
            merged[key] = str(value)
    return merged


@task(retries=2, retry_delay_seconds=60)
def run_sisap_task(
    fecha_inicio: str | None = None,
    fecha_fin: str | None = None,
    modo_carga: str | None = None,
    modulos: str | None = None,
    procedencias: str | None = None,
    regiones: str | None = None,
    producto_codigo: str | None = None,
    producto_nombre: str | None = None,
    max_queries: int | None = None,
    scope_workers: int | None = None,
    shard_workers: int | None = None,
    product_batch_size: int | None = None,
) -> None:
    settings = get_settings()
    environment = _merge_env(
        settings.sisap_env(),
        {
            "SISAP_FECHA_INICIO": fecha_inicio,
            "SISAP_FECHA_FIN": fecha_fin,
            "SISAP_MODO_CARGA": modo_carga,
            "SISAP_MODULOS": modulos,
            "SISAP_PROCEDENCIAS": procedencias,
            "SISAP_REGIONES": regiones,
            "SISAP_PRODUCTO_CODIGO": producto_codigo,
            "SISAP_PRODUCTO_NOMBRE": producto_nombre,
            "SISAP_MAX_QUERIES": max_queries,
            "SISAP_SCOPE_MAX_WORKERS": scope_workers,
            "SISAP_SHARD_MAX_WORKERS": shard_workers,
            "SISAP_PRODUCT_BATCH_SIZE": product_batch_size,
        },
    )
    run_python_module(
        "sisap_light.cli",
        arguments=["run-main"],
        working_dir=settings.sisap_root,
        environment=environment,
    )


@task(retries=2, retry_delay_seconds=60)
def run_sunat_task(
    fecha_corte_inicio: str | None = None,
    fecha_corte_fin: str | None = None,
    modo_carga: str | None = None,
) -> None:
    settings = get_settings()
    environment = _merge_env(
        settings.sunat_env(),
        {
            "SUNAT_FECHA_CORTE_INICIO": fecha_corte_inicio,
            "SUNAT_FECHA_CORTE_FIN": fecha_corte_fin,
            "SUNAT_MODO_CARGA": modo_carga,
        },
    )
    run_python_module(
        "sunat_file.cli",
        arguments=["run-main"],
        working_dir=settings.sunat_root,
        environment=environment,
    )


@flow(name="sisap-main-flow", log_prints=True)
def sisap_main_flow(
    fecha_inicio: str | None = None,
    fecha_fin: str | None = None,
    modo_carga: str | None = None,
    modulos: str | None = None,
    procedencias: str | None = None,
    regiones: str | None = None,
    producto_codigo: str | None = None,
    producto_nombre: str | None = None,
    max_queries: int | None = None,
    scope_workers: int | None = None,
    shard_workers: int | None = None,
    product_batch_size: int | None = None,
) -> None:
    run_sisap_task(
        fecha_inicio=fecha_inicio,
        fecha_fin=fecha_fin,
        modo_carga=modo_carga,
        modulos=modulos,
        procedencias=procedencias,
        regiones=regiones,
        producto_codigo=producto_codigo,
        producto_nombre=producto_nombre,
        max_queries=max_queries,
        scope_workers=scope_workers,
        shard_workers=shard_workers,
        product_batch_size=product_batch_size,
    )


@flow(name="sisap-master-flow", log_prints=True, task_runner=ConcurrentTaskRunner())
def sisap_master_flow(
    fecha_inicio: str | None = None,
    fecha_fin: str | None = None,
    modo_carga: str | None = None,
    modulos: str | None = None,
    procedencias: str | None = None,
    regiones: str | None = None,
    productos: str | None = None,
    estrategia_instanciacion: str | None = None,
    max_instancias_paralelas: int | None = None,
    max_queries: int | None = None,
    shard_workers: int | None = None,
    product_batch_size: int | None = None,
) -> dict[str, object]:
    settings = get_settings()
    work_units = build_sisap_work_units(
        estrategia=estrategia_instanciacion or settings.sisap_estrategia_instanciacion,
        modulos=modulos or settings.sisap_modulos,
        procedencias=procedencias or settings.sisap_procedencias,
        regiones=regiones or settings.sisap_regiones,
        productos=productos or settings.sisap_productos,
    )
    batch_size = max(int(max_instancias_paralelas or settings.sisap_max_instancias_paralelas), 1)
    resultados: list[dict[str, str]] = []

    for start in range(0, len(work_units), batch_size):
        batch = work_units[start : start + batch_size]
        futures: list[tuple[SisapWorkUnit, object]] = []
        for unit in batch:
            task_name = f"sisap-{unit.instancia_id}"
            future = run_sisap_task.with_options(name=task_name).submit(
                fecha_inicio=fecha_inicio,
                fecha_fin=fecha_fin,
                modo_carga=modo_carga,
                modulos=unit.modulo,
                procedencias=unit.scope_valor if unit.scope_tipo == "procedencias" else None,
                regiones=unit.scope_valor if unit.scope_tipo == "regiones" else None,
                producto_codigo=unit.producto_codigo,
                producto_nombre=unit.producto_nombre,
                max_queries=max_queries,
                scope_workers=1,
                shard_workers=shard_workers,
                product_batch_size=product_batch_size,
            )
            futures.append((unit, future))

        for unit, future in futures:
            try:
                future.result()
                resultados.append(
                    {
                        "instancia": unit.instancia_id,
                        "estado": "success",
                    }
                )
            except Exception as exc:
                resultados.append(
                    {
                        "instancia": unit.instancia_id,
                        "estado": "error",
                        "error": str(exc),
                    }
                )

    return {
        "estrategia_instanciacion": estrategia_instanciacion or settings.sisap_estrategia_instanciacion,
        "instancias_planificadas": len(work_units),
        "instancias_ejecutadas": len(resultados),
        "resultados": resultados,
    }


@flow(name="sunat-main-flow", log_prints=True)
def sunat_main_flow(
    fecha_corte_inicio: str | None = None,
    fecha_corte_fin: str | None = None,
    modo_carga: str | None = None,
) -> None:
    run_sunat_task(
        fecha_corte_inicio=fecha_corte_inicio,
        fecha_corte_fin=fecha_corte_fin,
        modo_carga=modo_carga,
    )


@flow(name="agro-ingesta-flow", log_prints=True)
def agro_ingesta_flow(
    run_sisap: bool = True,
    run_sunat: bool = True,
    sisap_fecha_inicio: str | None = None,
    sisap_fecha_fin: str | None = None,
    sunat_fecha_corte_inicio: str | None = None,
    sunat_fecha_corte_fin: str | None = None,
) -> None:
    settings = get_settings()

    if run_sisap and settings.prefect_enable_sisap:
        sisap_master_flow.with_options(timeout_seconds=60 * settings.prefect_sisap_timeout_minutes)(
            fecha_inicio=sisap_fecha_inicio,
            fecha_fin=sisap_fecha_fin,
        )

    if run_sunat and settings.prefect_enable_sunat:
        sunat_main_flow.with_options(timeout_seconds=60 * settings.prefect_sunat_timeout_minutes)(
            fecha_corte_inicio=sunat_fecha_corte_inicio,
            fecha_corte_fin=sunat_fecha_corte_fin,
        )


def _main() -> None:
    settings = get_settings()
    mode = sys.argv[1] if len(sys.argv) > 1 else "agro"
    if mode == "sisap":
        sisap_master_flow.with_options(timeout_seconds=60 * settings.prefect_sisap_timeout_minutes)()
    elif mode == "sisap-main":
        sisap_main_flow.with_options(timeout_seconds=60 * settings.prefect_sisap_timeout_minutes)()
    elif mode == "sunat":
        sunat_main_flow.with_options(timeout_seconds=60 * settings.prefect_sunat_timeout_minutes)()
    else:
        agro_ingesta_flow()


if __name__ == "__main__":
    _main()
