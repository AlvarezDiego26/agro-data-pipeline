from __future__ import annotations

import sys

from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner

from agro_orquestacion.config import get_settings
from agro_orquestacion.runner import install_requirements, run_python_module


def _merge_env(base_env: dict[str, str], overrides: dict[str, str | int | None]) -> dict[str, str]:
    merged = dict(base_env)
    for key, value in overrides.items():
        if value is not None:
            merged[key] = str(value)
    return merged


@task(retries=2, retry_delay_seconds=60)
def bootstrap_sisap_runtime() -> None:
    settings = get_settings()
    install_requirements(
        settings.sisap_root / "requirements.txt",
        working_dir=settings.sisap_root,
        environment=settings.sisap_env(),
    )


@task(retries=2, retry_delay_seconds=60)
def bootstrap_sunat_runtime() -> None:
    settings = get_settings()
    install_requirements(
        settings.sunat_root / "requirements.txt",
        working_dir=settings.sunat_root,
        environment=settings.sunat_env(),
    )


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
    use_control_table: bool | None = None,
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
            "SISAP_USE_CONTROL_TABLE": (
                "true" if use_control_table else "false"
            ) if use_control_table is not None else None,
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
    bootstrap_sisap_runtime()
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
        use_control_table=True,
    )


def _split_csv(raw_value: str | None) -> list[str]:
    if not raw_value:
        return []
    return [item.strip() for item in raw_value.split(",") if item.strip()]


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
    scope_workers: int | None = None,
    shard_workers: int | None = None,
    product_batch_size: int | None = None,
) -> dict[str, object]:
    settings = get_settings()
    bootstrap_sisap_runtime()
    module_list = _split_csv(modulos or settings.sisap_modulos)
    requested_procedencias = procedencias or settings.sisap_procedencias
    requested_regiones = regiones or settings.sisap_regiones
    batch_size = max(int(max_instancias_paralelas or settings.sisap_max_instancias_paralelas), 1)
    resultados: list[dict[str, str]] = []

    for start in range(0, len(module_list), batch_size):
        batch = module_list[start : start + batch_size]
        futures: list[tuple[str, object]] = []
        for modulo in batch:
            task_name = f"sisap-{modulo}"
            future = run_sisap_task.with_options(name=task_name).submit(
                fecha_inicio=fecha_inicio,
                fecha_fin=fecha_fin,
                modo_carga=modo_carga,
                modulos=modulo,
                procedencias=requested_procedencias if modulo in {"volumen", "precios"} else None,
                regiones=requested_regiones if modulo in {"ciudades-mayoristas", "ciudades-minoristas"} else None,
                max_queries=max_queries,
                scope_workers=scope_workers or settings.sisap_scope_max_workers,
                shard_workers=shard_workers or settings.sisap_shard_max_workers,
                product_batch_size=product_batch_size or settings.sisap_product_batch_size,
                use_control_table=False,
            )
            futures.append((modulo, future))

        for modulo, future in futures:
            try:
                future.result()
                resultados.append(
                    {
                        "instancia": modulo,
                        "estado": "success",
                    }
                )
            except Exception as exc:
                resultados.append(
                    {
                        "instancia": modulo,
                        "estado": "error",
                        "error": str(exc),
                    }
                )

    return {
        "estrategia_instanciacion": "por_modulo",
        "instancias_planificadas": len(module_list),
        "instancias_ejecutadas": len(resultados),
        "resultados": resultados,
    }


@flow(name="sunat-main-flow", log_prints=True)
def sunat_main_flow(
    fecha_corte_inicio: str | None = None,
    fecha_corte_fin: str | None = None,
    modo_carga: str | None = None,
) -> None:
    bootstrap_sunat_runtime()
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
