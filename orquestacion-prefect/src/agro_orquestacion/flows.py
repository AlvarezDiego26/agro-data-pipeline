from __future__ import annotations

import concurrent.futures
import shutil
import sys
from pathlib import Path

from prefect import flow, task
from prefect.logging import get_run_logger

from agro_orquestacion.config import get_settings
from agro_orquestacion.runner import ensure_runtime_python, run_command, run_python_module


def _merge_env(base_env: dict[str, str], overrides: dict[str, str | int | None]) -> dict[str, str]:
    merged = dict(base_env)
    for key, value in overrides.items():
        if value is not None:
            merged[key] = str(value)
    return merged


def _build_duckdb_init_sql(settings) -> Path:
    runtime_root = settings.duckdb_runtime_path
    source_path = runtime_root / "sql" / Path(settings.duckdb_build_init_sql_path).name
    output_path = runtime_root / "sql" / "51-build-api-cache-fast.runtime.sql"

    source_sql = source_path.read_text(encoding="utf-8")
    rendered_sql = source_sql.format(
        duckdb_minio_endpoint=settings.duckdb_minio_endpoint or settings.minio_endpoint,
        minio_access_key=settings.minio_access_key,
        minio_secret_key=settings.minio_secret_key,
        minio_region=settings.minio_region,
    )
    output_path.write_text(rendered_sql, encoding="utf-8")
    return output_path


@task(
    retries=5, 
    retry_delay_seconds=[60, 300, 600, 1200, 1800], 
    tags=["sisap-concurrency"]
)
def run_sisap_task(
    fecha_inicio: str | None = None,
    fecha_fin: str | None = None,
    modo_carga: str | None = None,
    modulos: str | None = None,
    procedencias: str | None = None,
    regiones: str | None = None,
    mercados: str | None = None,
    mercado_codigo: str | None = None,
    mercado_nombre: str | None = None,
    producto_codigo: str | None = None,
    producto_nombre: str | None = None,
    max_queries: int | None = None,
    scope_workers: int | None = None,
    shard_workers: int | None = None,
    product_batch_size: int | None = None,
    use_control_table: bool | None = None,
) -> None:
    settings = get_settings()
    python_executable = ensure_runtime_python(
        "sisap",
        settings.sisap_requirements_path,
        settings.sisap_root,
        settings.runtime_venvs_root,
    )
    environment = _merge_env(
        settings.sisap_env(),
        {
            "SISAP_FECHA_INICIO": fecha_inicio,
            "SISAP_FECHA_FIN": fecha_fin,
            "SISAP_MODO_CARGA": modo_carga,
            "SISAP_MODULOS": modulos,
            "SISAP_PROCEDENCIAS": procedencias,
            "SISAP_REGIONES": regiones,
            "SISAP_MERCADOS": mercados,
            "SISAP_MERCADO_CODIGO": mercado_codigo,
            "SISAP_MERCADO_NOMBRE": mercado_nombre,
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
        python_executable=python_executable,
    )


@task(
    retries=3, 
    retry_delay_seconds=[60, 600, 1800], 
    tags=["sunat-concurrency"]
)
def run_sunat_task(
    fecha_corte_inicio: str | None = None,
    fecha_corte_fin: str | None = None,
    modo_carga: str | None = None,
) -> None:
    settings = get_settings()
    python_executable = ensure_runtime_python(
        "sunat",
        settings.sunat_requirements_path,
        settings.sunat_root,
        settings.runtime_venvs_root,
    )
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
        python_executable=python_executable,
    )


@task(
    retries=3,
    retry_delay_seconds=[60, 300, 900],
    tags=["midagri-ce-concurrency"]
)
def run_midagri_ce_task(
    fecha_corte_inicio: str | None = None,
    fecha_corte_fin: str | None = None,
    modo_carga: str | None = None,
    rebuild_clean: bool = False,
) -> None:
    settings = get_settings()
    python_executable = ensure_runtime_python(
        "midagri-comercio-exterior",
        settings.midagri_ce_requirements_path,
        settings.midagri_ce_root,
        settings.runtime_venvs_root,
    )
    environment = _merge_env(
        settings.midagri_ce_env(),
        {
            "MIDAGRI_CE_FECHA_CORTE_INICIO": fecha_corte_inicio,
            "MIDAGRI_CE_FECHA_CORTE_FIN": fecha_corte_fin,
            "MIDAGRI_CE_MODO_CARGA": modo_carga,
        },
    )
    arguments = ["rebuild-clean"] if rebuild_clean else ["run-main"]
    run_python_module(
        "midagri_comercio_exterior.cli",
        arguments=arguments,
        working_dir=settings.midagri_ce_root,
        environment=environment,
        python_executable=python_executable,
    )


@task(
    retries=2,
    retry_delay_seconds=[60, 300],
    tags=["duckdb-refresh"]
)
def run_duckdb_refresh_task(force_rebuild: bool = True) -> None:
    settings = get_settings()
    logger = get_run_logger()

    runtime_root = settings.duckdb_runtime_path
    build_path = settings.duckdb_build_database_path
    snapshot_path = settings.duckdb_snapshot_database_path
    wal_path = Path(f"{build_path}.wal")

    runtime_root.mkdir(parents=True, exist_ok=True)
    build_path.parent.mkdir(parents=True, exist_ok=True)
    init_sql_path = _build_duckdb_init_sql(settings)

    if force_rebuild:
        for stale_path in (build_path, snapshot_path, wal_path):
            if stale_path.exists():
                stale_path.unlink()

    logger.info("[DuckDB] Construyendo cache en %s", build_path.name)
    run_command(
        [
            "docker",
            "exec",
            settings.duckdb_container_name,
            "/duckdb",
            f"/data/{settings.duckdb_build_database_name}",
            "-init",
            f"/sql/{init_sql_path.name}",
            "-c",
            "SHOW TABLES;",
        ],
        working_dir=runtime_root,
        environment=settings.duckdb_env(),
    )

    if not build_path.exists() or build_path.stat().st_size <= 0:
        raise RuntimeError(f"DuckDB no genero un build valido en {build_path}.")

    shutil.copy2(build_path, snapshot_path)
    logger.info("[DuckDB] Snapshot publicado en %s", snapshot_path)


@task(
    retries=2,
    retry_delay_seconds=[60, 300],
    tags=["serving-publish"]
)
def publish_serving_task() -> None:
    settings = get_settings()
    logger = get_run_logger()

    snapshot_path = settings.duckdb_snapshot_database_path
    host_root = settings.agro_analitica_host_path
    container_env_path = settings.agro_analitica_container_api_env_path
    host_env_path = settings.agro_analitica_host_api_env_path

    if not snapshot_path.exists() or snapshot_path.stat().st_size <= 0:
        raise RuntimeError(
            f"No existe un snapshot valido para publicar en serving: {snapshot_path}."
        )

    if not container_env_path.exists():
        raise RuntimeError(
            "No existe el archivo .env requerido para publicar serving dentro del worker: "
            f"{container_env_path}."
        )

    logger.info("[Serving] Publicando snapshot %s a PostgreSQL/Supabase", snapshot_path.name)
    run_command(
        [
            "docker",
            "run",
            "--rm",
            "--env-file",
            str(host_env_path),
            "-v",
            f"{host_root}:/workspace/agro-analitica",
            "-w",
            "/workspace/agro-analitica/api",
            settings.serving_publish_node_image,
            "bash",
            "-lc",
            "npm install && npm run publish:serving",
        ],
        working_dir=settings.orquestacion_root,
        environment={},
    )


@flow(name="duckdb-refresh-flow", log_prints=True)
def duckdb_refresh_flow(force_rebuild: bool = True) -> None:
    settings = get_settings()
    if not settings.prefect_enable_duckdb_refresh:
        return
    run_duckdb_refresh_task.with_options(
        timeout_seconds=60 * settings.prefect_duckdb_refresh_timeout_minutes
    )(force_rebuild=force_rebuild)


@flow(name="serving-sync-flow", log_prints=True)
def serving_sync_flow(force_rebuild: bool = True) -> None:
    settings = get_settings()
    if not settings.prefect_enable_serving_sync:
        return

    run_duckdb_refresh_task.with_options(
        timeout_seconds=60 * settings.prefect_duckdb_refresh_timeout_minutes
    )(force_rebuild=force_rebuild)

    publish_serving_task.with_options(
        timeout_seconds=60 * settings.prefect_serving_sync_timeout_minutes
    )()


def _run_sisap_module_task(
    *,
    modulo: str,
    fecha_inicio: str | None,
    fecha_fin: str | None,
    modo_carga: str | None,
    procedencias: str | None,
    regiones: str | None,
    mercado_codigo: str | None,
    mercado_nombre: str | None,
    producto_codigo: str | None,
    producto_nombre: str | None,
    max_queries: int | None,
    scope_workers: int | None,
    shard_workers: int | None,
    product_batch_size: int | None,
) -> dict[str, object]:
    settings = get_settings()
    run_sisap_task.with_options(name=f"sisap-{modulo}").submit(
        fecha_inicio=fecha_inicio,
        fecha_fin=fecha_fin,
        modo_carga=modo_carga,
        modulos=modulo,
        procedencias=procedencias,
        regiones=regiones,
        mercado_codigo=mercado_codigo,
        mercado_nombre=mercado_nombre,
        producto_codigo=producto_codigo,
        producto_nombre=producto_nombre,
        max_queries=max_queries,
        scope_workers=scope_workers or settings.sisap_scope_max_workers,
        shard_workers=shard_workers or settings.sisap_shard_max_workers,
        product_batch_size=product_batch_size or settings.sisap_product_batch_size,
        use_control_table=True,
    ).result()
    return {
        "modulo": modulo,
        "estado": "success",
    }


def _run_sisap_module_flow(
    *,
    modulo: str,
    fecha_inicio: str | None = None,
    fecha_fin: str | None = None,
    modo_carga: str | None = None,
    procedencias: str | None = None,
    regiones: str | None = None,
    mercado_codigo: str | None = None,
    mercado_nombre: str | None = None,
    producto_codigo: str | None = None,
    producto_nombre: str | None = None,
    max_queries: int | None = None,
    scope_workers: int | None = None,
    shard_workers: int | None = None,
    product_batch_size: int | None = None,
) -> dict[str, object]:
    return _run_sisap_module_task(
        modulo=modulo,
        fecha_inicio=fecha_inicio,
        fecha_fin=fecha_fin,
        modo_carga=modo_carga,
        procedencias=procedencias,
        regiones=regiones,
        mercado_codigo=mercado_codigo,
        mercado_nombre=mercado_nombre,
        producto_codigo=producto_codigo,
        producto_nombre=producto_nombre,
        max_queries=max_queries,
        scope_workers=scope_workers,
        shard_workers=shard_workers,
        product_batch_size=product_batch_size,
    )


@flow(name="sisap-precios-flow", log_prints=True)
def sisap_precios_flow(
    fecha_inicio: str | None = None,
    fecha_fin: str | None = None,
    modo_carga: str | None = None,
    procedencias: str | None = None,
    producto_codigo: str | None = None,
    producto_nombre: str | None = None,
    max_queries: int | None = None,
    scope_workers: int | None = None,
    shard_workers: int | None = None,
    product_batch_size: int | None = None,
) -> dict[str, object]:
    settings = get_settings()
    return _run_sisap_module_flow(
        modulo="precios",
        fecha_inicio=fecha_inicio,
        fecha_fin=fecha_fin,
        modo_carga=modo_carga,
        procedencias=procedencias or settings.sisap_procedencias,
        regiones=None,
        mercado_codigo=None,
        mercado_nombre=None,
        producto_codigo=producto_codigo or settings.sisap_producto_codigo or None,
        producto_nombre=producto_nombre or settings.sisap_producto_nombre or None,
        max_queries=max_queries,
        scope_workers=scope_workers,
        shard_workers=shard_workers,
        product_batch_size=product_batch_size,
    )


@flow(name="sisap-volumen-flow", log_prints=True)
def sisap_volumen_flow(
    fecha_inicio: str | None = None,
    fecha_fin: str | None = None,
    modo_carga: str | None = None,
    procedencias: str | None = None,
    mercado_codigo: str | None = None,
    mercado_nombre: str | None = None,
    producto_codigo: str | None = None,
    producto_nombre: str | None = None,
    max_queries: int | None = None,
    scope_workers: int | None = None,
    shard_workers: int | None = None,
    product_batch_size: int | None = None,
) -> dict[str, object]:
    settings = get_settings()
    return _run_sisap_module_flow(
        modulo="volumen",
        fecha_inicio=fecha_inicio,
        fecha_fin=fecha_fin,
        modo_carga=modo_carga,
        procedencias=procedencias or settings.sisap_procedencias,
        regiones=None,
        mercado_codigo=mercado_codigo or settings.sisap_mercado_codigo or None,
        mercado_nombre=mercado_nombre or settings.sisap_mercado_nombre or None,
        producto_codigo=producto_codigo or settings.sisap_producto_codigo or None,
        producto_nombre=producto_nombre or settings.sisap_producto_nombre or None,
        max_queries=max_queries,
        scope_workers=scope_workers,
        shard_workers=shard_workers,
        product_batch_size=product_batch_size,
    )


@flow(name="sisap-ciudades-mayoristas-flow", log_prints=True)
def sisap_ciudades_mayoristas_flow(
    fecha_inicio: str | None = None,
    fecha_fin: str | None = None,
    modo_carga: str | None = None,
    regiones: str | None = None,
    producto_codigo: str | None = None,
    producto_nombre: str | None = None,
    max_queries: int | None = None,
    scope_workers: int | None = None,
    shard_workers: int | None = None,
    product_batch_size: int | None = None,
) -> dict[str, object]:
    settings = get_settings()
    return _run_sisap_module_flow(
        modulo="ciudades-mayoristas",
        fecha_inicio=fecha_inicio,
        fecha_fin=fecha_fin,
        modo_carga=modo_carga,
        procedencias=None,
        regiones=regiones or settings.sisap_regiones,
        mercado_codigo=None,
        mercado_nombre=None,
        producto_codigo=producto_codigo or settings.sisap_producto_codigo or None,
        producto_nombre=producto_nombre or settings.sisap_producto_nombre or None,
        max_queries=max_queries,
        scope_workers=scope_workers,
        shard_workers=shard_workers,
        product_batch_size=product_batch_size,
    )


@flow(name="sisap-ciudades-minoristas-flow", log_prints=True)
def sisap_ciudades_minoristas_flow(
    fecha_inicio: str | None = None,
    fecha_fin: str | None = None,
    modo_carga: str | None = None,
    regiones: str | None = None,
    producto_codigo: str | None = None,
    producto_nombre: str | None = None,
    max_queries: int | None = None,
    scope_workers: int | None = None,
    shard_workers: int | None = None,
    product_batch_size: int | None = None,
) -> dict[str, object]:
    settings = get_settings()
    return _run_sisap_module_flow(
        modulo="ciudades-minoristas",
        fecha_inicio=fecha_inicio,
        fecha_fin=fecha_fin,
        modo_carga=modo_carga,
        procedencias=None,
        regiones=regiones or settings.sisap_regiones,
        mercado_codigo=None,
        mercado_nombre=None,
        producto_codigo=producto_codigo or settings.sisap_producto_codigo or None,
        producto_nombre=producto_nombre or settings.sisap_producto_nombre or None,
        max_queries=max_queries,
        scope_workers=scope_workers,
        shard_workers=shard_workers,
        product_batch_size=product_batch_size,
    )


@flow(name="sisap-regiones-flow", log_prints=True)
def sisap_regiones_flow(
    fecha_inicio: str | None = None,
    fecha_fin: str | None = None,
    modo_carga: str | None = None,
    regiones: str | None = None,
    producto_codigo: str | None = None,
    producto_nombre: str | None = None,
    max_queries: int | None = None,
    scope_workers: int | None = None,
    shard_workers: int | None = None,
    product_batch_size: int | None = None,
) -> dict[str, object]:
    settings = get_settings()
    return _run_sisap_module_flow(
        modulo="regiones",
        fecha_inicio=fecha_inicio,
        fecha_fin=fecha_fin,
        modo_carga=modo_carga,
        procedencias=None,
        regiones=regiones or settings.sisap_regiones,
        mercado_codigo=None,
        mercado_nombre=None,
        producto_codigo=producto_codigo or settings.sisap_producto_codigo or None,
        producto_nombre=producto_nombre or settings.sisap_producto_nombre or None,
        max_queries=max_queries,
        scope_workers=scope_workers,
        shard_workers=shard_workers,
        product_batch_size=product_batch_size,
    )


@flow(name="sisap-master-flow", log_prints=True)
def sisap_master_flow(
    fecha_inicio: str | None = None,
    fecha_fin: str | None = None,
    modo_carga: str | None = None,
    modulos: str | None = None,
    procedencias: str | None = None,
    regiones: str | None = None,
    max_queries: int | None = None,
    scope_workers: int | None = None,
    shard_workers: int | None = None,
    product_batch_size: int | None = None,
) -> dict[str, object]:
    settings = get_settings()
    requested_modules = [
        item.strip()
        for item in (modulos or settings.sisap_modulos).split(",")
        if item.strip()
    ]
    flow_map = {
        "precios": lambda: sisap_precios_flow.with_options(
            timeout_seconds=60 * settings.prefect_sisap_timeout_minutes
        )(
            fecha_inicio=fecha_inicio,
            fecha_fin=fecha_fin,
            modo_carga=modo_carga,
            procedencias=procedencias or settings.sisap_procedencias,
            producto_codigo=settings.sisap_producto_codigo or None,
            producto_nombre=settings.sisap_producto_nombre or None,
            max_queries=max_queries,
            scope_workers=scope_workers,
            shard_workers=shard_workers,
            product_batch_size=product_batch_size,
        ),
        "volumen": lambda: sisap_volumen_flow.with_options(
            timeout_seconds=60 * settings.prefect_sisap_timeout_minutes
        )(
            fecha_inicio=fecha_inicio,
            fecha_fin=fecha_fin,
            modo_carga=modo_carga,
            procedencias=procedencias or settings.sisap_procedencias,
            mercado_codigo=settings.sisap_mercado_codigo or None,
            mercado_nombre=settings.sisap_mercado_nombre or None,
            producto_codigo=settings.sisap_producto_codigo or None,
            producto_nombre=settings.sisap_producto_nombre or None,
            max_queries=max_queries,
            scope_workers=scope_workers,
            shard_workers=shard_workers,
            product_batch_size=product_batch_size,
        ),
        "ciudades-mayoristas": lambda: sisap_ciudades_mayoristas_flow.with_options(
            timeout_seconds=60 * settings.prefect_sisap_timeout_minutes
        )(
            fecha_inicio=fecha_inicio,
            fecha_fin=fecha_fin,
            modo_carga=modo_carga,
            regiones=regiones or settings.sisap_regiones,
            producto_codigo=settings.sisap_producto_codigo or None,
            producto_nombre=settings.sisap_producto_nombre or None,
            max_queries=max_queries,
            scope_workers=scope_workers,
            shard_workers=shard_workers,
            product_batch_size=product_batch_size,
        ),
        "ciudades-minoristas": lambda: sisap_ciudades_minoristas_flow.with_options(
            timeout_seconds=60 * settings.prefect_sisap_timeout_minutes
        )(
            fecha_inicio=fecha_inicio,
            fecha_fin=fecha_fin,
            modo_carga=modo_carga,
            regiones=regiones or settings.sisap_regiones,
            producto_codigo=settings.sisap_producto_codigo or None,
            producto_nombre=settings.sisap_producto_nombre or None,
            max_queries=max_queries,
            scope_workers=scope_workers,
            shard_workers=shard_workers,
            product_batch_size=product_batch_size,
        ),
        "regiones": lambda: sisap_regiones_flow.with_options(
            timeout_seconds=60 * settings.prefect_sisap_timeout_minutes
        )(
            fecha_inicio=fecha_inicio,
            fecha_fin=fecha_fin,
            modo_carga=modo_carga,
            regiones=regiones or settings.sisap_regiones,
            producto_codigo=settings.sisap_producto_codigo or None,
            producto_nombre=settings.sisap_producto_nombre or None,
            max_queries=max_queries,
            scope_workers=scope_workers,
            shard_workers=shard_workers,
            product_batch_size=product_batch_size,
        ),
    }

    resultados: list[dict[str, object]] = []
    errores: list[str] = []

    valid_modules = [modulo for modulo in requested_modules if modulo in flow_map]
    max_parallel_modules = max(
        min(settings.sisap_max_instancias_paralelas, len(valid_modules) or 1),
        1,
    )

    if max_parallel_modules == 1:
        for modulo in valid_modules:
            try:
                flow_map[modulo]()
                resultados.append({"instancia": modulo, "estado": "success"})
            except Exception as exc:
                resultados.append({"instancia": modulo, "estado": "error", "error": str(exc)})
                errores.append(f"{modulo}: {exc}")
    else:
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_parallel_modules) as executor:
            futures = {
                executor.submit(flow_map[modulo]): modulo
                for modulo in valid_modules
            }
            for future in concurrent.futures.as_completed(futures):
                modulo = futures[future]
                try:
                    future.result()
                    resultados.append({"instancia": modulo, "estado": "success"})
                except Exception as exc:
                    resultados.append({"instancia": modulo, "estado": "error", "error": str(exc)})
                    errores.append(f"{modulo}: {exc}")

    summary = {
        "estrategia_instanciacion": "flows_independientes",
        "instancias_planificadas": len(requested_modules),
        "instancias_ejecutadas": len(resultados),
        "resultados": resultados,
    }
    if errores:
        raise RuntimeError("Fallaron instancias del maestro SISAP: " + " | ".join(errores))
    return summary


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


@flow(name="midagri-ce-main-flow", log_prints=True)
def midagri_ce_main_flow(
    fecha_corte_inicio: str | None = None,
    fecha_corte_fin: str | None = None,
    modo_carga: str | None = None,
    rebuild_clean: bool = False,
) -> None:
    run_midagri_ce_task(
        fecha_corte_inicio=fecha_corte_inicio,
        fecha_corte_fin=fecha_corte_fin,
        modo_carga=modo_carga,
        rebuild_clean=rebuild_clean,
    )


@flow(name="agro-ingesta-flow", log_prints=True)
def agro_ingesta_flow(
    run_sisap: bool = True,
    run_sunat: bool = True,
    sisap_fecha_inicio: str | None = None,
    sisap_fecha_fin: str | None = None,
    sunat_fecha_corte_inicio: str | None = None,
    sunat_fecha_corte_fin: str | None = None,
    refresh_duckdb: bool | None = None,
) -> None:
    settings = get_settings()

    def _run_sisap():
        if run_sisap and settings.prefect_enable_sisap:
            sisap_master_flow.with_options(timeout_seconds=60 * settings.prefect_sisap_timeout_minutes)(
                fecha_inicio=sisap_fecha_inicio,
                fecha_fin=sisap_fecha_fin,
            )

    def _run_sunat():
        if run_sunat and settings.prefect_enable_sunat:
            sunat_main_flow.with_options(timeout_seconds=60 * settings.prefect_sunat_timeout_minutes)(
                fecha_corte_inicio=sunat_fecha_corte_inicio,
                fecha_corte_fin=sunat_fecha_corte_fin,
            )

    requested_pipelines = []
    if run_sisap and settings.prefect_enable_sisap:
        requested_pipelines.append(_run_sisap)
    if run_sunat and settings.prefect_enable_sunat:
        requested_pipelines.append(_run_sunat)

    max_parallel_pipelines = max(
        min(settings.prefect_max_parallel_pipelines, len(requested_pipelines) or 1),
        1,
    )

    if max_parallel_pipelines == 1:
        for pipeline_runner in requested_pipelines:
            pipeline_runner()
    else:
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_parallel_pipelines) as executor:
            futures = [executor.submit(pipeline_runner) for pipeline_runner in requested_pipelines]
            for future in concurrent.futures.as_completed(futures):
                future.result()

    should_refresh_duckdb = settings.prefect_enable_duckdb_refresh and (
        refresh_duckdb
        if refresh_duckdb is not None
        else settings.prefect_duckdb_refresh_after_ingesta
    )
    if should_refresh_duckdb:
        duckdb_refresh_flow.with_options(
            timeout_seconds=60 * settings.prefect_duckdb_refresh_timeout_minutes
        )(force_rebuild=True)


def _main() -> None:
    settings = get_settings()
    mode = sys.argv[1] if len(sys.argv) > 1 else "agro"
    if mode == "sisap":
        sisap_master_flow.with_options(timeout_seconds=60 * settings.prefect_sisap_timeout_minutes)()
    elif mode == "sisap-precios":
        sisap_precios_flow.with_options(timeout_seconds=60 * settings.prefect_sisap_timeout_minutes)()
    elif mode == "sisap-volumen":
        sisap_volumen_flow.with_options(timeout_seconds=60 * settings.prefect_sisap_timeout_minutes)()
    elif mode == "sisap-ciudades-mayoristas":
        sisap_ciudades_mayoristas_flow.with_options(timeout_seconds=60 * settings.prefect_sisap_timeout_minutes)()
    elif mode == "sisap-ciudades-minoristas":
        sisap_ciudades_minoristas_flow.with_options(timeout_seconds=60 * settings.prefect_sisap_timeout_minutes)()
    elif mode == "sisap-regiones":
        sisap_regiones_flow.with_options(timeout_seconds=60 * settings.prefect_sisap_timeout_minutes)()
    elif mode == "sunat":
        sunat_main_flow.with_options(timeout_seconds=60 * settings.prefect_sunat_timeout_minutes)()
    elif mode == "midagri-ce":
        midagri_ce_main_flow.with_options(timeout_seconds=60 * settings.prefect_midagri_ce_timeout_minutes)()
    elif mode == "duckdb-refresh":
        duckdb_refresh_flow.with_options(timeout_seconds=60 * settings.prefect_duckdb_refresh_timeout_minutes)()
    elif mode == "serving-sync":
        serving_sync_flow.with_options(timeout_seconds=60 * settings.prefect_serving_sync_timeout_minutes)()
    else:
        agro_ingesta_flow()


if __name__ == "__main__":
    _main()
