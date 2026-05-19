from __future__ import annotations

from datetime import timedelta

from prefect.client.schemas.objects import ConcurrencyLimitConfig, ConcurrencyLimitStrategy
from prefect.blocks.system import Secret
from prefect.deployments.runner import deploy as deploy_runner
from prefect.runner.storage import GitRepository
from prefect.types.entrypoint import EntrypointType

from agro_orquestacion.config import get_settings
from agro_orquestacion.flows import (
    duckdb_refresh_flow,
    midagri_ce_main_flow,
    serving_sync_flow,
    sisap_precios_flow,
    sisap_regiones_flow,
    sisap_volumen_flow,
    sunat_main_flow,
)


def _runtime_env(base_env: dict[str, str], *, pythonpath: str) -> dict[str, str]:
    env = dict(base_env)
    env["PYTHONPATH"] = pythonpath
    return env


def _managed_pythonpath() -> str:
    return ":".join(
        [
            "orquestacion-prefect/src",
            "ingesta-datos/sisap/src",
            "ingesta-datos/sunat/src",
            "ingesta-datos/midagri-comercio-exterior/src",
        ]
    )
from prefect.client.schemas.schedules import IntervalSchedule, CronSchedule

def _schedule_kwargs(enabled: bool, hours: int, is_sunat: bool = False, minute_offset: int = 0) -> dict[str, any]:
    if not enabled:
        return {"schedule": None}
    
    if is_sunat:
        # SUNAT corre a las 00:05 y 12:05 para no chocar con el inicio de hora
        return {"schedule": CronSchedule(cron=f"{5 + minute_offset} 0,12 * * *")}
    
    # SISAP corre cada 4 horas, pero escalonado por el minuto especificado
    # Ejemplo: minuto 0, 10, 20...
    return {"schedule": CronSchedule(cron=f"{minute_offset} */4 * * *")}


def _duckdb_schedule_kwargs(enabled: bool, hours: int) -> dict[str, any]:
    if not enabled:
        return {"schedule": None}
    return {"schedule": IntervalSchedule(interval=timedelta(hours=max(hours, 1)))}


def _build_source(settings) -> GitRepository:
    if settings.prefect_github_access_token:
        Secret(value=settings.prefect_github_access_token).save(
            settings.prefect_github_secret_block_name,
            overwrite=True,
        )
        credentials = {
            "access_token": Secret.load(settings.prefect_github_secret_block_name),
        }
        if settings.prefect_github_username:
            credentials["username"] = settings.prefect_github_username
        return GitRepository(
            url=settings.prefect_repo_url,
            branch=settings.prefect_repo_branch,
            credentials=credentials,
        )
    return GitRepository(url=settings.prefect_repo_url, branch=settings.prefect_repo_branch)


def _validate_runtime(settings) -> None:
    if settings.prefect_repo_url == "https://github.com/tu-organizacion/tu-repo.git":
        raise ValueError(
            "Configura PREFECT_REPO_URL con el repositorio real antes de publicar deployments managed."
        )
    if settings.storage_backend.lower() == "minio":
        missing = []
        if not settings.minio_access_key:
            missing.append("MINIO_ACCESS_KEY")
        if not settings.minio_secret_key:
            missing.append("MINIO_SECRET_KEY")
        if missing:
            raise ValueError(
                "Faltan credenciales de MinIO para publicar deployments managed: "
                + ", ".join(missing)
            )


def _entrypoint(flow_name: str) -> str:
    return f"orquestacion-prefect/src/agro_orquestacion/flows.py:{flow_name}"


def _deployment_concurrency(limit: int) -> ConcurrencyLimitConfig:
    return ConcurrencyLimitConfig(
        limit=max(limit, 1),
        collision_strategy=ConcurrencyLimitStrategy.CANCEL_NEW,
    )


def _deploy_managed(settings) -> None:
    source = _build_source(settings)
    sisap_runtime_env = _runtime_env(
        settings.sisap_env(),
        pythonpath=_managed_pythonpath(),
    )
    sunat_runtime_env = _runtime_env(
        settings.sunat_env(),
        pythonpath=_managed_pythonpath(),
    )
    midagri_runtime_env = _runtime_env(
        settings.midagri_ce_env(),
        pythonpath=_managed_pythonpath(),
    )
    duckdb_runtime_env = _runtime_env(
        settings.duckdb_env(),
        pythonpath=_managed_pythonpath(),
    )

    sisap_precios_flow.from_source(
        source=source,
        entrypoint=_entrypoint("sisap_precios_flow"),
    ).deploy(
        name="sisap-precios-managed",
        work_pool_name=settings.prefect_target_work_pool_name,
        concurrency_limit=_deployment_concurrency(settings.prefect_sisap_deployment_concurrency_limit),
        **_schedule_kwargs(
            settings.prefect_enable_schedules and settings.prefect_enable_sisap, 
            settings.prefect_sisap_master_interval_hours
        ),
        parameters={
            "fecha_inicio": settings.sisap_fecha_inicio,
            "fecha_fin": settings.sisap_fecha_fin or None,
            "modo_carga": settings.sisap_modo_carga,
            "procedencias": settings.sisap_procedencias,
            "producto_codigo": settings.sisap_producto_codigo or None,
            "producto_nombre": settings.sisap_producto_nombre or None,
            "max_queries": settings.sisap_max_queries,
            "scope_workers": settings.sisap_scope_max_workers,
            "shard_workers": settings.sisap_shard_max_workers,
            "product_batch_size": settings.sisap_product_batch_size,
        },
        job_variables={
            "pip_packages": settings.prefect_requirements,
            "env": sisap_runtime_env,
        },
        tags=["sisap", "managed", "precios", "ingesta"],
        entrypoint_type=EntrypointType.MODULE_PATH,
    )

    sisap_volumen_flow.from_source(
        source=source,
        entrypoint=_entrypoint("sisap_volumen_flow"),
    ).deploy(
        name="sisap-volumen-managed",
        work_pool_name=settings.prefect_target_work_pool_name,
        concurrency_limit=_deployment_concurrency(settings.prefect_sisap_deployment_concurrency_limit),
        **_schedule_kwargs(
            settings.prefect_enable_schedules and settings.prefect_enable_sisap, 
            settings.prefect_sisap_master_interval_hours, 
            minute_offset=10
        ),
        parameters={
            "fecha_inicio": settings.sisap_fecha_inicio,
            "fecha_fin": settings.sisap_fecha_fin or None,
            "modo_carga": settings.sisap_modo_carga,
            "procedencias": settings.sisap_procedencias,
            "mercado_codigo": settings.sisap_mercado_codigo or None,
            "mercado_nombre": settings.sisap_mercado_nombre or None,
            "producto_codigo": settings.sisap_producto_codigo or None,
            "producto_nombre": settings.sisap_producto_nombre or None,
            "max_queries": settings.sisap_max_queries,
            "scope_workers": settings.sisap_scope_max_workers,
            "shard_workers": settings.sisap_shard_max_workers,
            "product_batch_size": settings.sisap_product_batch_size,
        },
        job_variables={
            "pip_packages": settings.prefect_requirements,
            "env": sisap_runtime_env,
        },
        tags=["sisap", "managed", "volumen", "ingesta"],
    )

    sisap_regiones_flow.from_source(
        source=source,
        entrypoint=_entrypoint("sisap_regiones_flow"),
    ).deploy(
        name="sisap-regiones-managed",
        work_pool_name=settings.prefect_target_work_pool_name,
        concurrency_limit=_deployment_concurrency(settings.prefect_sisap_deployment_concurrency_limit),
        **_schedule_kwargs(
            settings.prefect_enable_schedules and settings.prefect_enable_sisap, 
            settings.prefect_sisap_master_interval_hours, 
            minute_offset=20
        ),
        parameters={
            "fecha_inicio": settings.sisap_fecha_inicio,
            "fecha_fin": settings.sisap_fecha_fin or None,
            "modo_carga": settings.sisap_modo_carga,
            "regiones": settings.sisap_regiones,
            "producto_codigo": settings.sisap_producto_codigo or None,
            "producto_nombre": settings.sisap_producto_nombre or None,
            "max_queries": settings.sisap_max_queries,
            "scope_workers": settings.sisap_scope_max_workers,
            "shard_workers": settings.sisap_shard_max_workers,
            "product_batch_size": settings.sisap_product_batch_size,
        },
        job_variables={
            "pip_packages": settings.prefect_requirements,
            "env": sisap_runtime_env,
        },
        tags=["sisap", "managed", "regiones", "ingesta"],
    )

    sunat_main_flow.from_source(
        source=source,
        entrypoint=_entrypoint("sunat_main_flow"),
    ).deploy(
        name="sunat-managed",
        work_pool_name=settings.prefect_target_work_pool_name,
        concurrency_limit=_deployment_concurrency(settings.prefect_sunat_deployment_concurrency_limit),
        **_schedule_kwargs(
            settings.prefect_enable_schedules and settings.prefect_enable_sunat, 
            settings.prefect_sunat_interval_hours, 
            is_sunat=True
        ),
        parameters={
            "fecha_corte_inicio": settings.sunat_fecha_corte_inicio,
            "fecha_corte_fin": settings.sunat_fecha_corte_fin or None,
            "modo_carga": settings.sunat_modo_carga,
        },
        job_variables={
            "pip_packages": settings.prefect_requirements,
            "env": sunat_runtime_env,
        },
        tags=["sunat", "managed", "ingesta"],
        entrypoint_type=EntrypointType.MODULE_PATH,
    )

    midagri_ce_main_flow.from_source(
        source=source,
        entrypoint=_entrypoint("midagri_ce_main_flow"),
    ).deploy(
        name="midagri-ce-managed",
        work_pool_name=settings.prefect_target_work_pool_name,
        concurrency_limit=_deployment_concurrency(settings.prefect_midagri_ce_deployment_concurrency_limit),
        **_schedule_kwargs(
            settings.prefect_enable_schedules and settings.prefect_enable_midagri_ce,
            settings.prefect_midagri_ce_interval_hours,
            minute_offset=30,
        ),
        parameters={
            "fecha_corte_inicio": settings.midagri_ce_fecha_corte_inicio,
            "fecha_corte_fin": settings.midagri_ce_fecha_corte_fin or None,
            "modo_carga": settings.midagri_ce_modo_carga,
            "rebuild_clean": False,
        },
        job_variables={
            "pip_packages": settings.prefect_requirements,
            "env": midagri_runtime_env,
        },
        tags=["midagri-ce", "managed", "ingesta"],
        entrypoint_type=EntrypointType.MODULE_PATH,
    )

    duckdb_refresh_flow.from_source(
        source=source,
        entrypoint=_entrypoint("duckdb_refresh_flow"),
    ).deploy(
        name="duckdb-refresh-managed",
        work_pool_name=settings.prefect_target_work_pool_name,
        concurrency_limit=_deployment_concurrency(settings.prefect_duckdb_deployment_concurrency_limit),
        **_duckdb_schedule_kwargs(
            settings.prefect_enable_duckdb_refresh_schedule and settings.prefect_enable_duckdb_refresh,
            settings.prefect_duckdb_refresh_interval_hours,
        ),
        parameters={
            "force_rebuild": True,
        },
        job_variables={
            "pip_packages": settings.prefect_requirements,
            "env": duckdb_runtime_env,
        },
        tags=["duckdb", "managed", "serving"],
        entrypoint_type=EntrypointType.MODULE_PATH,
    )

    serving_sync_flow.from_source(
        source=source,
        entrypoint=_entrypoint("serving_sync_flow"),
    ).deploy(
        name="serving-sync-managed",
        work_pool_name=settings.prefect_target_work_pool_name,
        concurrency_limit=_deployment_concurrency(settings.prefect_serving_sync_deployment_concurrency_limit),
        **_duckdb_schedule_kwargs(
            settings.prefect_enable_serving_sync_schedule and settings.prefect_enable_serving_sync,
            settings.prefect_serving_sync_interval_hours,
        ),
        parameters={
            "force_rebuild": True,
        },
        job_variables={
            "pip_packages": settings.prefect_requirements,
            "env": duckdb_runtime_env,
        },
        tags=["serving", "managed", "sync"],
        entrypoint_type=EntrypointType.MODULE_PATH,
    )

def _deploy_process(settings) -> None:
    runtime_pythonpath = str(settings.orquestacion_root / "src")
    working_dir = str(settings.repo_root)
    sisap_job_variables = {
        "env": _runtime_env(settings.sisap_env(), pythonpath=runtime_pythonpath),
        "working_dir": working_dir,
    }
    sunat_job_variables = {
        "env": _runtime_env(settings.sunat_env(), pythonpath=runtime_pythonpath),
        "working_dir": working_dir,
    }
    midagri_job_variables = {
        "env": _runtime_env(settings.midagri_ce_env(), pythonpath=runtime_pythonpath),
        "working_dir": working_dir,
    }
    duckdb_job_variables = {
        "env": _runtime_env(settings.duckdb_env(), pythonpath=runtime_pythonpath),
        "working_dir": working_dir,
    }

    sisap_precios = sisap_precios_flow.to_deployment(
        name="sisap-precios-local",
        work_pool_name=settings.prefect_target_work_pool_name,
        concurrency_limit=_deployment_concurrency(settings.prefect_sisap_deployment_concurrency_limit),
        **_schedule_kwargs(
            settings.prefect_enable_schedules and settings.prefect_enable_sisap, 
            settings.prefect_sisap_master_interval_hours
        ),
        parameters={
            "fecha_inicio": settings.sisap_fecha_inicio,
            "fecha_fin": settings.sisap_fecha_fin or None,
            "modo_carga": settings.sisap_modo_carga,
            "procedencias": settings.sisap_procedencias,
            "producto_codigo": settings.sisap_producto_codigo or None,
            "producto_nombre": settings.sisap_producto_nombre or None,
            "max_queries": settings.sisap_max_queries,
            "scope_workers": settings.sisap_scope_max_workers,
            "shard_workers": settings.sisap_shard_max_workers,
            "product_batch_size": settings.sisap_product_batch_size,
        },
        job_variables=sisap_job_variables,
        tags=["sisap", "local", "precios", "ingesta"],
        entrypoint_type=EntrypointType.MODULE_PATH,
    )

    sisap_volumen = sisap_volumen_flow.to_deployment(
        name="sisap-volumen-local",
        work_pool_name=settings.prefect_target_work_pool_name,
        concurrency_limit=_deployment_concurrency(settings.prefect_sisap_deployment_concurrency_limit),
        **_schedule_kwargs(
            settings.prefect_enable_schedules and settings.prefect_enable_sisap, 
            settings.prefect_sisap_master_interval_hours, 
            minute_offset=10
        ),
        parameters={
            "fecha_inicio": settings.sisap_fecha_inicio,
            "fecha_fin": settings.sisap_fecha_fin or None,
            "modo_carga": settings.sisap_modo_carga,
            "procedencias": settings.sisap_procedencias,
            "mercado_codigo": settings.sisap_mercado_codigo or None,
            "mercado_nombre": settings.sisap_mercado_nombre or None,
            "producto_codigo": settings.sisap_producto_codigo or None,
            "producto_nombre": settings.sisap_producto_nombre or None,
            "max_queries": settings.sisap_max_queries,
            "scope_workers": settings.sisap_scope_max_workers,
            "shard_workers": settings.sisap_shard_max_workers,
            "product_batch_size": settings.sisap_product_batch_size,
        },
        job_variables=sisap_job_variables,
        tags=["sisap", "local", "volumen", "ingesta"],
        entrypoint_type=EntrypointType.MODULE_PATH,
    )

    sisap_regiones = sisap_regiones_flow.to_deployment(
        name="sisap-regiones-local",
        work_pool_name=settings.prefect_target_work_pool_name,
        concurrency_limit=_deployment_concurrency(settings.prefect_sisap_deployment_concurrency_limit),
        **_schedule_kwargs(
            settings.prefect_enable_schedules and settings.prefect_enable_sisap, 
            settings.prefect_sisap_master_interval_hours, 
            minute_offset=20
        ),
        parameters={
            "fecha_inicio": settings.sisap_fecha_inicio,
            "fecha_fin": settings.sisap_fecha_fin or None,
            "modo_carga": settings.sisap_modo_carga,
            "regiones": settings.sisap_regiones,
            "producto_codigo": settings.sisap_producto_codigo or None,
            "producto_nombre": settings.sisap_producto_nombre or None,
            "max_queries": settings.sisap_max_queries,
            "scope_workers": settings.sisap_scope_max_workers,
            "shard_workers": settings.sisap_shard_max_workers,
            "product_batch_size": settings.sisap_product_batch_size,
        },
        job_variables=sisap_job_variables,
        tags=["sisap", "local", "regiones", "ingesta"],
        entrypoint_type=EntrypointType.MODULE_PATH,
    )

    sunat_main = sunat_main_flow.to_deployment(
        name="sunat-local",
        work_pool_name=settings.prefect_target_work_pool_name,
        concurrency_limit=_deployment_concurrency(settings.prefect_sunat_deployment_concurrency_limit),
        **_schedule_kwargs(
            settings.prefect_enable_schedules and settings.prefect_enable_sunat, 
            settings.prefect_sunat_interval_hours, 
            is_sunat=True
        ),
        parameters={
            "fecha_corte_inicio": settings.sunat_fecha_corte_inicio,
            "fecha_corte_fin": settings.sunat_fecha_corte_fin or None,
            "modo_carga": settings.sunat_modo_carga,
        },
        job_variables=sunat_job_variables,
        tags=["sunat", "local", "process", "ingesta"],
        entrypoint_type=EntrypointType.MODULE_PATH,
    )

    midagri_ce_main = midagri_ce_main_flow.to_deployment(
        name="midagri-ce-local",
        work_pool_name=settings.prefect_target_work_pool_name,
        concurrency_limit=_deployment_concurrency(settings.prefect_midagri_ce_deployment_concurrency_limit),
        **_schedule_kwargs(
            settings.prefect_enable_schedules and settings.prefect_enable_midagri_ce,
            settings.prefect_midagri_ce_interval_hours,
            minute_offset=30,
        ),
        parameters={
            "fecha_corte_inicio": settings.midagri_ce_fecha_corte_inicio,
            "fecha_corte_fin": settings.midagri_ce_fecha_corte_fin or None,
            "modo_carga": settings.midagri_ce_modo_carga,
            "rebuild_clean": False,
        },
        job_variables=midagri_job_variables,
        tags=["midagri-ce", "local", "process", "ingesta"],
        entrypoint_type=EntrypointType.MODULE_PATH,
    )

    duckdb_refresh = duckdb_refresh_flow.to_deployment(
        name="duckdb-refresh-local",
        work_pool_name=settings.prefect_target_work_pool_name,
        concurrency_limit=_deployment_concurrency(settings.prefect_duckdb_deployment_concurrency_limit),
        **_duckdb_schedule_kwargs(
            settings.prefect_enable_duckdb_refresh_schedule and settings.prefect_enable_duckdb_refresh,
            settings.prefect_duckdb_refresh_interval_hours,
        ),
        parameters={
            "force_rebuild": True,
        },
        job_variables=duckdb_job_variables,
        tags=["duckdb", "local", "process", "serving"],
        entrypoint_type=EntrypointType.MODULE_PATH,
    )

    serving_sync = serving_sync_flow.to_deployment(
        name="serving-sync-local",
        work_pool_name=settings.prefect_target_work_pool_name,
        concurrency_limit=_deployment_concurrency(settings.prefect_serving_sync_deployment_concurrency_limit),
        **_duckdb_schedule_kwargs(
            settings.prefect_enable_serving_sync_schedule and settings.prefect_enable_serving_sync,
            settings.prefect_serving_sync_interval_hours,
        ),
        parameters={
            "force_rebuild": True,
        },
        job_variables=duckdb_job_variables,
        tags=["serving", "local", "process", "sync"],
        entrypoint_type=EntrypointType.MODULE_PATH,
    )

    deploy_runner(
        sisap_precios,
        sisap_volumen,
        sisap_regiones,
        sunat_main,
        midagri_ce_main,
        duckdb_refresh,
        serving_sync,
        work_pool_name=settings.prefect_target_work_pool_name,
        build=False,
        push=False,
    )


def main() -> None:
    settings = get_settings()
    _validate_runtime(settings)
    mode = settings.prefect_execution_mode.lower()
    if mode == "process":
        _deploy_process(settings)
        return
    _deploy_managed(settings)


if __name__ == "__main__":
    main()
