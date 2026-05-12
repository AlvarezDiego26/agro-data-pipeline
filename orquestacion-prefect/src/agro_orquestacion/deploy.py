from __future__ import annotations

from datetime import timedelta

from prefect.blocks.system import Secret
from prefect.deployments.runner import deploy as deploy_runner
from prefect.runner.storage import GitRepository
from prefect.types.entrypoint import EntrypointType

from agro_orquestacion.config import get_settings
from agro_orquestacion.flows import (
    agro_ingesta_flow,
    sisap_ciudades_mayoristas_flow,
    sisap_ciudades_minoristas_flow,
    sisap_master_flow,
    sisap_precios_flow,
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
        ]
    )
def _interval_kwargs(enabled: bool, hours: int) -> dict[str, timedelta]:
    if not enabled:
        return {}
    return {"interval": timedelta(hours=hours)}


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

    sisap_precios_flow.from_source(
        source=source,
        entrypoint=_entrypoint("sisap_precios_flow"),
    ).deploy(
        name="sisap-precios-managed",
        work_pool_name=settings.prefect_target_work_pool_name,
        **_interval_kwargs(settings.prefect_enable_schedules, settings.prefect_sisap_master_interval_hours),
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
        **_interval_kwargs(settings.prefect_enable_schedules, settings.prefect_sisap_master_interval_hours),
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

    sisap_ciudades_mayoristas_flow.from_source(
        source=source,
        entrypoint=_entrypoint("sisap_ciudades_mayoristas_flow"),
    ).deploy(
        name="sisap-ciudades-mayoristas-managed",
        work_pool_name=settings.prefect_target_work_pool_name,
        **_interval_kwargs(settings.prefect_enable_schedules, settings.prefect_sisap_master_interval_hours),
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
        tags=["sisap", "managed", "ciudades", "mayoristas", "ingesta"],
    )

    sisap_ciudades_minoristas_flow.from_source(
        source=source,
        entrypoint=_entrypoint("sisap_ciudades_minoristas_flow"),
    ).deploy(
        name="sisap-ciudades-minoristas-managed",
        work_pool_name=settings.prefect_target_work_pool_name,
        **_interval_kwargs(settings.prefect_enable_schedules, settings.prefect_sisap_master_interval_hours),
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
        tags=["sisap", "managed", "ciudades", "minoristas", "ingesta"],
    )

    sisap_master_flow.from_source(
        source=source,
        entrypoint=_entrypoint("sisap_master_flow"),
    ).deploy(
        name="sisap-master-managed",
        work_pool_name=settings.prefect_target_work_pool_name,
        job_variables={
            "pip_packages": settings.prefect_requirements,
            "env": sisap_runtime_env,
        },
        tags=["sisap", "managed", "master", "ingesta"],
        entrypoint_type=EntrypointType.MODULE_PATH,
    )

    sunat_main_flow.from_source(
        source=source,
        entrypoint=_entrypoint("sunat_main_flow"),
    ).deploy(
        name="sunat-managed",
        work_pool_name=settings.prefect_target_work_pool_name,
        **_interval_kwargs(settings.prefect_enable_schedules, settings.prefect_sunat_interval_hours),
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

    agro_ingesta_flow.from_source(
        source=source,
        entrypoint=_entrypoint("agro_ingesta_flow"),
    ).deploy(
        name="agro-managed",
        work_pool_name=settings.prefect_target_work_pool_name,
        job_variables={
            "pip_packages": settings.prefect_requirements,
            "env": sisap_runtime_env,
        },
        tags=["agro", "managed", "ingesta"],
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

    sisap_precios = sisap_precios_flow.to_deployment(
        name="sisap-precios-local",
        work_pool_name=settings.prefect_target_work_pool_name,
        **_interval_kwargs(settings.prefect_enable_schedules, settings.prefect_sisap_master_interval_hours),
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
        **_interval_kwargs(settings.prefect_enable_schedules, settings.prefect_sisap_master_interval_hours),
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

    sisap_ciudades_mayoristas = sisap_ciudades_mayoristas_flow.to_deployment(
        name="sisap-ciudades-mayoristas-local",
        work_pool_name=settings.prefect_target_work_pool_name,
        **_interval_kwargs(settings.prefect_enable_schedules, settings.prefect_sisap_master_interval_hours),
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
        tags=["sisap", "local", "ciudades", "mayoristas", "ingesta"],
        entrypoint_type=EntrypointType.MODULE_PATH,
    )

    sisap_ciudades_minoristas = sisap_ciudades_minoristas_flow.to_deployment(
        name="sisap-ciudades-minoristas-local",
        work_pool_name=settings.prefect_target_work_pool_name,
        **_interval_kwargs(settings.prefect_enable_schedules, settings.prefect_sisap_master_interval_hours),
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
        tags=["sisap", "local", "ciudades", "minoristas", "ingesta"],
        entrypoint_type=EntrypointType.MODULE_PATH,
    )

    sisap_master = sisap_master_flow.to_deployment(
        name="sisap-master-local",
        work_pool_name=settings.prefect_target_work_pool_name,
        job_variables=sisap_job_variables,
        tags=["sisap", "local", "master", "ingesta"],
        entrypoint_type=EntrypointType.MODULE_PATH,
    )

    sunat_main = sunat_main_flow.to_deployment(
        name="sunat-local",
        work_pool_name=settings.prefect_target_work_pool_name,
        **_interval_kwargs(settings.prefect_enable_schedules, settings.prefect_sunat_interval_hours),
        parameters={
            "fecha_corte_inicio": settings.sunat_fecha_corte_inicio,
            "fecha_corte_fin": settings.sunat_fecha_corte_fin or None,
            "modo_carga": settings.sunat_modo_carga,
        },
        job_variables=sunat_job_variables,
        tags=["sunat", "local", "process", "ingesta"],
        entrypoint_type=EntrypointType.MODULE_PATH,
    )

    agro_ingesta = agro_ingesta_flow.to_deployment(
        name="agro-local",
        work_pool_name=settings.prefect_target_work_pool_name,
        job_variables=sisap_job_variables,
        tags=["agro", "local", "process", "ingesta"],
        entrypoint_type=EntrypointType.MODULE_PATH,
    )

    deploy_runner(
        sisap_precios,
        sisap_volumen,
        sisap_ciudades_mayoristas,
        sisap_ciudades_minoristas,
        sisap_master,
        sunat_main,
        agro_ingesta,
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
