from __future__ import annotations

from datetime import timedelta

from prefect.deployments.runner import deploy as deploy_runner
from prefect.blocks.system import Secret
from prefect.runner.storage import GitRepository
from prefect.types.entrypoint import EntrypointType

from agro_orquestacion.config import get_settings
from agro_orquestacion.flows import agro_ingesta_flow, sisap_main_flow, sisap_master_flow, sunat_main_flow


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


def _deploy_managed(settings) -> None:
    source = _build_source(settings)
    runtime_env = _runtime_env(
        settings.sisap_env(),
        pythonpath=_managed_pythonpath(),
    )

    sisap_main_flow.from_source(
        source=source,
        entrypoint="orquestacion-prefect/src/agro_orquestacion/flows.py:sisap_main_flow",
    ).deploy(
        name="sisap-managed",
        work_pool_name=settings.prefect_target_work_pool_name,
        interval=timedelta(hours=settings.prefect_sisap_interval_hours),
        parameters={
            "fecha_inicio": settings.sisap_fecha_inicio,
            "fecha_fin": settings.sisap_fecha_fin or None,
            "modo_carga": settings.sisap_modo_carga,
            "modulos": settings.sisap_modulos,
            "procedencias": settings.sisap_procedencias,
            "regiones": settings.sisap_regiones,
            "max_queries": settings.sisap_max_queries,
            "scope_workers": settings.sisap_scope_max_workers,
            "shard_workers": settings.sisap_shard_max_workers,
            "product_batch_size": settings.sisap_product_batch_size,
        },
        job_variables={
            "pip_packages": settings.prefect_requirements,
            "env": runtime_env,
        },
        tags=["sisap", "managed", "ingesta"],
    )

    sisap_master_flow.from_source(
        source=source,
        entrypoint="orquestacion-prefect/src/agro_orquestacion/flows.py:sisap_master_flow",
    ).deploy(
        name="sisap-master-managed",
        work_pool_name=settings.prefect_target_work_pool_name,
        interval=timedelta(hours=settings.prefect_sisap_master_interval_hours),
        parameters={
            "fecha_inicio": settings.sisap_fecha_inicio,
            "fecha_fin": settings.sisap_fecha_fin or None,
            "modo_carga": settings.sisap_modo_carga,
            "modulos": settings.sisap_modulos,
            "procedencias": settings.sisap_procedencias,
            "regiones": settings.sisap_regiones,
            "productos": settings.sisap_productos,
            "estrategia_instanciacion": settings.sisap_estrategia_instanciacion,
            "max_instancias_paralelas": settings.sisap_max_instancias_paralelas,
            "max_queries": settings.sisap_max_queries,
            "shard_workers": settings.sisap_shard_max_workers,
            "product_batch_size": settings.sisap_product_batch_size,
        },
        job_variables={
            "pip_packages": settings.prefect_requirements,
            "env": runtime_env,
        },
        tags=["sisap", "managed", "master", "ingesta"],
    )

    sunat_main_flow.from_source(
        source=source,
        entrypoint="orquestacion-prefect/src/agro_orquestacion/flows.py:sunat_main_flow",
    ).deploy(
        name="sunat-managed",
        work_pool_name=settings.prefect_target_work_pool_name,
        interval=timedelta(hours=settings.prefect_sunat_interval_hours),
        parameters={
            "fecha_corte_inicio": settings.sunat_fecha_corte_inicio,
            "fecha_corte_fin": settings.sunat_fecha_corte_fin or None,
            "modo_carga": settings.sunat_modo_carga,
        },
        job_variables={
            "pip_packages": settings.prefect_requirements,
            "env": _runtime_env(
                settings.sunat_env(),
                pythonpath=_managed_pythonpath(),
            ),
        },
        tags=["sunat", "managed", "ingesta"],
    )

    agro_ingesta_flow.from_source(
        source=source,
        entrypoint="orquestacion-prefect/src/agro_orquestacion/flows.py:agro_ingesta_flow",
    ).deploy(
        name="agro-managed",
        work_pool_name=settings.prefect_target_work_pool_name,
        job_variables={
            "pip_packages": settings.prefect_requirements,
            "env": runtime_env,
        },
        tags=["agro", "managed", "ingesta"],
    )


def _deploy_process(settings) -> None:
    runtime_pythonpath = str(settings.orquestacion_root / "src")
    working_dir = str(settings.repo_root)

    sisap_main = sisap_main_flow.to_deployment(
        name="sisap-local",
        work_pool_name=settings.prefect_target_work_pool_name,
        interval=timedelta(hours=settings.prefect_sisap_interval_hours),
        parameters={
            "fecha_inicio": settings.sisap_fecha_inicio,
            "fecha_fin": settings.sisap_fecha_fin or None,
            "modo_carga": settings.sisap_modo_carga,
            "modulos": settings.sisap_modulos,
            "procedencias": settings.sisap_procedencias,
            "regiones": settings.sisap_regiones,
            "max_queries": settings.sisap_max_queries,
            "scope_workers": settings.sisap_scope_max_workers,
            "shard_workers": settings.sisap_shard_max_workers,
            "product_batch_size": settings.sisap_product_batch_size,
        },
        job_variables={
            "env": _runtime_env(settings.sisap_env(), pythonpath=runtime_pythonpath),
            "working_dir": working_dir,
        },
        tags=["sisap", "local", "process", "ingesta"],
        entrypoint_type=EntrypointType.MODULE_PATH,
    )

    sisap_master = sisap_master_flow.to_deployment(
        name="sisap-master-local",
        work_pool_name=settings.prefect_target_work_pool_name,
        interval=timedelta(hours=settings.prefect_sisap_master_interval_hours),
        parameters={
            "fecha_inicio": settings.sisap_fecha_inicio,
            "fecha_fin": settings.sisap_fecha_fin or None,
            "modo_carga": settings.sisap_modo_carga,
            "modulos": settings.sisap_modulos,
            "procedencias": settings.sisap_procedencias,
            "regiones": settings.sisap_regiones,
            "productos": settings.sisap_productos,
            "estrategia_instanciacion": settings.sisap_estrategia_instanciacion,
            "max_instancias_paralelas": settings.sisap_max_instancias_paralelas,
            "max_queries": settings.sisap_max_queries,
            "shard_workers": settings.sisap_shard_max_workers,
            "product_batch_size": settings.sisap_product_batch_size,
        },
        job_variables={
            "env": _runtime_env(settings.sisap_env(), pythonpath=runtime_pythonpath),
            "working_dir": working_dir,
        },
        tags=["sisap", "local", "process", "master", "ingesta"],
        entrypoint_type=EntrypointType.MODULE_PATH,
    )

    sunat_main = sunat_main_flow.to_deployment(
        name="sunat-local",
        work_pool_name=settings.prefect_target_work_pool_name,
        interval=timedelta(hours=settings.prefect_sunat_interval_hours),
        parameters={
            "fecha_corte_inicio": settings.sunat_fecha_corte_inicio,
            "fecha_corte_fin": settings.sunat_fecha_corte_fin or None,
            "modo_carga": settings.sunat_modo_carga,
        },
        job_variables={
            "env": _runtime_env(settings.sunat_env(), pythonpath=runtime_pythonpath),
            "working_dir": working_dir,
        },
        tags=["sunat", "local", "process", "ingesta"],
        entrypoint_type=EntrypointType.MODULE_PATH,
    )

    agro_ingesta = agro_ingesta_flow.to_deployment(
        name="agro-local",
        work_pool_name=settings.prefect_target_work_pool_name,
        job_variables={
            "env": _runtime_env(settings.sisap_env(), pythonpath=runtime_pythonpath),
            "working_dir": working_dir,
        },
        tags=["agro", "local", "process", "ingesta"],
        entrypoint_type=EntrypointType.MODULE_PATH,
    )

    deploy_runner(
        sisap_main,
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
