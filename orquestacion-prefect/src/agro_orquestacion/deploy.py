from __future__ import annotations

from datetime import timedelta

from prefect.runner.storage import GitRepository

from agro_orquestacion.config import get_settings
from agro_orquestacion.flows import sisap_main_flow, sisap_master_flow, sunat_main_flow


def main() -> None:
    settings = get_settings()
    source = GitRepository(url="https://github.com/OazisLabs/agro-proyecto.git", branch="main")

    sisap_main_flow.from_source(
        source=source,
        entrypoint="orquestacion-prefect/src/agro_orquestacion/flows.py:sisap_main_flow",
    ).deploy(
        name="sisap-managed",
        work_pool_name=settings.prefect_work_pool_name,
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
            "env": settings.sisap_env(),
        },
        tags=["sisap", "managed", "ingesta"],
    )

    sisap_master_flow.from_source(
        source=source,
        entrypoint="orquestacion-prefect/src/agro_orquestacion/flows.py:sisap_master_flow",
    ).deploy(
        name="sisap-master-managed",
        work_pool_name=settings.prefect_work_pool_name,
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
            "env": settings.sisap_env(),
        },
        tags=["sisap", "managed", "master", "ingesta"],
    )

    sunat_main_flow.from_source(
        source=source,
        entrypoint="orquestacion-prefect/src/agro_orquestacion/flows.py:sunat_main_flow",
    ).deploy(
        name="sunat-managed",
        work_pool_name=settings.prefect_work_pool_name,
        interval=timedelta(hours=settings.prefect_sunat_interval_hours),
        parameters={
            "fecha_corte_inicio": settings.sunat_fecha_corte_inicio,
            "fecha_corte_fin": settings.sunat_fecha_corte_fin or None,
            "modo_carga": settings.sunat_modo_carga,
        },
        job_variables={
            "pip_packages": settings.prefect_requirements,
            "env": settings.sunat_env(),
        },
        tags=["sunat", "managed", "ingesta"],
    )


if __name__ == "__main__":
    main()
