from __future__ import annotations

import asyncio
from datetime import timedelta

from prefect.runner import Runner

from agro_orquestacion.config import get_settings
from agro_orquestacion.flows import (
    sisap_ciudades_mayoristas_flow,
    sisap_ciudades_minoristas_flow,
    sisap_precios_flow,
    sisap_volumen_flow,
    sunat_main_flow,
)


async def _serve() -> None:
    settings = get_settings()
    if not settings.prefect_enable_schedules:
        raise ValueError(
            "El scheduling automatico esta deshabilitado. "
            "Activa PREFECT_ENABLE_SCHEDULES=true para usar serve."
        )
    runner = Runner(name="agro-local-runner", pause_on_shutdown=False)

    sisap_precios = sisap_precios_flow.to_deployment(
        name="sisap-precios-cada-4-horas",
        interval=timedelta(hours=settings.prefect_sisap_master_interval_hours),
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
        tags=["sisap", "precios", "ingesta"],
    )
    sisap_volumen = sisap_volumen_flow.to_deployment(
        name="sisap-volumen-cada-4-horas",
        interval=timedelta(hours=settings.prefect_sisap_master_interval_hours),
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
        tags=["sisap", "volumen", "ingesta"],
    )
    sisap_ciudades_mayoristas = sisap_ciudades_mayoristas_flow.to_deployment(
        name="sisap-ciudades-mayoristas-cada-4-horas",
        interval=timedelta(hours=settings.prefect_sisap_master_interval_hours),
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
        tags=["sisap", "ciudades", "mayoristas", "ingesta"],
    )
    sisap_ciudades_minoristas = sisap_ciudades_minoristas_flow.to_deployment(
        name="sisap-ciudades-minoristas-cada-4-horas",
        interval=timedelta(hours=settings.prefect_sisap_master_interval_hours),
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
        tags=["sisap", "ciudades", "minoristas", "ingesta"],
    )
    sunat_deployment = sunat_main_flow.to_deployment(
        name="sunat-cada-6-horas",
        interval=timedelta(hours=settings.prefect_sunat_interval_hours),
        tags=["sunat", "ingesta"],
    )

    await runner.add_deployment(sisap_precios)
    await runner.add_deployment(sisap_volumen)
    await runner.add_deployment(sisap_ciudades_mayoristas)
    await runner.add_deployment(sisap_ciudades_minoristas)
    await runner.add_deployment(sunat_deployment)
    await runner.start()


def main() -> None:
    asyncio.run(_serve())


if __name__ == "__main__":
    main()
