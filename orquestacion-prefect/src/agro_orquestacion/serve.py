from __future__ import annotations

import asyncio
from datetime import timedelta

from prefect.runner import Runner

from agro_orquestacion.config import get_settings
from agro_orquestacion.flows import sisap_master_flow, sunat_main_flow


async def _serve() -> None:
    settings = get_settings()
    runner = Runner(name="agro-local-runner", pause_on_shutdown=False)

    sisap_deployment = sisap_master_flow.to_deployment(
        name="sisap-master-cada-4-horas",
        interval=timedelta(hours=settings.prefect_sisap_master_interval_hours),
        tags=["sisap", "master", "ingesta"],
    )

    sunat_deployment = sunat_main_flow.to_deployment(
        name="sunat-cada-6-horas",
        interval=timedelta(hours=settings.prefect_sunat_interval_hours),
        tags=["sunat", "ingesta"],
    )

    await runner.add_deployment(sisap_deployment)
    await runner.add_deployment(sunat_deployment)
    await runner.start()


def main() -> None:
    asyncio.run(_serve())


if __name__ == "__main__":
    main()
