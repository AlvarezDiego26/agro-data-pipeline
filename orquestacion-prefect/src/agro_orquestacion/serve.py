from __future__ import annotations

from datetime import timedelta

from prefect import serve

from agro_orquestacion.config import get_settings
from agro_orquestacion.flows import sisap_master_flow, sunat_main_flow


def main() -> None:
    settings = get_settings()

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

    serve(
        sisap_deployment,
        sunat_deployment,
        pause_on_shutdown=False,
        print_starting_message=True,
    )


if __name__ == "__main__":
    main()
