from __future__ import annotations

from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    app_env: str = "local"
    prefect_execution_mode: str = "managed"
    prefect_work_pool_name: str = ""
    prefect_managed_work_pool_name: str = "agro-managed-pool"
    prefect_process_work_pool_name: str = "agro-process-pool"
    prefect_sunat_interval_hours: int = 12
    prefect_sisap_master_interval_hours: int = 4
    prefect_enable_schedules: bool = True
    prefect_enable_sisap: bool = True
    prefect_enable_sunat: bool = True
    prefect_enable_midagri_ce: bool = True
    prefect_enable_midagri_boletines: bool = True
    prefect_sisap_timeout_minutes: int = 240
    prefect_sunat_timeout_minutes: int = 180
    prefect_midagri_ce_interval_hours: int = 24
    prefect_midagri_ce_timeout_minutes: int = 120
    prefect_midagri_boletines_interval_hours: int = 12
    prefect_midagri_boletines_timeout_minutes: int = 120
    prefect_worker_process_limit: int = 1
    prefect_max_parallel_pipelines: int = 1
    prefect_sisap_deployment_concurrency_limit: int = 1
    prefect_sunat_deployment_concurrency_limit: int = 1
    prefect_midagri_ce_deployment_concurrency_limit: int = 1
    prefect_midagri_boletines_deployment_concurrency_limit: int = 1
    prefect_repo_url: str = "https://github.com/tu-organizacion/tu-repo.git"
    prefect_repo_branch: str = "main"
    prefect_github_access_token: str = ""
    prefect_github_username: str = ""
    prefect_github_secret_block_name: str = "github-repo-read-token"

    storage_backend: str = "minio"
    delta_enabled: bool = True
    minio_endpoint: str = "http://minio-api:9000"
    minio_access_key: str = ""
    minio_secret_key: str = ""
    minio_bucket: str = "agro-productos"
    minio_region: str = "us-east-1"

    sisap_minio_prefix: str = "Landing/sisap"
    sisap_control_dataset: str = "control/ingesta_control"
    sisap_control_events_dataset: str = "control/ingesta_control_eventos"
    sunat_minio_prefix: str = "Landing/sunat"
    midagri_ce_minio_prefix: str = "Landing/midagri_comercio_exterior"
    midagri_boletines_minio_prefix: str = "Landing/midagri_boletines"
    midagri_boletines_fecha_inicio: str = "2016-01-01"
    midagri_boletines_fecha_fin: str = ""
    midagri_boletines_modo_carga: str = "incremental"
    midagri_boletines_save_raw_binary: bool = True
    midagri_boletines_save_base_dataset: bool = True
    midagri_boletines_save_curated_dataset: bool = True

    sisap_fecha_inicio: str = "2016-01-01"
    sisap_fecha_fin: str = ""
    sisap_modo_carga: str = "incremental"
    sisap_modulos: str = "volumen,precios,regiones"
    sisap_procedencias: str = "all"
    sisap_regiones: str = "all"
    sisap_productos: str = "all"
    sisap_mercado_codigo: str = ""
    sisap_mercado_nombre: str = ""
    sisap_mercados: str = "all"
    sisap_producto_codigo: str = ""
    sisap_producto_nombre: str = ""
    sisap_use_control_table: bool = True
    sisap_estrategia_instanciacion: str = "por_modulo"
    sisap_max_instancias_paralelas: int = 1
    sisap_max_scopes: int | None = None
    sisap_max_productos: int | None = None
    sisap_max_queries: int | None = None
    sisap_scope_max_workers: int = 1
    sisap_shard_max_workers: int = 1
    sisap_product_batch_size: int = 1
    sisap_control_flush_every: int = 100
    sisap_output_flush_every: int = 25
    sisap_delta_finalize_every_items: int = 200
    sisap_use_local_delta_staging: bool = True
    sisap_defer_delta_finalize: bool = True

    sunat_fecha_corte_inicio: str = "2016-01-01"
    sunat_fecha_corte_fin: str = ""
    sunat_modo_carga: str = "incremental"
    midagri_ce_source_url: str = (
        "https://www.gob.pe/institucion/midagri/informes-publicaciones/"
        "2730438-compendio-anual-de-comercio-exterior-agrario"
    )
    midagri_ce_fecha_corte_inicio: str = "2016-01-01"
    midagri_ce_fecha_corte_fin: str = ""
    midagri_ce_modo_carga: str = "incremental"

    @property
    def repo_root(self) -> Path:
        return Path(__file__).resolve().parents[3]

    @property
    def orquestacion_root(self) -> Path:
        return self.repo_root / "orquestacion-prefect"

    @property
    def sisap_root(self) -> Path:
        return self.repo_root / "ingesta-datos" / "sisap"

    @property
    def sunat_root(self) -> Path:
        return self.repo_root / "ingesta-datos" / "sunat"

    @property
    def midagri_ce_root(self) -> Path:
        return self.repo_root / "ingesta-datos" / "midagri-comercio-exterior"

    @property
    def midagri_boletines_root(self) -> Path:
        return self.repo_root / "ingesta-datos" / "midagri-boletines"

    @property
    def midagri_boletines_requirements_path(self) -> Path:
        return self.midagri_boletines_root / "requirements.txt"

    @property
    def runtime_venvs_root(self) -> Path:
        return self.repo_root / ".runtime-venvs"

    @property
    def sisap_requirements_path(self) -> Path:
        return self.sisap_root / "requirements.txt"

    @property
    def sunat_requirements_path(self) -> Path:
        return self.sunat_root / "requirements.txt"

    @property
    def midagri_ce_requirements_path(self) -> Path:
        return self.midagri_ce_root / "requirements.txt"

    @property
    def prefect_requirements(self) -> list[str]:
        return [
            "prefect>=3,<4",
            "pydantic-settings>=2,<3",
            "python-dotenv==1.1.0",
        ]

    @property
    def prefect_target_work_pool_name(self) -> str:
        if self.prefect_work_pool_name:
            return self.prefect_work_pool_name
        if self.prefect_execution_mode.lower() == "process":
            return self.prefect_process_work_pool_name
        return self.prefect_managed_work_pool_name

    def sisap_env(self) -> dict[str, str]:
        env: dict[str, str] = {
            "PYTHONPATH": "src",
            "STORAGE_BACKEND": self.storage_backend,
            "DELTA_ENABLED": str(self.delta_enabled).lower(),
            "MINIO_ENDPOINT": self.minio_endpoint,
            "MINIO_ACCESS_KEY": self.minio_access_key,
            "MINIO_SECRET_KEY": self.minio_secret_key,
            "MINIO_BUCKET": self.minio_bucket,
            "MINIO_REGION": self.minio_region,
            "MINIO_PREFIX": self.sisap_minio_prefix,
            "SISAP_CONTROL_DATASET": self.sisap_control_dataset,
            "SISAP_CONTROL_EVENTS_DATASET": self.sisap_control_events_dataset,
            "SISAP_FECHA_INICIO": self.sisap_fecha_inicio,
            "SISAP_FECHA_FIN": self.sisap_fecha_fin,
            "SISAP_MODO_CARGA": self.sisap_modo_carga,
            "SISAP_MODULOS": self.sisap_modulos,
            "SISAP_PROCEDENCIAS": self.sisap_procedencias,
            "SISAP_REGIONES": self.sisap_regiones,
            "SISAP_MERCADO_CODIGO": self.sisap_mercado_codigo,
            "SISAP_MERCADO_NOMBRE": self.sisap_mercado_nombre,
            "SISAP_MERCADOS": self.sisap_mercados,
            "SISAP_PRODUCTO_CODIGO": self.sisap_producto_codigo,
            "SISAP_PRODUCTO_NOMBRE": self.sisap_producto_nombre,
            "SISAP_SCOPE_MAX_WORKERS": str(self.sisap_scope_max_workers),
            "SISAP_SHARD_MAX_WORKERS": str(self.sisap_shard_max_workers),
            "SISAP_PRODUCT_BATCH_SIZE": str(self.sisap_product_batch_size),
            "SISAP_USE_CONTROL_TABLE": str(self.sisap_use_control_table).lower(),
            "SISAP_CONTROL_FLUSH_EVERY": str(self.sisap_control_flush_every),
            "SISAP_OUTPUT_FLUSH_EVERY": str(self.sisap_output_flush_every),
            "SISAP_DELTA_FINALIZE_EVERY_ITEMS": str(self.sisap_delta_finalize_every_items),
            "SISAP_USE_LOCAL_DELTA_STAGING": str(self.sisap_use_local_delta_staging).lower(),
            "SISAP_DEFER_DELTA_FINALIZE": str(self.sisap_defer_delta_finalize).lower(),
        }
        if self.sisap_max_scopes is not None:
            env["SISAP_MAX_SCOPES"] = str(self.sisap_max_scopes)
        if self.sisap_max_productos is not None:
            env["SISAP_MAX_PRODUCTOS"] = str(self.sisap_max_productos)
        if self.sisap_max_queries is not None:
            env["SISAP_MAX_QUERIES"] = str(self.sisap_max_queries)
        return env

    def sunat_env(self) -> dict[str, str]:
        return {
            "PYTHONPATH": "src",
            "SUNAT_STORAGE_BACKEND": self.storage_backend,
            "SUNAT_DELTA_ENABLED": str(self.delta_enabled).lower(),
            "MINIO_ENDPOINT": self.minio_endpoint,
            "MINIO_ACCESS_KEY": self.minio_access_key,
            "MINIO_SECRET_KEY": self.minio_secret_key,
            "MINIO_BUCKET": self.minio_bucket,
            "MINIO_REGION": self.minio_region,
            "MINIO_PREFIX": self.sunat_minio_prefix,
            "SUNAT_FECHA_CORTE_INICIO": self.sunat_fecha_corte_inicio,
            "SUNAT_FECHA_CORTE_FIN": self.sunat_fecha_corte_fin,
            "SUNAT_MODO_CARGA": self.sunat_modo_carga,
        }

    def midagri_ce_env(self) -> dict[str, str]:
        return {
            "PYTHONPATH": "src",
            "MIDAGRI_CE_STORAGE_BACKEND": self.storage_backend,
            "MIDAGRI_CE_DELTA_ENABLED": str(self.delta_enabled).lower(),
            "MINIO_ENDPOINT": self.minio_endpoint,
            "MINIO_ACCESS_KEY": self.minio_access_key,
            "MINIO_SECRET_KEY": self.minio_secret_key,
            "MINIO_BUCKET": self.minio_bucket,
            "MINIO_REGION": self.minio_region,
            "MIDAGRI_CE_MINIO_PREFIX": self.midagri_ce_minio_prefix,
            "MIDAGRI_CE_SOURCE_PAGE_URL": self.midagri_ce_source_url,
            "MIDAGRI_CE_FECHA_CORTE_INICIO": self.midagri_ce_fecha_corte_inicio,
            "MIDAGRI_CE_FECHA_CORTE_FIN": self.midagri_ce_fecha_corte_fin,
            "MIDAGRI_CE_MODO_CARGA": self.midagri_ce_modo_carga,
        }

    def midagri_boletines_env(self) -> dict[str, str]:
        return {
            "PYTHONPATH": "src",
            "STORAGE_BACKEND": self.storage_backend,
            "DELTA_ENABLED": str(self.delta_enabled).lower(),
            "MINIO_ENDPOINT": self.minio_endpoint,
            "MINIO_ACCESS_KEY": self.minio_access_key,
            "MINIO_SECRET_KEY": self.minio_secret_key,
            "MINIO_BUCKET": self.minio_bucket,
            "MINIO_REGION": self.minio_region,
            "MINIO_PREFIX": self.midagri_boletines_minio_prefix,
            "MIDAGRI_BOLETINES_FECHA_INICIO": self.midagri_boletines_fecha_inicio,
            "MIDAGRI_BOLETINES_FECHA_FIN": self.midagri_boletines_fecha_fin,
            "MIDAGRI_BOLETINES_MODO_CARGA": self.midagri_boletines_modo_carga,
            "MIDAGRI_BOLETINES_SAVE_RAW_BINARY": str(self.midagri_boletines_save_raw_binary).lower(),
            "MIDAGRI_BOLETINES_SAVE_BASE_DATASET": str(self.midagri_boletines_save_base_dataset).lower(),
            "MIDAGRI_BOLETINES_SAVE_CURATED_DATASET": str(self.midagri_boletines_save_curated_dataset).lower(),
        }

def get_settings() -> Settings:
    return Settings()
