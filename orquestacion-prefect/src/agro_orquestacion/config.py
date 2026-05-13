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
    prefect_sisap_timeout_minutes: int = 240
    prefect_sunat_timeout_minutes: int = 180
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
    sisap_max_instancias_paralelas: int = 8
    sisap_max_scopes: int | None = None
    sisap_max_productos: int | None = None
    sisap_max_queries: int | None = None
    sisap_scope_max_workers: int = 2
    sisap_shard_max_workers: int = 4
    sisap_product_batch_size: int = 1

    sunat_fecha_corte_inicio: str = "2016-01-01"
    sunat_fecha_corte_fin: str = ""
    sunat_modo_carga: str = "incremental"

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
    def runtime_venvs_root(self) -> Path:
        return self.repo_root / ".runtime-venvs"

    @property
    def sisap_requirements_path(self) -> Path:
        return self.sisap_root / "requirements.txt"

    @property
    def sunat_requirements_path(self) -> Path:
        return self.sunat_root / "requirements.txt"

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


def get_settings() -> Settings:
    return Settings()
