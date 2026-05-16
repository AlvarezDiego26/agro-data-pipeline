from __future__ import annotations

from datetime import date
from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_env: str = "local"
    midagri_ce_source_page_url: str = (
        "https://www.gob.pe/institucion/midagri/informes-publicaciones/"
        "2730438-compendio-anual-de-comercio-exterior-agrario"
    )
    midagri_ce_download_timeout_seconds: int = 90
    midagri_ce_download_retry_intentos: int = 3
    midagri_ce_download_retry_espera_segundos: int = 5
    midagri_ce_fecha_corte_inicio: str = "2016-01-01"
    midagri_ce_fecha_corte_fin: str = ""
    midagri_ce_modo_carga: str = "incremental"
    midagri_ce_use_control_table: bool = True
    midagri_ce_save_remote_listing_dataset: bool = False
    midagri_ce_save_raw_binary: bool = False
    midagri_ce_save_base_dataset: bool = False
    midagri_ce_save_inventory_dataset: bool = True
    midagri_ce_control_dataset: str = "control_state.parquet"
    midagri_ce_control_events_dataset: str = "control_events_local.parquet"
    midagri_ce_inbox_dir: Path = Path("data/inbox/midagri_comercio_exterior")
    midagri_ce_processed_dir: Path = Path("data/processed/midagri_comercio_exterior")
    midagri_ce_error_dir: Path = Path("data/error/midagri_comercio_exterior")
    midagri_ce_storage_backend: str = "local"
    midagri_ce_delta_enabled: bool = True
    minio_endpoint: str = "http://minio-api:9000"
    minio_access_key: str = ""
    minio_secret_key: str = ""
    minio_bucket: str = "agro-productos"
    minio_region: str = "us-east-1"
    midagri_ce_minio_prefix: str = "Landing/midagri_comercio_exterior"

    @property
    def base_dir(self) -> Path:
        return Path(__file__).resolve().parents[2]

    @property
    def data_dir(self) -> Path:
        return self.base_dir / "data"

    @property
    def raw_dir(self) -> Path:
        return self.data_dir / "raw"

    @property
    def clean_dir(self) -> Path:
        return self.data_dir / "clean"

    @property
    def landing_dir(self) -> Path:
        return self.base_dir / "Landing"

    @property
    def midagri_landing_dir(self) -> Path:
        return self.landing_dir / "midagri_comercio_exterior"

    @property
    def midagri_control_dir(self) -> Path:
        return self.midagri_landing_dir / "control"

    @property
    def downloads_dir(self) -> Path:
        return self.data_dir / "downloads"

    @property
    def midagri_downloads_dir(self) -> Path:
        return self.downloads_dir / "midagri_comercio_exterior"

    @property
    def clean_delta_dir(self) -> Path:
        return self.data_dir / "clean_delta"

    @property
    def control_dir(self) -> Path:
        return self.midagri_control_dir

    @property
    def control_local_state_path(self) -> Path:
        return self.control_dir / "control_state.parquet"

    @property
    def control_pending_state_path(self) -> Path:
        return self.control_dir / "control_pending.parquet"

    @property
    def control_local_events_path(self) -> Path:
        return self.control_dir / "control_events_local.parquet"

    @property
    def control_pending_events_path(self) -> Path:
        return self.control_dir / "control_events_pending.parquet"

    @property
    def is_minio(self) -> bool:
        return self.midagri_ce_storage_backend.strip().lower() == "minio"

    @property
    def delta_storage_options(self) -> dict[str, str]:
        if not self.is_minio:
            return {"allow_unsafe_rename": "true"}
        return {
            "AWS_ENDPOINT_URL": self.minio_endpoint,
            "AWS_ACCESS_KEY_ID": self.minio_access_key,
            "AWS_SECRET_ACCESS_KEY": self.minio_secret_key,
            "AWS_REGION": self.minio_region,
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
            "AWS_ALLOW_HTTP": "true" if self.minio_endpoint.startswith("http://") else "false",
        }

    def build_delta_uri(self, dataset_name: str) -> str:
        if "control" in dataset_name or "pending" in dataset_name:
            if self.is_minio:
                prefix = self.midagri_ce_minio_prefix.strip("/")
                target = f"control/{Path(dataset_name).name}"
                return f"s3://{self.minio_bucket}/{prefix}/{target}"
            return str(self.control_dir / Path(dataset_name).name)

        if self.is_minio:
            prefix = self.midagri_ce_minio_prefix.strip("/")
            return f"s3://{self.minio_bucket}/{prefix}/{dataset_name}"
        return str(self.clean_delta_dir / dataset_name)

    @staticmethod
    def _resolve_date(raw_value: str | None, fallback: date) -> date:
        value = (raw_value or "").strip().lower()
        if value in {"", "today", "hoy", "now", "actual"}:
            return fallback
        return date.fromisoformat(raw_value)

    @property
    def fecha_inicio_resuelta(self) -> date:
        return self._resolve_date(self.midagri_ce_fecha_corte_inicio, date.today())

    @property
    def fecha_fin_resuelta(self) -> date:
        return self._resolve_date(self.midagri_ce_fecha_corte_fin, date.today())

    @property
    def is_incremental(self) -> bool:
        return self.midagri_ce_modo_carga.strip().lower() == "incremental"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    settings = Settings()
    settings.midagri_ce_inbox_dir.mkdir(parents=True, exist_ok=True)
    settings.midagri_ce_processed_dir.mkdir(parents=True, exist_ok=True)
    settings.midagri_ce_error_dir.mkdir(parents=True, exist_ok=True)
    settings.midagri_downloads_dir.mkdir(parents=True, exist_ok=True)
    settings.raw_dir.mkdir(parents=True, exist_ok=True)
    settings.clean_dir.mkdir(parents=True, exist_ok=True)
    settings.control_dir.mkdir(parents=True, exist_ok=True)
    if not settings.is_minio:
        settings.clean_delta_dir.mkdir(parents=True, exist_ok=True)
    return settings
