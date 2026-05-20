from datetime import date
from functools import lru_cache
from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file='.env',
        env_file_encoding='utf-8',
        extra='ignore',
    )

    app_env: str = 'local'
    midagri_boletines_fecha_inicio: str = '2016-01-01'
    midagri_boletines_fecha_fin: str = ''
    midagri_boletines_modo_carga: str = 'incremental'
    midagri_boletines_timeout_seconds: int = 30
    midagri_boletines_retry_intentos: int = 3
    midagri_boletines_retry_espera_segundos: int = 5
    midagri_boletines_save_raw_binary: bool = True
    midagri_boletines_save_base_dataset: bool = True
    midagri_boletines_save_curated_dataset: bool = True

    storage_backend: str = 'local'
    delta_enabled: bool = True
    minio_endpoint: str = 'http://minio-api:9000'
    minio_access_key: str = ''
    minio_secret_key: str = ''
    minio_bucket: str = 'agro-productos'
    minio_region: str = 'us-east-1'
    minio_prefix: str = 'Landing/midagri_boletines'

    @property
    def base_dir(self) -> Path:
        return Path(__file__).resolve().parents[2]

    @property
    def data_dir(self) -> Path:
        return self.base_dir / 'data'

    @property
    def raw_dir(self) -> Path:
        return self.data_dir / 'raw'

    @property
    def raw_pdf_dir(self) -> Path:
        return self.raw_dir / 'pdf'

    @property
    def clean_delta_dir(self) -> Path:
        return self.data_dir / 'Landing/midagri_boletines'

    @property
    def landing_dir(self) -> Path:
        return self.data_dir / 'Landing'

    @property
    def midagri_boletines_landing_dir(self) -> Path:
        return self.landing_dir / 'midagri_boletines'

    @property
    def is_minio(self) -> bool:
        return self.storage_backend.strip().lower() == 'minio'

    @property
    def delta_storage_options(self) -> dict[str, str] | None:
        if not self.is_minio:
            return None
        return {
            'AWS_ENDPOINT_URL': self.minio_endpoint,
            'AWS_ACCESS_KEY_ID': self.minio_access_key,
            'AWS_SECRET_ACCESS_KEY': self.minio_secret_key,
            'AWS_REGION': self.minio_region,
            'AWS_ALLOW_HTTP': 'true' if self.minio_endpoint.startswith('http://') else 'false',
            'AWS_S3_ALLOW_UNSAFE_RENAME': 'true',
        }

    def build_delta_uri(self, dataset_name: str) -> str:
        if self.is_minio:
            prefix = self.minio_prefix.strip('/')
            if prefix:
                return f's3://{self.minio_bucket}/{prefix}/{dataset_name}'
            return f's3://{self.minio_bucket}/{dataset_name}'
        return str(self.clean_delta_dir / dataset_name)

    @staticmethod
    def _resolve_date(raw_value: str | None, fallback: date) -> date:
        value = (raw_value or '').strip().lower()
        if value in {'', 'today', 'hoy', 'now', 'actual'}:
            return fallback
        return date.fromisoformat(raw_value)

    @property
    def fecha_inicio_resuelta(self) -> date:
        return self._resolve_date(self.midagri_boletines_fecha_inicio, date.today())

    @property
    def fecha_fin_resuelta(self) -> date:
        return self._resolve_date(self.midagri_boletines_fecha_fin, date.today())

    @property
    def is_incremental(self) -> bool:
        return self.midagri_boletines_modo_carga.strip().lower() == 'incremental'


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    settings = Settings()
    settings.raw_pdf_dir.mkdir(parents=True, exist_ok=True)
    if not settings.is_minio:
        settings.clean_delta_dir.mkdir(parents=True, exist_ok=True)
    return settings
