from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file='.env', env_file_encoding='utf-8', extra='ignore')

    app_env: str = 'local'
    sunat_fecha_corte_inicio: str = '2023-01-01'
    sunat_fecha_corte_fin: str = ''
    sunat_inbox_dir: Path = Path('data/inbox/sunat')
    sunat_processed_dir: Path = Path('data/processed/sunat')
    sunat_error_dir: Path = Path('data/error/sunat')
    sunat_storage_backend: str = 'local'
    sunat_delta_enabled: bool = True
    minio_endpoint: str = 'http://minio-api:9000'
    minio_access_key: str = ''
    minio_secret_key: str = ''
    minio_bucket: str = 'nombre-del-bucket'
    minio_region: str = 'us-east-1'
    minio_prefix: str = 'landing/sunat'

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
    def clean_dir(self) -> Path:
        return self.data_dir / 'clean'

    @property
    def clean_delta_dir(self) -> Path:
        return self.data_dir / 'clean_delta'

    @property
    def is_minio(self) -> bool:
        return self.sunat_storage_backend.strip().lower() == 'minio'

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


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    settings = Settings()
    settings.sunat_inbox_dir.mkdir(parents=True, exist_ok=True)
    settings.sunat_processed_dir.mkdir(parents=True, exist_ok=True)
    settings.sunat_error_dir.mkdir(parents=True, exist_ok=True)
    settings.raw_dir.mkdir(parents=True, exist_ok=True)
    settings.clean_dir.mkdir(parents=True, exist_ok=True)
    if not settings.is_minio:
        settings.clean_delta_dir.mkdir(parents=True, exist_ok=True)
    return settings
