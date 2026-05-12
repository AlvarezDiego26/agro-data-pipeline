from datetime import date
from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file='.env', env_file_encoding='utf-8', extra='ignore')

    app_env: str = 'local'
    sunat_source_page_url: str = 'http://www.aduanet.gob.pe/aduanas/informae/presentacion_bases_web.htm'
    sunat_download_timeout_seconds: int = 60
    sunat_download_retry_intentos: int = 3
    sunat_download_retry_espera_segundos: int = 5
    sunat_fecha_corte_inicio: str = '2016-01-01'
    sunat_fecha_corte_fin: str = ''
    sunat_modo_carga: str = 'incremental'
    sunat_incremental_overlap_dias: int = 0
    sunat_use_control_table: bool = True
    sunat_control_dataset: str = 'control/control_state.parquet'
    sunat_control_events_dataset: str = 'control/control_events_local.parquet'
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
    minio_prefix: str = 'Landing/sunat'

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
    def landing_dir(self) -> Path:
        return self.base_dir / 'Landing'

    @property
    def sunat_landing_dir(self) -> Path:
        return self.landing_dir / 'sunat'

    @property
    def sunat_control_dir(self) -> Path:
        return self.sunat_landing_dir / 'control'

    @property
    def extraccion_dir(self) -> Path:
        return self.sunat_landing_dir / 'extraccion'

    @property
    def consolidacion_agricola_dir(self) -> Path:
        return self.sunat_landing_dir / 'consolidacion_agricola'

    @property
    def clean_delta_dir(self) -> Path:
        return self.data_dir / 'clean_delta'

    @property
    def downloads_dir(self) -> Path:
        return self.data_dir / 'downloads'

    @property
    def sunat_downloads_dir(self) -> Path:
        return self.downloads_dir / 'sunat'

    @property
    def control_dir(self) -> Path:
        return self.sunat_control_dir

    @property
    def control_local_state_path(self) -> Path:
        return self.control_dir / 'control_state.parquet'

    @property
    def control_pending_state_path(self) -> Path:
        return self.control_dir / 'control_pending.parquet'

    @property
    def control_local_events_path(self) -> Path:
        return self.control_dir / 'control_events_local.parquet'

    @property
    def control_pending_events_path(self) -> Path:
        return self.control_dir / 'control_events_pending.parquet'

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
        if dataset_name.startswith('control/'):
            # For control, return file URI
            if self.is_minio:
                prefix = self.minio_prefix.strip('/')
                if prefix:
                    return f's3://{self.minio_bucket}/{prefix}/{dataset_name}'
                return f's3://{self.minio_bucket}/{dataset_name}'
            return str(self.base_dir / dataset_name)
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
        return self._resolve_date(self.sunat_fecha_corte_inicio, date.today())

    @property
    def fecha_fin_resuelta(self) -> date:
        return self._resolve_date(self.sunat_fecha_corte_fin, date.today())

    @property
    def is_incremental(self) -> bool:
        return self.sunat_modo_carga.strip().lower() == 'incremental'

    @property
    def is_manual(self) -> bool:
        return self.sunat_modo_carga.strip().lower() == 'manual'


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    settings = Settings()
    settings.sunat_inbox_dir.mkdir(parents=True, exist_ok=True)
    settings.sunat_processed_dir.mkdir(parents=True, exist_ok=True)
    settings.sunat_error_dir.mkdir(parents=True, exist_ok=True)
    settings.sunat_downloads_dir.mkdir(parents=True, exist_ok=True)
    settings.raw_dir.mkdir(parents=True, exist_ok=True)
    settings.clean_dir.mkdir(parents=True, exist_ok=True)
    settings.control_dir.mkdir(parents=True, exist_ok=True)
    settings.extraccion_dir.mkdir(parents=True, exist_ok=True)
    settings.consolidacion_agricola_dir.mkdir(parents=True, exist_ok=True)
    if not settings.is_minio:
        settings.clean_delta_dir.mkdir(parents=True, exist_ok=True)
    return settings
