from datetime import date
from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file='.env',
        env_file_encoding='utf-8',
        extra='ignore',
    )

    app_env: str = 'local'
    sisap_base_url: str = 'http://sistemas.midagri.gob.pe/sisap/portal2/mayorista/'
    sisap_ciudades_url: str = 'http://sistemas.midagri.gob.pe/sisap/portal2/ciudades/'
    sisap_report_url: str = 'http://sistemas.midagri.gob.pe/sisap/portal2/mayorista/resumenes/filtrar'
    sisap_ciudades_report_url: str = 'http://sistemas.midagri.gob.pe/sisap/portal2/ciudades/resumenes/filtrar'
    sisap_generos_url: str = 'http://sistemas.midagri.gob.pe/sisap/portal2/mayorista/generos/filtrarPorMercado'
    sisap_variedades_url: str = 'http://sistemas.midagri.gob.pe/sisap/portal2/mayorista/variedades/filtrarPorGenero'
    sisap_volumen_procedencias_url: str = 'http://sistemas.midagri.gob.pe/sisap/portal2/mayorista/resumenes/filtrarVolumenPorProcedencias'
    sisap_timeout_seconds: int = 30
    sisap_retry_intentos: int = 3
    sisap_retry_espera_segundos: int = 3
    sisap_fecha_inicio: str = '2023-01-01'
    sisap_fecha_fin: str = ''
    sisap_modo_carga: str = 'backfill'
    sisap_incremental_overlap_dias: int = 0
    sisap_procedencia_codigo: str | None = None
    sisap_procedencia_nombre: str | None = None
    sisap_region_codigo: str | None = None
    sisap_region_nombre: str | None = None
    sisap_mercado_codigo: str = '15011501'
    sisap_mercado_nombre: str = 'Lima Metropolitana'
    sisap_producto_codigo: str | None = None
    sisap_producto_nombre: str | None = None
    sisap_max_queries: int | None = None
    sisap_modulos: str = 'volumen,precios,ciudades-mayoristas,ciudades-minoristas'
    sisap_procedencias: str = 'Arequipa'
    sisap_regiones: str = 'Arequipa'
    sisap_pause_seconds: int = 30

    storage_backend: str = 'local'
    delta_enabled: bool = True
    minio_endpoint: str = 'http://minio-api:9000'
    minio_access_key: str = ''
    minio_secret_key: str = ''
    minio_bucket: str = 'nombre-del-bucket'
    minio_region: str = 'us-east-1'
    minio_prefix: str = 'landing/sisap'

    sisap_output_dir: Path = Field(default=Path('data/raw'))

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
    def raw_html_dir(self) -> Path:
        return self.raw_dir / 'html'

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

    @staticmethod
    def _split_csv(raw_value: str | None) -> list[str]:
        if not raw_value:
            return []
        return [item.strip() for item in raw_value.split(',') if item.strip()]

    @property
    def fecha_inicio_resuelta(self) -> date:
        return self._resolve_date(self.sisap_fecha_inicio, date.today())

    @property
    def fecha_fin_resuelta(self) -> date:
        return self._resolve_date(self.sisap_fecha_fin, date.today())

    @property
    def is_incremental(self) -> bool:
        return self.sisap_modo_carga.strip().lower() == 'incremental'

    @property
    def modulos_resueltos(self) -> list[str]:
        return self._split_csv(self.sisap_modulos)

    @property
    def procedencias_resueltas(self) -> list[str]:
        return self._split_csv(self.sisap_procedencias)

    @property
    def regiones_resueltas(self) -> list[str]:
        return self._split_csv(self.sisap_regiones)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    settings = Settings()
    settings.raw_dir.mkdir(parents=True, exist_ok=True)
    settings.clean_dir.mkdir(parents=True, exist_ok=True)
    settings.raw_html_dir.mkdir(parents=True, exist_ok=True)
    if not settings.is_minio:
        settings.clean_delta_dir.mkdir(parents=True, exist_ok=True)
    return settings
