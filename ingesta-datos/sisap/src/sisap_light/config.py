from datetime import date
from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from sisap_light.ingesta_datos.catalogos.procedencias import PROCEDENCIAS_SISAP
from sisap_light.ingesta_datos.catalogos.mercados import MERCADOS_SISAP


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
    sisap_fecha_inicio: str = '2016-01-01'
    sisap_fecha_fin: str = ''
    sisap_modo_carga: str = 'incremental'
    sisap_incremental_overlap_dias: int = 0
    sisap_use_control_table: bool = True
    sisap_control_dataset: str = 'control/ingesta_control'
    sisap_control_events_dataset: str = 'control/ingesta_control_eventos'
    sisap_procedencia_codigo: str | None = None
    sisap_procedencia_nombre: str | None = None
    sisap_mercado_codigo: str | None = None
    sisap_mercado_nombre: str | None = None
    sisap_mercados: str = 'all'
    sisap_region_codigo: str | None = None
    sisap_region_nombre: str | None = None
    sisap_producto_codigo: str | None = None
    sisap_producto_nombre: str | None = None
    sisap_max_productos: int | None = None
    sisap_max_queries: int | None = None
    sisap_modulos: str = 'volumen,precios,regiones'
    sisap_procedencias: str = 'all'
    sisap_regiones: str = 'all'
    sisap_max_scopes: int | None = None
    sisap_pause_seconds: int = 30
    sisap_parallel_enabled: bool = True
    sisap_scope_max_workers: int = 2
    sisap_shard_max_workers: int = 4
    sisap_product_batch_size: int = 1

    storage_backend: str = 'local'
    delta_enabled: bool = True
    minio_endpoint: str = 'http://minio-api:9000'
    minio_access_key: str = ''
    minio_secret_key: str = ''
    minio_bucket: str = 'nombre-del-bucket'
    minio_region: str = 'us-east-1'
    minio_prefix: str = 'Landing/sisap'

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
    def control_dir(self) -> Path:
        return self.data_dir / 'control'

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

    @staticmethod
    def _all_scopes() -> list[str]:
        return [
            item['nombre']
            for item in PROCEDENCIAS_SISAP
            if item['nombre'].strip().lower() != 'desconocida'
        ]

    @staticmethod
    def _all_mercados() -> list[str]:
        return [item['nombre'] for item in MERCADOS_SISAP]

    @staticmethod
    def _is_all_keyword(values: list[str]) -> bool:
        normalized = {value.strip().lower() for value in values}
        return bool(normalized) and normalized <= {'all', '*', 'todas', 'todos'}

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
    def is_manual(self) -> bool:
        return self.sisap_modo_carga.strip().lower() == 'manual'

    @property
    def modulos_resueltos(self) -> list[str]:
        return self._split_csv(self.sisap_modulos)

    @property
    def procedencias_resueltas(self) -> list[str]:
        values = self._split_csv(self.sisap_procedencias)
        if not values or self._is_all_keyword(values):
            values = self._all_scopes()
        if self.sisap_max_scopes is not None and self.sisap_max_scopes > 0:
            return values[: self.sisap_max_scopes]
        return values

    @property
    def regiones_resueltas(self) -> list[str]:
        values = self._split_csv(self.sisap_regiones)
        if not values or self._is_all_keyword(values):
            values = self._all_scopes()
        if self.sisap_max_scopes is not None and self.sisap_max_scopes > 0:
            return values[: self.sisap_max_scopes]
        return values

    @property
    def mercados_resueltos(self) -> list[str]:
        values = self._split_csv(self.sisap_mercados)
        if not values or self._is_all_keyword(values):
            values = self._all_mercados()
        if self.sisap_max_scopes is not None and self.sisap_max_scopes > 0:
            return values[: self.sisap_max_scopes]
        return values

    @property
    def parallel_enabled(self) -> bool:
        return self.sisap_parallel_enabled

    @property
    def scope_max_workers(self) -> int:
        return max(int(self.sisap_scope_max_workers or 1), 1)

    @property
    def shard_max_workers(self) -> int:
        return max(int(self.sisap_shard_max_workers or 1), 1)

    @property
    def product_batch_size(self) -> int:
        return max(int(self.sisap_product_batch_size or 1), 1)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    settings = Settings()
    settings.raw_dir.mkdir(parents=True, exist_ok=True)
    settings.clean_dir.mkdir(parents=True, exist_ok=True)
    settings.raw_html_dir.mkdir(parents=True, exist_ok=True)
    settings.control_dir.mkdir(parents=True, exist_ok=True)
    if not settings.is_minio:
        settings.clean_delta_dir.mkdir(parents=True, exist_ok=True)
    return settings
