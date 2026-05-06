from sisap_light.config import get_settings
from sisap_light.ingesta_datos.extractores.http_client import SisapHttpClient
from sisap_light.schemas import SisapQuery


class SisapCiudadesExtractor:
    def __init__(self):
        self.settings = get_settings()
        self.http = SisapHttpClient()

    def fetch_home(self) -> str:
        return self.http.get(self.settings.sisap_ciudades_url)

    def build_params(self, query: SisapQuery, variable: str) -> dict[str, str]:
        fecha = query.fecha_fin.strftime("%d/%m/%Y")
        desde = query.fecha_inicio.strftime("%d/%m/%Y")
        hasta = query.fecha_fin.strftime("%d/%m/%Y")
        return {
            "region": query.region_codigo or "",
            "variables[]": variable,
            "fecha": fecha,
            "desde": desde,
            "hasta": hasta,
            "productos[]": query.producto_codigo,
            "producto": query.producto_codigo,
            "periodicidad": "intervalo",
            "__ajax_carga_final": "consulta",
            "ajax": "true",
        }

    def fetch_report(self, query: SisapQuery, variable: str) -> str:
        params = self.build_params(query, variable)
        return self.http.get(self.settings.sisap_ciudades_report_url, params=params)

