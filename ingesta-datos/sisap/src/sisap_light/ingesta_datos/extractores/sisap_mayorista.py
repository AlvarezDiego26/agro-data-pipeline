from datetime import timedelta

from sisap_light.config import get_settings
from sisap_light.ingesta_datos.extractores.http_client import SisapHttpClient
from sisap_light.procesamiento.parsers.html_forms import extract_hidden_inputs, extract_post_id
from sisap_light.schemas import SisapQuery


class SisapMayoristaExtractor:
    def __init__(self):
        self.settings = get_settings()
        self.base_url = self.settings.sisap_base_url
        self.report_url = self.settings.sisap_report_url
        self.client = SisapHttpClient()

    def fetch_home(self) -> str:
        return self.client.get(self.base_url)

    def fetch_productos_por_mercado_html(self, mercado_codigo: str) -> str:
        """HTML con los checkboxes de productos vigentes para el mercado (filtrarPorMercado)."""
        home_html = self.fetch_home()
        hidden = extract_hidden_inputs(home_html)
        post_id = extract_post_id(home_html) or hidden.get('postID', '')
        payload = {
            'mercado': mercado_codigo,
            '__ajax_carga_final': hidden.get('__ajax_carga_final', 'consulta'),
            'postID': post_id,
        }
        return self.client.post(self.settings.sisap_generos_url, data=payload)

    def build_payload(self, query: SisapQuery, home_html: str, variable: str = "volumen") -> dict[str, str]:
        hidden = extract_hidden_inputs(home_html)
        fecha_fin = query.fecha_fin.strftime("%d/%m/%Y")
        fecha_inicio = query.fecha_inicio.strftime("%d/%m/%Y")
        
        # El portal usa la semana del anio de la fecha fin
        _, week_num, _ = query.fecha_fin.isocalendar()
        semana = str(week_num)

        is_interval = query.fecha_inicio != query.fecha_fin
        periodicidad = "intervalo" if is_interval else "dia"

        payload = {
            **hidden,
            "mercado": query.mercado_codigo or "*",
            "variables[]": variable,
            "procedencias[]": query.procedencia_codigo or "",
            "fecha": fecha_fin,
            "desde": fecha_inicio,
            "hasta": fecha_fin,
            "anios[]": str(query.fecha_fin.year),
            "meses[]": query.fecha_fin.strftime("%m"),
            "semanas[]": semana,
            "productos[]": query.producto_codigo,
            "periodicidad": periodicidad,
            "__ajax_carga_final": "consulta",
            "ajax": "true",
        }
        return payload


    def fetch_report(self, query: SisapQuery, variable: str = "volumen") -> str:
        home_html = self.fetch_home()
        payload = self.build_payload(query=query, home_html=home_html, variable=variable)
        return self.client.post(self.report_url, data=payload)


