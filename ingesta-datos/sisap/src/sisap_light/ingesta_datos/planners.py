from datetime import date, timedelta

from sisap_light.ingesta_datos.catalogos.productos import PRODUCTOS_AGRICOLAS_PRIORITARIOS
from sisap_light.ingesta_datos.catalogos.procedencias import PROCEDENCIAS_SISAP
from sisap_light.schemas import ModuloSisap, SisapQuery


def _find_nombre(items: list[dict], codigo: str) -> str | None:
    for item in items:
        if item["codigo"] == codigo:
            return item["nombre"]
    return None


def _month_windows(fecha_inicio: date, fecha_fin: date) -> list[tuple[date, date]]:
    windows: list[tuple[date, date]] = []
    current = fecha_inicio.replace(day=1)

    while current <= fecha_fin:
        next_month = (current.replace(day=28) + timedelta(days=4)).replace(day=1)
        month_start = current if current >= fecha_inicio else fecha_inicio
        month_end = min(next_month - timedelta(days=1), fecha_fin)
        windows.append((month_start, month_end))
        current = next_month

    return windows


def build_mayorista_queries(
    modulo: ModuloSisap,
    fecha_inicio: date,
    fecha_fin: date,
    procedencia_codigo: str,
    mercado_codigo: str,
    mercado_nombre: str,
    productos: list[dict] | None = None,
) -> list[SisapQuery]:
    productos_base = productos or PRODUCTOS_AGRICOLAS_PRIORITARIOS
    procedencia_nombre = _find_nombre(PROCEDENCIAS_SISAP, procedencia_codigo)
    windows = _month_windows(fecha_inicio, fecha_fin)

    return [
        SisapQuery(
            modulo=modulo,
            producto_codigo=producto["codigo"],
            producto_nombre=producto["nombre"],
            fecha_inicio=window_inicio,
            fecha_fin=window_fin,
            procedencia_codigo=procedencia_codigo,
            procedencia_nombre=procedencia_nombre,
            mercado_codigo=mercado_codigo,
            mercado_nombre=mercado_nombre,
        )
        for producto in productos_base
        for window_inicio, window_fin in windows
    ]



def build_ciudades_queries(
    modulo: ModuloSisap,
    fecha_inicio: date,
    fecha_fin: date,
    region_codigo: str,
    productos: list[dict] | None = None,
) -> list[SisapQuery]:
    productos_base = productos or PRODUCTOS_AGRICOLAS_PRIORITARIOS
    region_nombre = _find_nombre(PROCEDENCIAS_SISAP, region_codigo)
    windows = _month_windows(fecha_inicio, fecha_fin)

    return [
        SisapQuery(
            modulo=modulo,
            producto_codigo=producto["codigo"],
            producto_nombre=producto["nombre"],
            fecha_inicio=window_inicio,
            fecha_fin=window_fin,
            region_codigo=region_codigo,
            region_nombre=region_nombre,
        )
        for producto in productos_base
        for window_inicio, window_fin in windows
    ]

