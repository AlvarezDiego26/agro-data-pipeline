from datetime import date, timedelta

from sisap_light.ingesta_datos.catalogos.productos import PRODUCTOS_AGRICOLAS_PRIORITARIOS
from sisap_light.ingesta_datos.catalogos.procedencias import PROCEDENCIAS_SISAP
from sisap_light.schemas import ModuloSisap, SisapQuery


def _find_nombre(items: list[dict], codigo: str) -> str | None:
    for item in items:
        if item["codigo"] == codigo:
            return item["nombre"]
    return None


def _date_windows(fecha_inicio: date, fecha_fin: date) -> list[tuple[date, date]]:
    if fecha_inicio > fecha_fin:
        return []

    if fecha_inicio.year == fecha_fin.year and fecha_inicio.month == fecha_fin.month:
        return [(fecha_inicio, fecha_fin)]

    windows: list[tuple[date, date]] = []
    current_start = fecha_inicio

    while current_start <= fecha_fin:
        next_month = (current_start.replace(day=28) + timedelta(days=4)).replace(day=1)
        current_end = min(next_month - timedelta(days=1), fecha_fin)
        windows.append((current_start, current_end))
        current_start = current_end + timedelta(days=1)

    return windows


def build_mayorista_queries(
    modulo: ModuloSisap,
    fecha_inicio: date,
    fecha_fin: date,
    procedencia_codigo: str | None,
    mercado_codigo: str,
    mercado_nombre: str,
    productos: list[dict] | None = None,
) -> list[SisapQuery]:
    """procedencia_codigo vacio o None: consulta mayorista sin filtro de procedencia (volumen consolidado)."""
    productos_base = productos or PRODUCTOS_AGRICOLAS_PRIORITARIOS
    code = (procedencia_codigo or "").strip()
    procedencia_nombre = _find_nombre(PROCEDENCIAS_SISAP, code) if code else None
    windows = _date_windows(fecha_inicio, fecha_fin)

    return [
        SisapQuery(
            modulo=modulo,
            producto_codigo=producto["codigo"],
            producto_nombre=producto["nombre"],
            fecha_inicio=window_inicio,
            fecha_fin=window_fin,
            procedencia_codigo=code or None,
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
    windows = _date_windows(fecha_inicio, fecha_fin)

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

