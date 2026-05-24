from datetime import date, timedelta

from sisap_light.ingesta_datos.catalogos.productos import PRODUCTOS_AGRICOLAS_PRIORITARIOS
from sisap_light.ingesta_datos.catalogos.procedencias import PROCEDENCIAS_SISAP
from sisap_light.config import get_settings
from sisap_light.schemas import ModuloSisap, SisapQuery


def _find_nombre(items: list[dict], codigo: str) -> str | None:
    for item in items:
        if item["codigo"] == codigo:
            return item["nombre"]
    return None


def _first_day_of_month(value: date) -> date:
    return value.replace(day=1)


def _add_months(value: date, months: int) -> date:
    year = value.year + ((value.month - 1 + months) // 12)
    month = ((value.month - 1 + months) % 12) + 1
    return date(year, month, 1)


def _split_with_month_step(
    fecha_inicio: date,
    fecha_fin: date,
    *,
    months_per_window: int,
) -> list[tuple[date, date]]:
    if fecha_inicio > fecha_fin:
        return []

    windows: list[tuple[date, date]] = []
    current_start = fecha_inicio
    step = max(int(months_per_window or 1), 1)
    while current_start <= fecha_fin:
        current_month_start = _first_day_of_month(current_start)
        window_limit = _add_months(current_month_start, step) - timedelta(days=1)
        current_end = min(window_limit, fecha_fin)
        windows.append((current_start, current_end))
        current_start = current_end + timedelta(days=1)
    return windows


def _date_windows(fecha_inicio: date, fecha_fin: date) -> list[tuple[date, date]]:
    if fecha_inicio > fecha_fin:
        return []

    settings = get_settings()
    recent_monthly_lookback = settings.recent_monthly_lookback_months
    historical_chunk_months = settings.historical_chunk_months

    if fecha_inicio.year == fecha_fin.year and fecha_inicio.month == fecha_fin.month:
        return [(fecha_inicio, fecha_fin)]

    recent_cutoff_month = _add_months(
        _first_day_of_month(fecha_fin),
        -(recent_monthly_lookback - 1),
    )

    if fecha_inicio >= recent_cutoff_month:
        return _split_with_month_step(fecha_inicio, fecha_fin, months_per_window=1)

    historical_end = min(fecha_fin, recent_cutoff_month - timedelta(days=1))
    windows = _split_with_month_step(
        fecha_inicio,
        historical_end,
        months_per_window=historical_chunk_months,
    )

    if fecha_fin >= recent_cutoff_month:
        recent_start = max(fecha_inicio, recent_cutoff_month)
        windows.extend(
            _split_with_month_step(recent_start, fecha_fin, months_per_window=1)
        )

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

