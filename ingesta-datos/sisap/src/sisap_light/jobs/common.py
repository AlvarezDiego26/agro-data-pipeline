from __future__ import annotations
from sisap_light.schemas import ModuloSisap

from datetime import date, timedelta
import unicodedata
from pathlib import Path

import polars as pl
from loguru import logger

from sisap_light.ingesta_datos.catalogos.productos import PRODUCTOS_AGRICOLAS_PRIORITARIOS
from sisap_light.ingesta_datos.catalogos.procedencias import PROCEDENCIAS_SISAP
from sisap_light.ingesta_datos.catalogos.mercados import MERCADOS_SISAP
from sisap_light.config import get_settings
from sisap_light.procesamiento.storage.control import (
    append_control_events,
    build_control_event_id,
    build_control_record_timestamp,
    get_control_status,
    upsert_control_records,
)
from sisap_light.procesamiento.storage.delta import save_delta_table
from sisap_light.procesamiento.storage.merge import deduplicate_dataset
from sisap_light.procesamiento.storage.parquet import save_partitioned_parquet
from sisap_light.procesamiento.limpieza import normalize_dataset, validate_expected_columns, validate_non_empty
from sisap_light.procesamiento.storage.merge import business_key_columns

_CONTROL_STATUS_CACHE: dict[tuple[str, str, str, str, str, str, str], dict[str, object] | None] = {}
_CONTROL_READ_DISABLED = False
_LEGACY_VOLUMEN_BOUNDS_CACHE: dict[str, tuple[date | None, date | None]] = {}


def _mercado_control_value(mercado_codigo: str | None) -> str:
    return mercado_codigo or ''


def control_state_key(modulo: str, mercado_codigo: str | None, producto_codigo: str) -> tuple[str, str, str]:
    return modulo, _mercado_control_value(mercado_codigo), producto_codigo


def _get_cached_control_status(
    control_modulo: str,
    output_name: str,
    scope_label: str,
    scope_value: str,
    producto_codigo: str,
    mercado_codigo: str | None = None,
) -> dict[str, object] | None:
    global _CONTROL_READ_DISABLED

    settings = get_settings()
    if not settings.sisap_use_control_table or _CONTROL_READ_DISABLED:
        return None

    mc = _mercado_control_value(mercado_codigo)
    cache_key = ('sisap', control_modulo, output_name, scope_label, scope_value, mc, producto_codigo)
    if cache_key not in _CONTROL_STATUS_CACHE:
        try:
            _CONTROL_STATUS_CACHE[cache_key] = get_control_status(
                fuente='sisap',
                modulo=control_modulo,
                dataset=output_name,
                scope_tipo=scope_label,
                scope_valor=scope_value,
                producto_codigo=producto_codigo,
                mercado_codigo=mc,
            )
        except TimeoutError:
            _CONTROL_READ_DISABLED = True
            _CONTROL_STATUS_CACHE[cache_key] = None
            logger.warning(
                'Se deshabilita la lectura de tabla de control para esta corrida; '
                'se continuara usando la ultima fecha detectada en los datos escritos.'
            )
    return _CONTROL_STATUS_CACHE[cache_key]


def normalize_text(value: str | None) -> str:
    text = unicodedata.normalize('NFKD', value or '')
    text = ''.join(ch for ch in text if not unicodedata.combining(ch))
    return text.strip().lower()


def slugify(value: str | None) -> str:
    text = normalize_text(value)
    text = text.replace(' ', '_').replace('/', '_').replace('-', '_')
    while '__' in text:
        text = text.replace('__', '_')
    return text or 'no_definido'


def find_by_codigo(items: list[dict], codigo: str | None) -> dict | None:
    if not codigo:
        return None
    for item in items:
        if item['codigo'] == codigo:
            return item
    return None


def find_by_nombre(items: list[dict], nombre: str | None) -> dict | None:
    if not nombre:
        return None
    target = normalize_text(nombre)
    for item in items:
        if normalize_text(item['nombre']) == target:
            return item
    return None


def resolve_item(items: list[dict], codigo: str | None, nombre: str | None, entity_label: str) -> dict:
    item = find_by_codigo(items, codigo)
    if item is None:
        item = find_by_nombre(items, nombre)
    if item is None:
        raise ValueError(f'No se pudo resolver {entity_label}. Usa codigo o nombre valido.')
    return item


def resolve_productos(producto_codigo: str | None, producto_nombre: str | None) -> list[dict]:
    if producto_codigo:
        producto = find_by_codigo(PRODUCTOS_AGRICOLAS_PRIORITARIOS, producto_codigo)
        if producto is None:
            raise ValueError('No se pudo resolver el producto por codigo.')
        return [producto]

    if producto_nombre:
        producto = find_by_nombre(PRODUCTOS_AGRICOLAS_PRIORITARIOS, producto_nombre)
        if producto is None:
            raise ValueError('No se pudo resolver el producto por nombre.')
        return [producto]

    settings = get_settings()
    productos = PRODUCTOS_AGRICOLAS_PRIORITARIOS
    if settings.sisap_max_productos is not None and settings.sisap_max_productos > 0:
        return productos[: settings.sisap_max_productos]
    return productos


def resolve_mercados(mercado_codigo: str | None, mercado_nombre: str | None) -> list[dict]:
    if mercado_codigo:
        if mercado_codigo == '*':
            return [{'codigo': '*', 'nombre': 'Todos los mercados'}]
        mercado = find_by_codigo(MERCADOS_SISAP, mercado_codigo)
        if mercado is None:
            raise ValueError('No se pudo resolver el mercado por codigo.')
        return [mercado]

    if mercado_nombre:
        mercado = find_by_nombre(MERCADOS_SISAP, mercado_nombre)
        if mercado is None:
            raise ValueError('No se pudo resolver el mercado por nombre.')
        return [mercado]

    return MERCADOS_SISAP


def iter_mercados_ejecucion(mercado_nombre_override: str | None = None) -> list[dict]:
    settings = get_settings()
    if mercado_nombre_override:
        return resolve_mercados(None, mercado_nombre_override)
    if settings.sisap_mercado_codigo:
        return resolve_mercados(settings.sisap_mercado_codigo, None)
    if settings.sisap_mercado_nombre:
        return resolve_mercados(None, settings.sisap_mercado_nombre)

    resolved: list[dict] = []
    for token in settings.mercados_resueltos:
        mercado = find_by_nombre(MERCADOS_SISAP, token)
        if mercado is None:
            mercado = find_by_codigo(MERCADOS_SISAP, token)
        if mercado:
            resolved.append(mercado)
        else:
            logger.warning('Mercado ignorado (no esta en catalogo MERCADOS_SISAP): {}', token)

    if not resolved:
        raise ValueError(
            'No hay mercados validos para ejecutar. Define sisap_mercados en .env o sisap_mercado_codigo / sisap_mercado_nombre.'
        )
    return resolved


def apply_producto_filters(productos: list[dict]) -> list[dict]:
    settings = get_settings()
    if settings.sisap_producto_codigo:
        filtered = [item for item in productos if item['codigo'] == settings.sisap_producto_codigo]
        if not filtered:
            raise ValueError(
                f"El producto_codigo={settings.sisap_producto_codigo} no aparece en la lista resuelta para este mercado."
            )
        productos = filtered
    elif settings.sisap_producto_nombre:
        target = normalize_text(settings.sisap_producto_nombre)
        filtered = [item for item in productos if normalize_text(item['nombre']) == target]
        if not filtered:
            raise ValueError(
                f"El producto_nombre={settings.sisap_producto_nombre} no aparece en la lista resuelta para este mercado."
            )
        productos = filtered

    if settings.sisap_max_productos is not None and settings.sisap_max_productos > 0:
        return productos[: settings.sisap_max_productos]
    return productos


def discover_productos_mercado_mayorista(mercado_codigo: str) -> list[dict]:
    if mercado_codigo == '*':
        return []

    from sisap_light.ingesta_datos.extractores.sisap_mayorista import SisapMayoristaExtractor
    extractor = SisapMayoristaExtractor()
    html = extractor.fetch_productos_por_mercado_html(mercado_codigo)
    from sisap_light.procesamiento.parsers.html_forms import extract_checkbox_products

    raw = extract_checkbox_products(html)
    productos = [{'codigo': item['value'], 'nombre': item['label']} for item in raw if item.get('value') and item['value'] != 'NA']
    return productos


def resolve_productos_for_mercado_mayorista(mercado_codigo: str) -> list[dict]:
    try:
        discovered = discover_productos_mercado_mayorista(mercado_codigo)
    except Exception as exc:
        logger.warning(
            'Fallo al descubrir productos desde SISAP para mercado {} ({}). Se usa catalogo estatico.',
            mercado_codigo,
            exc,
        )
        discovered = []

    if not discovered:
        logger.warning(
            'Lista de productos vacia para mercado {}; usando PRODUCTOS_AGRICOLAS_PRIORITARIOS.',
            mercado_codigo,
        )
        discovered = list(PRODUCTOS_AGRICOLAS_PRIORITARIOS)

    return apply_producto_filters(discovered)


def filter_plan(plan: list, max_queries: int | None) -> list:
    if max_queries and max_queries > 0:
        return plan[:max_queries]
    return plan


def build_product_folder(producto_nombre: str) -> str:
    return f'producto={slugify(producto_nombre)}'


def build_scope_folder(scope_label: str, scope_value: str) -> str:
    return f'{scope_label}={slugify(scope_value)}'


def build_dataset_name(output_name: str, scope_label: str, scope_value: str, producto_nombre: str) -> str:
    return f'{output_name}/{build_scope_folder(scope_label, scope_value)}/{build_product_folder(producto_nombre)}'


def build_local_output_dir(output_name: str, scope_label: str, scope_value: str, producto_nombre: str) -> Path:
    settings = get_settings()
    return settings.clean_dir / output_name / build_scope_folder(scope_label, scope_value) / build_product_folder(producto_nombre)


def build_scope_output_dir(output_name: str, scope_label: str, scope_value: str) -> Path:
    settings = get_settings()
    return settings.clean_dir / output_name / build_scope_folder(scope_label, scope_value)


def _get_delta_date_bounds(dataset_name: str) -> tuple[date | None, date | None]:
    settings = get_settings()
    try:
        uri = settings.build_delta_uri(dataset_name)
        fecha_df = (
            pl.scan_delta(uri, storage_options=settings.delta_storage_options)
            .select(
                pl.col('fecha').min().alias('min_fecha'),
                pl.col('fecha').max().alias('max_fecha'),
            )
            .collect()
        )
        if fecha_df.is_empty():
            return None, None
        return fecha_df.row(0)
    except Exception:
        return None, None


def _get_delta_consolidated_bounds(
    output_name: str,
    scope_label: str,
    scope_value: str,
    producto_codigo: str,
    mercado_codigo: str | None,
) -> tuple[date | None, date | None]:
    settings = get_settings()
    try:
        uri = settings.build_delta_uri(output_name)
        scan = pl.scan_delta(uri, storage_options=settings.delta_storage_options)
        schema_cols = set(scan.collect_schema().names())
        lf = scan.filter(pl.col('producto_codigo') == producto_codigo)
        if scope_label == 'procedencia':
            lf = lf.filter(pl.col('procedencia') == scope_value)
        elif scope_label == 'region':
            lf = lf.filter(pl.col('region') == scope_value)
        elif scope_label == 'volumen_mercado' and 'mercado_codigo' in schema_cols:
            lf = lf.filter(pl.col('mercado_codigo') == scope_value)
        # Tablas antiguas sin mercado_codigo: no filtrar (bounds menos precisos, migra con primera escritura nueva).
        if mercado_codigo and 'mercado_codigo' in schema_cols:
            lf = lf.filter(pl.col('mercado_codigo') == mercado_codigo)
        fecha_df = lf.select(
            pl.col('fecha').min().alias('min_fecha'),
            pl.col('fecha').max().alias('max_fecha'),
        ).collect()
        if fecha_df.is_empty():
            return None, None
        row = fecha_df.row(0)
        if row[0] is None and row[1] is None:
            return None, None
        return row
    except Exception:
        return None, None


def _get_local_parquet_date_bounds(base_dir: Path) -> tuple[date | None, date | None]:
    parquet_files = list(base_dir.rglob('data.parquet'))
    if not parquet_files:
        return None, None

    minima: date | None = None
    maxima: date | None = None
    for file_path in parquet_files:
        try:
            fecha_df = pl.read_parquet(file_path, columns=['fecha']).drop_nulls()
            if fecha_df.is_empty():
                continue
            current_min = fecha_df.get_column('fecha').min()
            current_max = fecha_df.get_column('fecha').max()
            if minima is None or current_min < minima:
                minima = current_min
            if maxima is None or current_max > maxima:
                maxima = current_max
        except Exception:
            continue
    return minima, maxima


def _merge_date_bounds(rows: list[tuple[date | None, date | None]]) -> tuple[date | None, date | None]:
    mins = [r[0] for r in rows if r[0] is not None]
    maxs = [r[1] for r in rows if r[1] is not None]
    return (min(mins) if mins else None, max(maxs) if maxs else None)


def get_loaded_date_bounds(
    output_name: str,
    scope_label: str,
    scope_value: str,
    producto_codigo: str,
    producto_nombre: str,
    mercado_codigo: str | None = None,
) -> tuple[date | None, date | None]:
    settings = get_settings()
    if settings.delta_enabled:
        return _get_delta_consolidated_bounds(
            output_name, scope_label, scope_value, producto_codigo, mercado_codigo
        )

    dataset_name = build_dataset_name(output_name, scope_label, scope_value, producto_nombre)
    min_delta_date, max_delta_date = _get_delta_date_bounds(dataset_name)
    if max_delta_date is not None:
        return min_delta_date, max_delta_date

    local_dir = build_local_output_dir(output_name, scope_label, scope_value, producto_nombre)
    return _get_local_parquet_date_bounds(local_dir)


def resolve_query_dates(
    control_modulo: str,
    output_name: str,
    scope_label: str,
    scope_value: str,
    producto_codigo: str,
    producto_nombre: str,
    fecha_inicio: date,
    fecha_fin: date,
    mercado_codigo: str | None = None,
) -> tuple[date, date] | None:
    global _CONTROL_READ_DISABLED
    settings = get_settings()
    if settings.is_manual or not settings.is_incremental:
        return fecha_inicio, fecha_fin

    control_last_loaded: date | None = None
    control_min_loaded: date | None = None
    control_max_loaded: date | None = None
    history_complete = False
    if settings.sisap_use_control_table and not _CONTROL_READ_DISABLED:
        control_state = _get_cached_control_status(
            control_modulo,
            output_name,
            scope_label,
            scope_value,
            producto_codigo,
            mercado_codigo,
        )
        if control_state:
            history_complete = bool(control_state.get('historico_completo'))
            control_last_loaded = control_state.get('ultima_fecha_exitosa')
            control_min_loaded = control_state.get('fecha_minima_exitosa')
            control_max_loaded = control_state.get('fecha_maxima_exitosa')

    data_min_loaded, data_max_loaded = get_loaded_date_bounds(
        output_name,
        scope_label,
        scope_value,
        producto_codigo,
        producto_nombre,
        mercado_codigo,
    )

    # La fuente de verdad para cobertura es la data escrita.
    # El control solo sirve como apoyo cuando todavia no existe data materializada.
    min_loaded = data_min_loaded if data_min_loaded is not None else control_min_loaded
    max_loaded = data_max_loaded if data_max_loaded is not None else control_max_loaded
    last_loaded = data_max_loaded if data_max_loaded is not None else control_last_loaded

    if data_max_loaded is None:
        logger.info(
            'No se encontro data previa materializada para {} {}. '
            'Iniciando carga historica desde la fecha configurada: {}',
            output_name,
            producto_codigo,
            fecha_inicio,
        )
        return fecha_inicio, fecha_fin

    if min_loaded is not None and min_loaded > fecha_inicio:
        logger.warning(
            'Se detecto historial parcial en {} {} producto={}: '
            'la data escrita inicia en {} y la configuracion pide {}. '
            'Se relanzara backfill historico desde la fecha configurada.',
            output_name,
            scope_value,
            producto_codigo,
            min_loaded,
            fecha_inicio,
        )
        return fecha_inicio, fecha_fin

    if min_loaded is not None and min_loaded <= fecha_inicio and not history_complete:
        logger.info(
            'Se infirio historico completo desde la data escrita para {} {} producto={}. '
            'La cobertura ya arranca en {}.',
            output_name,
            scope_value,
            producto_codigo,
            min_loaded,
        )

    if settings.sisap_incremental_overlap_dias > 0:
        next_start = last_loaded - timedelta(days=settings.sisap_incremental_overlap_dias)
        next_start = max(next_start, fecha_inicio)
    else:
        next_start = last_loaded + timedelta(days=1)

    if next_start > fecha_fin:
        logger.debug(
            '{} {} producto={} ya esta al dia (ultima={}, fin={})',
            output_name, scope_value, producto_codigo, last_loaded, fecha_fin,
        )
        return None

    return next_start, fecha_fin


def expand_mayorista_plan_for_procedencia(
    control_modulo: str,
    output_name: str,
    modulo: 'ModuloSisap',
    mercado: dict,
    procedencia: dict | None,
    productos_override: list[dict] | None = None,
) -> list['SisapQuery']:
    """procedencia=None: volumen consolidado por mercado (sin filtro procedencias[] en SISAP)."""
    from sisap_light.ingesta_datos.planners import build_mayorista_queries
    from sisap_light.schemas import SisapQuery

    settings = get_settings()
    if productos_override is not None:
        productos_mercado = apply_producto_filters(list(productos_override))
    else:
        productos_mercado = resolve_productos_for_mercado_mayorista(mercado['codigo'])

    if procedencia is None:
        scope_label = 'volumen_mercado'
        m_code = str(mercado['codigo'])
        scope_value = 'consolidado' if m_code == '*' else m_code
        proc_code: str | None = None
    else:
        scope_label = 'procedencia'
        scope_value = procedencia['nombre']
        proc_code = procedencia['codigo']

    plan: list[SisapQuery] = []
    for producto in productos_mercado:
        resolved_dates = resolve_query_dates(
            control_modulo=control_modulo,
            output_name=output_name,
            scope_label=scope_label,
            scope_value=scope_value,
            producto_codigo=producto['codigo'],
            producto_nombre=producto['nombre'],
            fecha_inicio=settings.fecha_inicio_resuelta,
            fecha_fin=settings.fecha_fin_resuelta,
            mercado_codigo=mercado['codigo'],
        )
        if resolved_dates is None:
            continue

        fecha_inicio, fecha_fin = resolved_dates
        plan.extend(
            build_mayorista_queries(
                modulo=modulo,
                fecha_inicio=fecha_inicio,
                fecha_fin=fecha_fin,
                procedencia_codigo=proc_code,
                mercado_codigo=mercado['codigo'],
                mercado_nombre=mercado['nombre'],
                productos=[producto],
            )
        )
    return plan


def init_control_states() -> dict[tuple[str, str, str], dict]:
    return {}


def register_control_query(
    states: dict[tuple[str, str, str], dict],
    modulo: str,
    dataset: str,
    scope_label: str,
    scope_value: str,
    query,
) -> None:
    key = control_state_key(modulo, query.mercado_codigo, query.producto_codigo)
    state = states.get(key)
    if state is None:
        previous_state = _get_cached_control_status(
            modulo,
            dataset,
            scope_label,
            scope_value,
            query.producto_codigo,
            query.mercado_codigo,
        )
        state = {
            'fuente': 'sisap',
            'modulo': modulo,
            'dataset': dataset,
            'scope_tipo': scope_label,
            'scope_valor': scope_value,
            'mercado_codigo': query.mercado_codigo or '',
            'mercado_nombre': query.mercado_nombre or '',
            'producto_codigo': query.producto_codigo,
            'producto_nombre': query.producto_nombre,
            'modo_carga': get_settings().sisap_modo_carga,
            'fecha_inicio_solicitada': query.fecha_inicio,
            'fecha_fin_solicitada': query.fecha_fin,
            'fecha_inicio_ejecutada': None,
            'fecha_fin_ejecutada': None,
            'ultima_fecha_exitosa': previous_state.get('ultima_fecha_exitosa') if previous_state else None,
            'fecha_minima_exitosa': previous_state.get('fecha_minima_exitosa') if previous_state else None,
            'fecha_maxima_exitosa': previous_state.get('fecha_maxima_exitosa') if previous_state else None,
            'historico_completo': bool(previous_state.get('historico_completo')) if previous_state else False,
            'estado': 'pendiente',
            'mensaje_error': None,
            '_bloqueado': False,
        }
        states[key] = state
    else:
        if query.fecha_inicio < state['fecha_inicio_solicitada']:
            state['fecha_inicio_solicitada'] = query.fecha_inicio
        if query.fecha_fin > state['fecha_fin_solicitada']:
            state['fecha_fin_solicitada'] = query.fecha_fin


def register_control_success(
    states: dict[tuple[str, str, str], dict],
    modulo: str,
    dataset: str,
    scope_label: str,
    scope_value: str,
    query,
    estado: str = 'success',
) -> None:
    register_control_query(states, modulo, dataset, scope_label, scope_value, query)
    state = states[control_state_key(modulo, query.mercado_codigo, query.producto_codigo)]

    if state['fecha_inicio_ejecutada'] is None or query.fecha_inicio < state['fecha_inicio_ejecutada']:
        state['fecha_inicio_ejecutada'] = query.fecha_inicio
    if state['fecha_fin_ejecutada'] is None or query.fecha_fin > state['fecha_fin_ejecutada']:
        state['fecha_fin_ejecutada'] = query.fecha_fin

    if not state['_bloqueado']:
        if state['fecha_minima_exitosa'] is None or query.fecha_inicio < state['fecha_minima_exitosa']:
            state['fecha_minima_exitosa'] = query.fecha_inicio
        if state['fecha_maxima_exitosa'] is None or query.fecha_fin > state['fecha_maxima_exitosa']:
            state['fecha_maxima_exitosa'] = query.fecha_fin
        state['ultima_fecha_exitosa'] = query.fecha_fin
        state['estado'] = estado
        state['mensaje_error'] = None


def register_control_failure(
    states: dict[tuple[str, str, str], dict],
    modulo: str,
    dataset: str,
    scope_label: str,
    scope_value: str,
    query,
    error_message: str,
) -> None:
    register_control_query(states, modulo, dataset, scope_label, scope_value, query)
    state = states[control_state_key(modulo, query.mercado_codigo, query.producto_codigo)]

    if state['fecha_inicio_ejecutada'] is None or query.fecha_inicio < state['fecha_inicio_ejecutada']:
        state['fecha_inicio_ejecutada'] = query.fecha_inicio
    if state['fecha_fin_ejecutada'] is None or query.fecha_fin > state['fecha_fin_ejecutada']:
        state['fecha_fin_ejecutada'] = query.fecha_fin

    state['_bloqueado'] = True
    state['estado'] = 'error'
    if not state['mensaje_error']:
        state['mensaje_error'] = error_message


def persist_control_states(states: dict[tuple[str, str, str], dict]) -> str:
    if not get_settings().sisap_use_control_table:
        return ''
    if not states:
        return ''

    now = build_control_record_timestamp()
    configured_start = get_settings().fecha_inicio_resuelta
    rows: list[dict] = []
    for state in states.values():
        ultima_fecha_exitosa = state['ultima_fecha_exitosa']
        fecha_minima_exitosa = state['fecha_minima_exitosa']
        fecha_maxima_exitosa = state['fecha_maxima_exitosa']
        if state['_bloqueado']:
            estado = 'error'
        elif (
            state['estado'] == 'empty'
            and ultima_fecha_exitosa is not None
            and ultima_fecha_exitosa >= state['fecha_fin_solicitada']
        ):
            estado = 'empty'
        elif ultima_fecha_exitosa is not None and state['estado'] != 'empty' and ultima_fecha_exitosa >= state['fecha_fin_solicitada']:
            estado = 'success'
        elif ultima_fecha_exitosa is not None:
            estado = 'partial'
        else:
            estado = state['estado']

        historico_completo = (
            not state['_bloqueado']
            and fecha_minima_exitosa is not None
            and fecha_maxima_exitosa is not None
            and fecha_minima_exitosa <= configured_start
            and fecha_maxima_exitosa >= state['fecha_fin_solicitada']
            and estado in {'success', 'empty'}
        )

        rows.append(
            {
                'fuente': state['fuente'],
                'modulo': state['modulo'],
                'dataset': state['dataset'],
                'scope_tipo': state['scope_tipo'],
                'scope_valor': state['scope_valor'],
                'mercado_codigo': state['mercado_codigo'],
                'mercado_nombre': state['mercado_nombre'],
                'producto_codigo': state['producto_codigo'],
                'producto_nombre': state['producto_nombre'],
                'modo_carga': state['modo_carga'],
                'fecha_inicio_solicitada': state['fecha_inicio_solicitada'],
                'fecha_fin_solicitada': state['fecha_fin_solicitada'],
                'fecha_inicio_ejecutada': state['fecha_inicio_ejecutada'],
                'fecha_fin_ejecutada': state['fecha_fin_ejecutada'],
                'ultima_fecha_exitosa': ultima_fecha_exitosa,
                'fecha_minima_exitosa': fecha_minima_exitosa,
                'fecha_maxima_exitosa': fecha_maxima_exitosa,
                'historico_completo': historico_completo,
                'estado': estado,
                'mensaje_error': state['mensaje_error'],
                'ejecutado_por': 'sisap_light',
                'fecha_ejecucion': now,
                'fecha_actualizacion': now,
            }
        )

    return upsert_control_records(pl.DataFrame(rows))


def persist_control_event(
    modulo: str,
    dataset: str,
    scope_label: str,
    scope_value: str,
    query,
    estado: str,
    mensaje_error: str | None = None,
) -> str:
    if not get_settings().sisap_use_control_table:
        return ''
    now = build_control_record_timestamp()
    ultima_fecha_exitosa = query.fecha_fin if estado in {'success', 'partial'} else None
    historico_completo = (
        estado in {'success', 'empty'}
        and query.fecha_inicio <= get_settings().fecha_inicio_resuelta
    )
    event_df = pl.DataFrame(
        [
            {
                'evento_id': build_control_event_id(),
                'fuente': 'sisap',
                'modulo': modulo,
                'dataset': dataset,
                'scope_tipo': scope_label,
                'scope_valor': scope_value,
                'mercado_codigo': query.mercado_codigo or '',
                'mercado_nombre': query.mercado_nombre or '',
                'producto_codigo': query.producto_codigo,
                'producto_nombre': query.producto_nombre,
                'modo_carga': get_settings().sisap_modo_carga,
                'fecha_inicio_solicitada': query.fecha_inicio,
                'fecha_fin_solicitada': query.fecha_fin,
                'fecha_inicio_ejecutada': query.fecha_inicio,
                'fecha_fin_ejecutada': query.fecha_fin,
                'ultima_fecha_exitosa': ultima_fecha_exitosa,
                'fecha_minima_exitosa': query.fecha_inicio if ultima_fecha_exitosa is not None else None,
                'fecha_maxima_exitosa': query.fecha_fin if ultima_fecha_exitosa is not None else None,
                'historico_completo': historico_completo,
                'estado': estado,
                'mensaje_error': mensaje_error or '',
                'ejecutado_por': 'sisap_light',
                'fecha_ejecucion': now,
                'fecha_actualizacion': now,
            }
        ]
    )
    return append_control_events(event_df)


def build_control_event_row(
    modulo: str,
    dataset: str,
    scope_label: str,
    scope_value: str,
    query,
    estado: str,
    mensaje_error: str | None = None,
) -> dict[str, object]:
    now = build_control_record_timestamp()
    ultima_fecha_exitosa = query.fecha_fin if estado in {'success', 'partial'} else None
    historico_completo = (
        estado in {'success', 'empty'}
        and query.fecha_inicio <= get_settings().fecha_inicio_resuelta
    )
    return {
        'evento_id': build_control_event_id(),
        'fuente': 'sisap',
        'modulo': modulo,
        'dataset': dataset,
        'scope_tipo': scope_label,
        'scope_valor': scope_value,
        'mercado_codigo': query.mercado_codigo or '',
        'mercado_nombre': query.mercado_nombre or '',
        'producto_codigo': query.producto_codigo,
        'producto_nombre': query.producto_nombre,
        'modo_carga': get_settings().sisap_modo_carga,
        'fecha_inicio_solicitada': query.fecha_inicio,
        'fecha_fin_solicitada': query.fecha_fin,
        'fecha_inicio_ejecutada': query.fecha_inicio,
        'fecha_fin_ejecutada': query.fecha_fin,
        'ultima_fecha_exitosa': ultima_fecha_exitosa,
        'fecha_minima_exitosa': query.fecha_inicio if ultima_fecha_exitosa is not None else None,
        'fecha_maxima_exitosa': query.fecha_fin if ultima_fecha_exitosa is not None else None,
        'historico_completo': historico_completo,
        'estado': estado,
        'mensaje_error': mensaje_error or '',
        'ejecutado_por': 'sisap_light',
        'fecha_ejecucion': now,
        'fecha_actualizacion': now,
    }


def persist_control_events_batch(event_rows: list[dict[str, object]]) -> str:
    if not get_settings().sisap_use_control_table:
        return ''
    if not event_rows:
        return ''
    return append_control_events(pl.DataFrame(event_rows))


def append_partitioned_output(
    frames: list[pl.DataFrame],
    output_name: str,
    expected_columns: list[str],
    sort_columns: list[str],
    scope_label: str,
    scope_value: str,
) -> Path:
    if not frames:
        raise ValueError(f'La corrida parcial de {output_name} no produjo data util.')

    final_df = pl.concat(frames, how='vertical_relaxed')

    # 1. Normalizar los nulos (sin tocar las llaves de negocio)
    final_df = normalize_dataset(final_df, output_name)

    # 2. Validaciones basicas de calidad
    validate_non_empty(final_df, output_name)
    validate_expected_columns(final_df, expected_columns, output_name)

    # 3. Formateo y orden local antes del Storage
    final_df = final_df.sort(sort_columns)
    final_df = final_df.with_columns(
        pl.col('fecha').alias('fecha_particion'),
        pl.col('fecha').dt.year().cast(pl.Int32).alias('anio'),
        pl.col('fecha').dt.strftime('%m').alias('mes'),
    )

    settings = get_settings()

    if settings.delta_enabled:
        save_delta_table(final_df, output_name, ['fecha_particion'])
        scope_output = build_scope_output_dir(output_name, scope_label, scope_value)
        scope_output.mkdir(parents=True, exist_ok=True)
        return scope_output

    scope_folder = build_scope_folder(scope_label, scope_value)
    scope_output = build_scope_output_dir(output_name, scope_label, scope_value)
    scope_output.mkdir(parents=True, exist_ok=True)

    productos = [
        str(item)
        for item in final_df.get_column('producto_nombre').drop_nulls().unique().sort().to_list()
    ]

    for producto_nombre in productos:
        product_folder = build_product_folder(producto_nombre)
        product_df = final_df.filter(pl.col('producto_nombre') == producto_nombre)
        output = scope_output / product_folder
        dataset_name = f'{output_name}/{scope_folder}/{product_folder}'
        save_partitioned_parquet(product_df, dataset_name, output, ['anio', 'mes'])

    return scope_output
