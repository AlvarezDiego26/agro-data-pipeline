from __future__ import annotations

from datetime import date, timedelta
import unicodedata
from pathlib import Path

import polars as pl
from loguru import logger

from sisap_light.ingesta_datos.catalogos.productos import PRODUCTOS_AGRICOLAS_PRIORITARIOS
from sisap_light.config import get_settings
from sisap_light.procesamiento.storage.control import (
    append_control_events,
    build_control_event_id,
    build_control_record_timestamp,
    get_last_successful_date,
    upsert_control_records,
)
from sisap_light.procesamiento.storage.delta import save_delta_table
from sisap_light.procesamiento.storage.merge import deduplicate_dataset
from sisap_light.procesamiento.storage.parquet import save_partitioned_parquet
from sisap_light.procesamiento.limpieza import normalize_dataset, validate_expected_columns, validate_non_empty
from sisap_light.procesamiento.storage.merge import business_key_columns

_CONTROL_LAST_SUCCESS_CACHE: dict[tuple[str, str, str, str, str, str], object | None] = {}
_CONTROL_READ_DISABLED = False


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


def _get_last_delta_date(dataset_name: str) -> date | None:
    settings = get_settings()
    try:
        uri = settings.build_delta_uri(dataset_name)
        fecha_df = pl.scan_delta(uri, storage_options=settings.delta_storage_options).select(pl.col('fecha').max()).collect()
        if fecha_df.is_empty():
            return None
        return fecha_df.item()
    except Exception:
        return None


def _get_last_local_parquet_date(base_dir: Path) -> date | None:
    parquet_files = list(base_dir.rglob('data.parquet'))
    if not parquet_files:
        return None

    maxima: date | None = None
    for file_path in parquet_files:
        try:
            fecha_df = pl.read_parquet(file_path, columns=['fecha']).drop_nulls()
            if fecha_df.is_empty():
                continue
            current_max = fecha_df.get_column('fecha').max()
            if maxima is None or current_max > maxima:
                maxima = current_max
        except Exception:
            continue
    return maxima


def get_last_loaded_date(output_name: str, scope_label: str, scope_value: str, producto_nombre: str) -> date | None:
    dataset_name = build_dataset_name(output_name, scope_label, scope_value, producto_nombre)
    last_delta_date = _get_last_delta_date(dataset_name)
    if last_delta_date is not None:
        return last_delta_date

    local_dir = build_local_output_dir(output_name, scope_label, scope_value, producto_nombre)
    return _get_last_local_parquet_date(local_dir)


def resolve_query_dates(
    control_modulo: str,
    output_name: str,
    scope_label: str,
    scope_value: str,
    producto_codigo: str,
    producto_nombre: str,
    fecha_inicio: date,
    fecha_fin: date,
) -> tuple[date, date] | None:
    global _CONTROL_READ_DISABLED
    settings = get_settings()
    if settings.is_manual or not settings.is_incremental:
        return fecha_inicio, fecha_fin

    last_loaded: date | None = None
    if settings.sisap_use_control_table and not _CONTROL_READ_DISABLED:
        cache_key = ('sisap', control_modulo, output_name, scope_label, scope_value, producto_codigo)
        if cache_key not in _CONTROL_LAST_SUCCESS_CACHE:
            try:
                _CONTROL_LAST_SUCCESS_CACHE[cache_key] = get_last_successful_date(
                    fuente='sisap',
                    modulo=control_modulo,
                    dataset=output_name,
                    scope_tipo=scope_label,
                    scope_valor=scope_value,
                    producto_codigo=producto_codigo,
                )
            except TimeoutError:
                _CONTROL_READ_DISABLED = True
                _CONTROL_LAST_SUCCESS_CACHE[cache_key] = None
                logger.warning(
                    'Se deshabilita la lectura de tabla de control para esta corrida; '
                    'se continuara usando la ultima fecha detectada en los datos escritos.'
                )
        control_last = _CONTROL_LAST_SUCCESS_CACHE[cache_key]
        if control_last is not None:
            last_loaded = control_last

    if last_loaded is None:
        last_loaded = get_last_loaded_date(output_name, scope_label, scope_value, producto_nombre)

    if last_loaded is None:
        return fecha_inicio, fecha_fin

    if settings.sisap_incremental_overlap_dias > 0:
        next_start = last_loaded - timedelta(days=settings.sisap_incremental_overlap_dias)
        if next_start < fecha_inicio:
            next_start = fecha_inicio
    else:
        next_start = last_loaded + timedelta(days=1)

    if next_start > fecha_fin:
        return None

    return next_start, fecha_fin


def init_control_states() -> dict[tuple[str, str], dict]:
    return {}


def register_control_query(
    states: dict[tuple[str, str], dict],
    modulo: str,
    dataset: str,
    scope_label: str,
    scope_value: str,
    query,
) -> None:
    key = (modulo, query.producto_codigo)
    state = states.get(key)
    if state is None:
        state = {
            'fuente': 'sisap',
            'modulo': modulo,
            'dataset': dataset,
            'scope_tipo': scope_label,
            'scope_valor': scope_value,
            'producto_codigo': query.producto_codigo,
            'producto_nombre': query.producto_nombre,
            'modo_carga': get_settings().sisap_modo_carga,
            'fecha_inicio_solicitada': query.fecha_inicio,
            'fecha_fin_solicitada': query.fecha_fin,
            'fecha_inicio_ejecutada': None,
            'fecha_fin_ejecutada': None,
            'ultima_fecha_exitosa': None,
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
    states: dict[tuple[str, str], dict],
    modulo: str,
    dataset: str,
    scope_label: str,
    scope_value: str,
    query,
    estado: str = 'success',
) -> None:
    register_control_query(states, modulo, dataset, scope_label, scope_value, query)
    state = states[(modulo, query.producto_codigo)]

    if state['fecha_inicio_ejecutada'] is None or query.fecha_inicio < state['fecha_inicio_ejecutada']:
        state['fecha_inicio_ejecutada'] = query.fecha_inicio
    if state['fecha_fin_ejecutada'] is None or query.fecha_fin > state['fecha_fin_ejecutada']:
        state['fecha_fin_ejecutada'] = query.fecha_fin

    if not state['_bloqueado']:
        state['ultima_fecha_exitosa'] = query.fecha_fin
        state['estado'] = estado
        state['mensaje_error'] = None


def register_control_failure(
    states: dict[tuple[str, str], dict],
    modulo: str,
    dataset: str,
    scope_label: str,
    scope_value: str,
    query,
    error_message: str,
) -> None:
    register_control_query(states, modulo, dataset, scope_label, scope_value, query)
    state = states[(modulo, query.producto_codigo)]

    if state['fecha_inicio_ejecutada'] is None or query.fecha_inicio < state['fecha_inicio_ejecutada']:
        state['fecha_inicio_ejecutada'] = query.fecha_inicio
    if state['fecha_fin_ejecutada'] is None or query.fecha_fin > state['fecha_fin_ejecutada']:
        state['fecha_fin_ejecutada'] = query.fecha_fin

    state['_bloqueado'] = True
    state['estado'] = 'error'
    if not state['mensaje_error']:
        state['mensaje_error'] = error_message


def persist_control_states(states: dict[tuple[str, str], dict]) -> str:
    if not get_settings().sisap_use_control_table:
        return ''
    if not states:
        return ''

    now = build_control_record_timestamp()
    rows: list[dict] = []
    for state in states.values():
        ultima_fecha_exitosa = state['ultima_fecha_exitosa']
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

        rows.append(
            {
                'fuente': state['fuente'],
                'modulo': state['modulo'],
                'dataset': state['dataset'],
                'scope_tipo': state['scope_tipo'],
                'scope_valor': state['scope_valor'],
                'producto_codigo': state['producto_codigo'],
                'producto_nombre': state['producto_nombre'],
                'modo_carga': state['modo_carga'],
                'fecha_inicio_solicitada': state['fecha_inicio_solicitada'],
                'fecha_fin_solicitada': state['fecha_fin_solicitada'],
                'fecha_inicio_ejecutada': state['fecha_inicio_ejecutada'],
                'fecha_fin_ejecutada': state['fecha_fin_ejecutada'],
                'ultima_fecha_exitosa': ultima_fecha_exitosa,
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
    event_df = pl.DataFrame(
        [
            {
                'evento_id': build_control_event_id(),
                'fuente': 'sisap',
                'modulo': modulo,
                'dataset': dataset,
                'scope_tipo': scope_label,
                'scope_valor': scope_value,
                'producto_codigo': query.producto_codigo,
                'producto_nombre': query.producto_nombre,
                'modo_carga': get_settings().sisap_modo_carga,
                'fecha_inicio_solicitada': query.fecha_inicio,
                'fecha_fin_solicitada': query.fecha_fin,
                'fecha_inicio_ejecutada': query.fecha_inicio,
                'fecha_fin_ejecutada': query.fecha_fin,
                'ultima_fecha_exitosa': ultima_fecha_exitosa,
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
    return {
        'evento_id': build_control_event_id(),
        'fuente': 'sisap',
        'modulo': modulo,
        'dataset': dataset,
        'scope_tipo': scope_label,
        'scope_valor': scope_value,
        'producto_codigo': query.producto_codigo,
        'producto_nombre': query.producto_nombre,
        'modo_carga': get_settings().sisap_modo_carga,
        'fecha_inicio_solicitada': query.fecha_inicio,
        'fecha_fin_solicitada': query.fecha_fin,
        'fecha_inicio_ejecutada': query.fecha_inicio,
        'fecha_fin_ejecutada': query.fecha_fin,
        'ultima_fecha_exitosa': ultima_fecha_exitosa,
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
    keys = business_key_columns(output_name, final_df.columns)
    final_df = normalize_dataset(final_df, keys)

    # 2. Validaciones basicas de calidad
    validate_non_empty(final_df, output_name)
    validate_expected_columns(final_df, expected_columns, output_name)

    # 3. Formateo y orden local antes del Storage (el Storage hara la deduplicacion)
    final_df = final_df.sort(sort_columns)
    final_df = final_df.with_columns(
        pl.col('fecha').dt.year().cast(pl.Int32).alias('anio'),
        pl.col('fecha').dt.strftime('%m').alias('mes'),
    )

    settings = get_settings()
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

        if settings.delta_enabled:
            save_delta_table(product_df, dataset_name, ['anio', 'mes'])
        else:
            save_partitioned_parquet(product_df, dataset_name, output, ['anio', 'mes'])

    return scope_output
