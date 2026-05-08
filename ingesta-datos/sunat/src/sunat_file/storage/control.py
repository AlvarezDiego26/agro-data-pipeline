from __future__ import annotations

from datetime import datetime
from pathlib import Path
from threading import RLock
from uuid import uuid4

import polars as pl
from loguru import logger

from sunat_file.config import get_settings

CONTROL_KEY_COLUMNS = ['fuente', 'modulo', 'dataset', 'scope_tipo', 'scope_valor']
CONTROL_STRING_COLUMNS = [
    'evento_id',
    'fuente',
    'modulo',
    'dataset',
    'scope_tipo',
    'scope_valor',
    'modo_carga',
    'estado',
    'mensaje_error',
    'ejecutado_por',
]
CONTROL_DATE_COLUMNS = [
    'fecha_inicio_solicitada',
    'fecha_fin_solicitada',
    'fecha_inicio_ejecutada',
    'fecha_fin_ejecutada',
    'ultima_fecha_exitosa',
]
CONTROL_DATETIME_COLUMNS = [
    'fecha_ejecucion',
    'fecha_actualizacion',
]
CONTROL_EVENT_KEY_COLUMNS = ['evento_id']
CONTROL_STATE_LOCK = RLock()


def _control_uri() -> str:
    settings = get_settings()
    return settings.build_delta_uri(settings.sunat_control_dataset)


def _control_events_uri() -> str:
    settings = get_settings()
    return settings.build_delta_uri(settings.sunat_control_events_dataset)


def _local_control_state_path() -> Path:
    return get_settings().control_local_state_path


def _pending_control_state_path() -> Path:
    return get_settings().control_pending_state_path


def _local_control_events_path() -> Path:
    return get_settings().control_local_events_path


def _pending_control_events_path() -> Path:
    return get_settings().control_pending_events_path


def _normalize_control_frame(frame: pl.DataFrame) -> pl.DataFrame:
    if frame.is_empty():
        return frame

    expressions: list[pl.Expr] = []
    for column in CONTROL_STRING_COLUMNS:
        if column in frame.columns:
            expressions.append(pl.col(column).cast(pl.Utf8, strict=False).fill_null(''))
    for column in CONTROL_DATE_COLUMNS:
        if column in frame.columns:
            expressions.append(pl.col(column).cast(pl.Date, strict=False))
    for column in CONTROL_DATETIME_COLUMNS:
        if column in frame.columns:
            expressions.append(pl.col(column).cast(pl.Datetime(time_unit='us'), strict=False))
    return frame.with_columns(expressions)


def _merge_control_frames(*frames: pl.DataFrame) -> pl.DataFrame:
    normalized = [_normalize_control_frame(frame) for frame in frames if not frame.is_empty()]
    if not normalized:
        return pl.DataFrame()
    merged = pl.concat(normalized, how='vertical_relaxed')
    if 'fecha_actualizacion' not in merged.columns:
        return merged
    return merged.sort('fecha_actualizacion').unique(subset=CONTROL_KEY_COLUMNS, keep='last')


def _append_event_frames(*frames: pl.DataFrame) -> pl.DataFrame:
    normalized = [_normalize_control_frame(frame) for frame in frames if not frame.is_empty()]
    if not normalized:
        return pl.DataFrame()
    merged = pl.concat(normalized, how='vertical_relaxed')
    if 'fecha_actualizacion' in merged.columns:
        merged = merged.sort('fecha_actualizacion')
    if 'evento_id' in merged.columns:
        merged = merged.unique(subset=CONTROL_EVENT_KEY_COLUMNS, keep='last')
    return merged


def _read_local_control_state() -> pl.DataFrame:
    path = _local_control_state_path()
    if not path.exists():
        return pl.DataFrame()
    try:
        return _normalize_control_frame(pl.read_parquet(path))
    except Exception:
        logger.exception('No se pudo leer el cache local de control SUNAT en {}', path)
        return pl.DataFrame()


def _write_local_control_state(control_df: pl.DataFrame) -> None:
    path = _local_control_state_path()
    if control_df.is_empty():
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    control_df.write_parquet(path)


def _read_pending_control_state() -> pl.DataFrame:
    path = _pending_control_state_path()
    if not path.exists():
        return pl.DataFrame()
    try:
        return _normalize_control_frame(pl.read_parquet(path))
    except Exception:
        logger.exception('No se pudo leer la cola pendiente de control SUNAT en {}', path)
        return pl.DataFrame()


def _write_pending_control_state(control_df: pl.DataFrame) -> None:
    path = _pending_control_state_path()
    if control_df.is_empty():
        if path.exists():
            path.unlink()
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    control_df.write_parquet(path)


def _read_local_control_events() -> pl.DataFrame:
    path = _local_control_events_path()
    if not path.exists():
        return pl.DataFrame()
    try:
        return _normalize_control_frame(pl.read_parquet(path))
    except Exception:
        logger.exception('No se pudo leer el journal local de eventos SUNAT en {}', path)
        return pl.DataFrame()


def _write_local_control_events(events_df: pl.DataFrame) -> None:
    path = _local_control_events_path()
    if events_df.is_empty():
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    events_df.write_parquet(path)


def _read_pending_control_events() -> pl.DataFrame:
    path = _pending_control_events_path()
    if not path.exists():
        return pl.DataFrame()
    try:
        return _normalize_control_frame(pl.read_parquet(path))
    except Exception:
        logger.exception('No se pudo leer la cola pendiente de eventos SUNAT en {}', path)
        return pl.DataFrame()


def _write_pending_control_events(events_df: pl.DataFrame) -> None:
    path = _pending_control_events_path()
    if events_df.is_empty():
        if path.exists():
            path.unlink()
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    events_df.write_parquet(path)


def read_control_table() -> pl.DataFrame:
    with CONTROL_STATE_LOCK:
        settings = get_settings()
        table_uri = _control_uri()
        storage_options = settings.delta_storage_options
        local_state = _read_local_control_state()
        pending_state = _read_pending_control_state()

        try:
            from deltalake import DeltaTable

            table = DeltaTable(table_uri, storage_options=storage_options)
            remote_state = _normalize_control_frame(pl.from_arrow(table.to_pyarrow_table()))
            merged_state = _merge_control_frames(remote_state, local_state, pending_state)
            if not merged_state.is_empty():
                _write_local_control_state(merged_state)
            return merged_state
        except Exception:
            return _merge_control_frames(local_state, pending_state)


def get_last_successful_date(fuente: str, dataset: str) -> object | None:
    control_df = read_control_table()
    if control_df.is_empty():
        return None
    filtered = control_df.filter(
        (pl.col('fuente') == fuente)
        & (pl.col('dataset') == dataset)
        & (pl.col('ultima_fecha_exitosa').is_not_null())
    )
    if filtered.is_empty():
        return None
    return filtered.sort('fecha_actualizacion').get_column('ultima_fecha_exitosa').tail(1).item()


def list_scope_values_by_status(
    fuente: str,
    modulo: str,
    dataset: str,
    scope_tipo: str,
    estados: set[str] | None = None,
) -> set[str]:
    control_df = read_control_table()
    if control_df.is_empty():
        return set()

    filtered = control_df.filter(
        (pl.col('fuente') == fuente)
        & (pl.col('modulo') == modulo)
        & (pl.col('dataset') == dataset)
        & (pl.col('scope_tipo') == scope_tipo)
    )
    if estados:
        filtered = filtered.filter(pl.col('estado').is_in(sorted(estados)))
    if filtered.is_empty():
        return set()
    return {
        value
        for value in filtered.get_column('scope_valor').drop_nulls().to_list()
        if value
    }


def upsert_control_records(records_df: pl.DataFrame) -> str:
    with CONTROL_STATE_LOCK:
        if records_df.is_empty():
            return ''

        settings = get_settings()
        table_uri = _control_uri()
        storage_options = settings.delta_storage_options
        incoming_df = _normalize_control_frame(records_df)
        local_state = _read_local_control_state()
        pending_state = _read_pending_control_state()
        merged_local_state = _merge_control_frames(local_state, pending_state, incoming_df)
        if not merged_local_state.is_empty():
            _write_local_control_state(merged_local_state)

        if not settings.is_minio:
            if not merged_local_state.is_empty():
                Path(table_uri).mkdir(parents=True, exist_ok=True)
                from deltalake.writer import write_deltalake

                write_deltalake(
                    table_uri,
                    merged_local_state.to_arrow(),
                    mode='overwrite',
                    storage_options=storage_options,
                )
            _write_pending_control_state(pl.DataFrame())
            return table_uri

        try:
            from deltalake import DeltaTable

            existing_table = DeltaTable(table_uri, storage_options=storage_options)
            remote_state = _normalize_control_frame(pl.from_arrow(existing_table.to_pyarrow_table()))
        except Exception:
            remote_state = pl.DataFrame()

        merged_remote_state = _merge_control_frames(remote_state, pending_state, incoming_df)
        try:
            from deltalake.writer import write_deltalake

            write_deltalake(
                table_uri,
                merged_remote_state.to_arrow(),
                mode='overwrite',
                storage_options=storage_options,
            )
            _write_pending_control_state(pl.DataFrame())
            _write_local_control_state(merged_remote_state)
            return table_uri
        except Exception:
            logger.exception('No se pudo sincronizar control SUNAT con MinIO; se conserva en cache local.')
            merged_pending_state = _merge_control_frames(pending_state, incoming_df)
            _write_pending_control_state(merged_pending_state)
            return str(_pending_control_state_path())


def sync_pending_control_state() -> dict[str, object]:
    with CONTROL_STATE_LOCK:
        settings = get_settings()
        pending_state = _read_pending_control_state()
        if pending_state.is_empty():
            return {'synced': True, 'pending_records': 0, 'target': _control_uri() if settings.is_minio else str(_local_control_state_path())}

        result_path = upsert_control_records(pending_state)
        remaining_pending = _read_pending_control_state()
        return {
            'synced': remaining_pending.is_empty(),
            'pending_records': remaining_pending.height,
            'target': result_path,
        }


def append_control_events(events_df: pl.DataFrame) -> str:
    with CONTROL_STATE_LOCK:
        if events_df.is_empty():
            return ''

        settings = get_settings()
        events_uri = _control_events_uri()
        incoming_events = _normalize_control_frame(events_df)
        local_events = _read_local_control_events()
        pending_events = _read_pending_control_events()
        merged_local_events = _append_event_frames(local_events, incoming_events)
        if not merged_local_events.is_empty():
            _write_local_control_events(merged_local_events)

        if not settings.is_minio:
            if not incoming_events.is_empty():
                Path(events_uri).mkdir(parents=True, exist_ok=True)
                from deltalake.writer import write_deltalake

                write_deltalake(
                    events_uri,
                    incoming_events.to_arrow(),
                    mode='append',
                    storage_options=settings.delta_storage_options,
                )
            _write_pending_control_events(pl.DataFrame())
            return events_uri

        events_to_sync = _append_event_frames(pending_events, incoming_events)
        if events_to_sync.is_empty():
            return events_uri

        try:
            from deltalake.writer import write_deltalake

            write_deltalake(
                events_uri,
                events_to_sync.to_arrow(),
                mode='append',
                storage_options=settings.delta_storage_options,
            )
            _write_pending_control_events(pl.DataFrame())
            return events_uri
        except Exception:
            logger.exception('No se pudo sincronizar el journal de eventos SUNAT con MinIO; se conserva en cache local.')
            _write_pending_control_events(events_to_sync)
            return str(_pending_control_events_path())


def sync_pending_control_events() -> dict[str, object]:
    with CONTROL_STATE_LOCK:
        settings = get_settings()
        pending_events = _read_pending_control_events()
        if pending_events.is_empty():
            return {'synced': True, 'pending_records': 0, 'target': _control_events_uri() if settings.is_minio else str(_local_control_events_path())}

        result_path = append_control_events(pending_events)
        remaining_pending = _read_pending_control_events()
        return {
            'synced': remaining_pending.is_empty(),
            'pending_records': remaining_pending.height,
            'target': result_path,
        }


def get_control_sync_status() -> dict[str, object]:
    pending_state = _read_pending_control_state()
    local_state = _read_local_control_state()
    pending_events = _read_pending_control_events()
    local_events = _read_local_control_events()
    return {
        'pending_records': pending_state.height,
        'local_records': local_state.height,
        'pending_path': str(_pending_control_state_path()),
        'local_path': str(_local_control_state_path()),
        'pending_event_records': pending_events.height,
        'local_event_records': local_events.height,
        'pending_events_path': str(_pending_control_events_path()),
        'local_events_path': str(_local_control_events_path()),
    }


def build_control_record_timestamp() -> datetime:
    return datetime.now()


def build_control_event_id() -> str:
    return uuid4().hex
