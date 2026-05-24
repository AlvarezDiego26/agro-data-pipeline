from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from threading import RLock
from uuid import uuid4
import os
import time

import polars as pl
from loguru import logger

from sisap_light.config import get_settings
from sisap_light.procesamiento.storage.delta import get_delta_lock, get_delta_runtime

CONTROL_KEY_COLUMNS = [
    'fuente',
    'modulo',
    'dataset',
    'scope_tipo',
    'scope_valor',
    'mercado_codigo',
    'producto_codigo',
]

CONTROL_STRING_COLUMNS = [
    'fuente',
    'modulo',
    'dataset',
    'scope_tipo',
    'scope_valor',
    'mercado_codigo',
    'mercado_nombre',
    'producto_codigo',
    'producto_nombre',
    'modo_carga',
    'estado',
    'mensaje_error',
    'ejecutado_por',
]

CONTROL_BOOL_COLUMNS = [
    'historico_completo',
]

CONTROL_DATE_COLUMNS = [
    'fecha_inicio_solicitada',
    'fecha_fin_solicitada',
    'fecha_inicio_ejecutada',
    'fecha_fin_ejecutada',
    'ultima_fecha_exitosa',
    'fecha_minima_exitosa',
    'fecha_maxima_exitosa',
]

CONTROL_DATETIME_COLUMNS = [
    'fecha_ejecucion',
    'fecha_actualizacion',
]

CONTROL_EVENT_KEY_COLUMNS = [
    'evento_id',
]

CONTROL_STATE_LOCK = RLock()
LOCK_TIMEOUT_SECONDS = 900.0  # Increased from 300 to 15 minutes for distributed scenarios with multiple parallel tasks
LOCK_POLL_SECONDS = 0.2


def _control_uri() -> str:
    settings = get_settings()
    return settings.build_delta_uri(settings.sisap_control_dataset)


def _control_events_uri() -> str:
    settings = get_settings()
    return settings.build_delta_uri(settings.sisap_control_events_dataset)


def _local_control_state_path() -> Path:
    return get_settings().control_local_state_path


def _pending_control_state_path() -> Path:
    return get_settings().control_pending_state_path


def _local_control_events_path() -> Path:
    return get_settings().control_local_events_path


def _pending_control_events_path() -> Path:
    return get_settings().control_pending_events_path


def _control_lock_path() -> Path:
    local_path = _local_control_state_path()
    local_path.parent.mkdir(parents=True, exist_ok=True)
    return local_path.parent / '.control_state.lock.d'


def _read_lock_owner_pid(owner_path: Path) -> int | None:
    try:
        if not owner_path.exists():
            return None
        raw_value = owner_path.read_text(encoding='utf-8').strip()
        return int(raw_value) if raw_value else None
    except (OSError, ValueError):
        return None


def _process_exists(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


@contextmanager
def _file_lock(lock_path: Path, timeout_seconds: float = LOCK_TIMEOUT_SECONDS):
    start = time.monotonic()
    stale_lock_threshold = timeout_seconds * 0.75  # More aggressive stale detection
    retry_count = 0
    
    while True:
        try:
            lock_path.mkdir(exist_ok=False)
            break
        except FileExistsError:
            retry_count += 1
            try:
                lock_age_seconds = time.time() - lock_path.stat().st_mtime
                owner_pid = _read_lock_owner_pid(lock_path / 'owner.txt')
                
                # Check if lock is stale (old or orphaned process)
                is_orphaned = owner_pid is not None and not _process_exists(owner_pid)
                is_too_old = lock_age_seconds > stale_lock_threshold
                
                if is_orphaned or is_too_old:
                    reason = 'orphaned' if is_orphaned else f'too old ({lock_age_seconds:.1f}s)'
                    logger.warning(
                        'Detectado lock {} en {} con pid {} (intento #{}).',
                        reason,
                        lock_path,
                        owner_pid,
                        retry_count,
                    )
                    import shutil
                    shutil.rmtree(lock_path, ignore_errors=True)
                    continue
            except OSError as e:
                logger.debug(f'Error al revisar lock en {lock_path}: {e}')
                pass

            elapsed = time.monotonic() - start
            if elapsed >= timeout_seconds:
                raise TimeoutError(f'No se pudo adquirir el lock de control en {lock_path} después de {elapsed:.1f}s.')
            time.sleep(LOCK_POLL_SECONDS)

    owner_path = lock_path / 'owner.txt'
    try:
        owner_path.write_text(str(os.getpid()), encoding='utf-8')
        yield
    finally:
        try:
            owner_path.unlink(missing_ok=True)
        except OSError:
            pass
        try:
            lock_path.rmdir()
        except OSError:
            pass


@contextmanager
def _control_guard():
    with CONTROL_STATE_LOCK:
        with _file_lock(_control_lock_path()):
            yield


def _normalize_control_frame(frame: pl.DataFrame) -> pl.DataFrame:
    if frame.is_empty():
        return frame

    expressions: list[pl.Expr] = []
    for column in CONTROL_STRING_COLUMNS:
        if column in frame.columns:
            expressions.append(pl.col(column).cast(pl.Utf8, strict=False).fill_null(''))
    for column in CONTROL_BOOL_COLUMNS:
        if column in frame.columns:
            expressions.append(pl.col(column).cast(pl.Boolean, strict=False).fill_null(False))
    for column in CONTROL_DATE_COLUMNS:
        if column in frame.columns:
            expressions.append(pl.col(column).cast(pl.Date, strict=False))
    for column in CONTROL_DATETIME_COLUMNS:
        if column in frame.columns:
            expressions.append(pl.col(column).cast(pl.Datetime(time_unit='us'), strict=False))
    return frame.with_columns(expressions)


def _align_control_keys_columns(frame: pl.DataFrame) -> pl.DataFrame:
    """Registros legacy sin mercado: columnas requeridas por CONTROL_KEY_COLUMNS."""
    if frame.is_empty():
        return frame
    out = frame
    if 'mercado_codigo' not in out.columns:
        out = out.with_columns(pl.lit('').alias('mercado_codigo'))
    if 'mercado_nombre' not in out.columns:
        out = out.with_columns(pl.lit('').alias('mercado_nombre'))
    return out


def _merge_control_frames(*frames: pl.DataFrame) -> pl.DataFrame:
    normalized = [_normalize_control_frame(frame) for frame in frames if not frame.is_empty()]
    if not normalized:
        return pl.DataFrame()
    merged = pl.concat(normalized, how='diagonal_relaxed')
    merged = _align_control_keys_columns(merged)
    if 'fecha_actualizacion' not in merged.columns:
        return merged
    return merged.sort('fecha_actualizacion').unique(subset=CONTROL_KEY_COLUMNS, keep='last')


def _append_event_frames(*frames: pl.DataFrame) -> pl.DataFrame:
    normalized = [_normalize_control_frame(frame) for frame in frames if not frame.is_empty()]
    if not normalized:
        return pl.DataFrame()
    merged = pl.concat(normalized, how='diagonal_relaxed')
    if 'fecha_actualizacion' in merged.columns:
        merged = merged.sort('fecha_actualizacion')
    if 'evento_id' in merged.columns:
        merged = merged.unique(subset=CONTROL_EVENT_KEY_COLUMNS, keep='last')
    return merged


def _quote_identifier(column: str) -> str:
    escaped = column.replace("`", "``")
    return f"`{escaped}`"


def _control_merge_predicate(columns: list[str]) -> str:
    keys = [column for column in CONTROL_KEY_COLUMNS if column in columns]
    if len(keys) != len(CONTROL_KEY_COLUMNS):
        missing = [column for column in CONTROL_KEY_COLUMNS if column not in columns]
        raise ValueError(f'Llaves de control incompletas para merge: {missing}')
    return " AND ".join(f"target.{column} = source.{column}" for column in keys)


def _control_change_predicate(columns: list[str]) -> str | None:
    comparable_columns = [column for column in columns if column not in CONTROL_KEY_COLUMNS]
    if not comparable_columns:
        return None

    comparisons: list[str] = []
    for column in comparable_columns:
        quoted = _quote_identifier(column)
        comparisons.append(
            "("
            f"(target.{quoted} IS NULL AND source.{quoted} IS NOT NULL) OR "
            f"(target.{quoted} IS NOT NULL AND source.{quoted} IS NULL) OR "
            f"(target.{quoted} != source.{quoted})"
            ")"
        )
    return " OR ".join(comparisons)


def _quarantine_corrupt_file(file_path: Path) -> None:
    if not file_path.exists():
        return
    backup_path = file_path.with_suffix(file_path.suffix + f'.corrupt_{datetime.now().strftime("%Y%m%d%H%M%S%f")}')
    try:
        file_path.replace(backup_path)
        logger.warning('Se movio el parquet corrupto de control a {}', backup_path)
    except OSError:
        logger.exception('No se pudo aislar el parquet corrupto {}', file_path)


def _read_parquet_safe(file_path: Path, error_message: str) -> pl.DataFrame:
    if not file_path.exists():
        return pl.DataFrame()
    try:
        return _normalize_control_frame(pl.read_parquet(file_path))
    except Exception:
        logger.exception(error_message, file_path)
        _quarantine_corrupt_file(file_path)
        return pl.DataFrame()


def _write_parquet_atomic(frame: pl.DataFrame, file_path: Path, keep_empty_file: bool = False) -> None:
    if frame.is_empty() and not keep_empty_file:
        if file_path.exists():
            file_path.unlink(missing_ok=True)
        return

    file_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = file_path.with_suffix(file_path.suffix + f'.tmp_{os.getpid()}_{time.time_ns()}')
    frame.write_parquet(temp_path)
    temp_path.replace(file_path)


def _read_local_control_state() -> pl.DataFrame:
    return _read_parquet_safe(_local_control_state_path(), 'No se pudo leer el cache local de control en {}')


def _write_local_control_state(control_df: pl.DataFrame) -> None:
    if control_df.is_empty():
        return
    _write_parquet_atomic(control_df, _local_control_state_path(), keep_empty_file=False)


def _read_pending_control_state() -> pl.DataFrame:
    return _read_parquet_safe(_pending_control_state_path(), 'No se pudo leer la cola pendiente de control en {}')


def _write_pending_control_state(control_df: pl.DataFrame) -> None:
    _write_parquet_atomic(control_df, _pending_control_state_path(), keep_empty_file=False)


def _read_local_control_events() -> pl.DataFrame:
    return _read_parquet_safe(_local_control_events_path(), 'No se pudo leer el journal local de eventos en {}')


def _write_local_control_events(events_df: pl.DataFrame) -> None:
    if events_df.is_empty():
        return
    _write_parquet_atomic(events_df, _local_control_events_path(), keep_empty_file=False)


def _read_pending_control_events() -> pl.DataFrame:
    return _read_parquet_safe(_pending_control_events_path(), 'No se pudo leer la cola pendiente de eventos de control en {}')


def _write_pending_control_events(events_df: pl.DataFrame) -> None:
    _write_parquet_atomic(events_df, _pending_control_events_path(), keep_empty_file=False)


def _build_control_filter_expr(
    fuente: str,
    modulo: str,
    dataset: str,
    scope_tipo: str,
    scope_valor: str,
    producto_codigo: str,
    mercado_codigo: str | None = None,
) -> pl.Expr:
    mc = mercado_codigo if mercado_codigo is not None else ''
    return (
        (pl.col('fuente') == fuente)
        & (pl.col('modulo') == modulo)
        & (pl.col('dataset') == dataset)
        & (pl.col('scope_tipo') == scope_tipo)
        & (pl.col('scope_valor') == scope_valor)
        & (pl.col('producto_codigo') == producto_codigo)
        & (pl.col('mercado_codigo').fill_null('') == mc)
    )


def _filter_control_frame(
    frame: pl.DataFrame,
    fuente: str,
    modulo: str,
    dataset: str,
    scope_tipo: str,
    scope_valor: str,
    producto_codigo: str,
    mercado_codigo: str | None = None,
) -> pl.DataFrame:
    frame = _ensure_control_mercado_columns(frame)
    if frame.is_empty():
        return frame
    return frame.filter(
        _build_control_filter_expr(
            fuente,
            modulo,
            dataset,
            scope_tipo,
            scope_valor,
            producto_codigo,
            mercado_codigo,
        )
    )


def _read_remote_control_state(
    fuente: str | None = None,
    modulo: str | None = None,
    dataset: str | None = None,
    scope_tipo: str | None = None,
    scope_valor: str | None = None,
    producto_codigo: str | None = None,
    mercado_codigo: str | None = None,
) -> pl.DataFrame:
    settings = get_settings()
    table_uri = _control_uri()
    storage_options = settings.delta_storage_options

    scan = pl.scan_delta(table_uri, storage_options=storage_options)
    schema_names = set(scan.collect_schema().names())

    lf = scan
    if fuente is not None:
        lf = lf.filter(pl.col('fuente') == fuente)
    if modulo is not None:
        lf = lf.filter(pl.col('modulo') == modulo)
    if dataset is not None:
        lf = lf.filter(pl.col('dataset') == dataset)
    if scope_tipo is not None:
        lf = lf.filter(pl.col('scope_tipo') == scope_tipo)
    if scope_valor is not None:
        lf = lf.filter(pl.col('scope_valor') == scope_valor)
    if producto_codigo is not None:
        lf = lf.filter(pl.col('producto_codigo') == producto_codigo)
    if mercado_codigo is not None:
        mc = mercado_codigo or ''
        if 'mercado_codigo' in schema_names:
            lf = lf.filter(pl.col('mercado_codigo').fill_null('') == mc)
        elif mc:
            return pl.DataFrame()

    remote_state = _normalize_control_frame(lf.collect())
    return _align_control_keys_columns(remote_state)


def read_control_table() -> pl.DataFrame:
    with _control_guard():
        local_state = _read_local_control_state()
        pending_state = _read_pending_control_state()

    try:
        remote_state = _read_remote_control_state()
        merged_state = _merge_control_frames(remote_state, local_state, pending_state)
        if not merged_state.is_empty():
            with _control_guard():
                _write_local_control_state(merged_state)
        return merged_state
    except Exception:
        return _merge_control_frames(local_state, pending_state)


def _ensure_control_mercado_columns(control_df: pl.DataFrame) -> pl.DataFrame:
    if control_df.is_empty():
        return control_df
    out = control_df
    if 'mercado_codigo' not in out.columns:
        out = out.with_columns(pl.lit('').alias('mercado_codigo'))
    if 'mercado_nombre' not in out.columns:
        out = out.with_columns(pl.lit('').alias('mercado_nombre'))
    return out


def get_last_successful_date(
    fuente: str,
    modulo: str,
    dataset: str,
    scope_tipo: str,
    scope_valor: str,
    producto_codigo: str,
    mercado_codigo: str | None = None,
) -> object | None:
    with _control_guard():
        local_state = _read_local_control_state()
        pending_state = _read_pending_control_state()
    try:
        remote_state = _read_remote_control_state(
            fuente=fuente,
            modulo=modulo,
            dataset=dataset,
            scope_tipo=scope_tipo,
            scope_valor=scope_valor,
            producto_codigo=producto_codigo,
            mercado_codigo=mercado_codigo,
        )
    except Exception:
        remote_state = pl.DataFrame()

    filtered = _merge_control_frames(remote_state, local_state, pending_state).filter(
        _build_control_filter_expr(
            fuente,
            modulo,
            dataset,
            scope_tipo,
            scope_valor,
            producto_codigo,
            mercado_codigo,
        )
        & (pl.col('ultima_fecha_exitosa').is_not_null())
    )
    if filtered.is_empty():
        return None
    return filtered.sort('fecha_actualizacion').get_column('ultima_fecha_exitosa').tail(1).item()


def get_control_status(
    fuente: str,
    modulo: str,
    dataset: str,
    scope_tipo: str,
    scope_valor: str,
    producto_codigo: str,
    mercado_codigo: str | None = None,
) -> dict[str, object] | None:
    with _control_guard():
        local_state = _read_local_control_state()
        pending_state = _read_pending_control_state()
    try:
        remote_state = _read_remote_control_state(
            fuente=fuente,
            modulo=modulo,
            dataset=dataset,
            scope_tipo=scope_tipo,
            scope_valor=scope_valor,
            producto_codigo=producto_codigo,
            mercado_codigo=mercado_codigo,
        )
    except Exception:
        remote_state = pl.DataFrame()

    filtered = _merge_control_frames(remote_state, local_state, pending_state).filter(
        _build_control_filter_expr(
            fuente,
            modulo,
            dataset,
            scope_tipo,
            scope_valor,
            producto_codigo,
            mercado_codigo,
        )
    )
    if filtered.is_empty():
        return None

    latest = filtered.sort('fecha_actualizacion').tail(1).to_dicts()
    return latest[0] if latest else None


def upsert_control_records(records_df: pl.DataFrame) -> str:
    with _control_guard():
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
                with get_delta_lock(table_uri):
                    _, write_deltalake = get_delta_runtime()
                    write_deltalake(
                        table_uri,
                        merged_local_state.to_arrow(),
                        mode='overwrite',
                        storage_options=storage_options,
                    )
            _write_pending_control_state(pl.DataFrame())
            return table_uri

        try:
            with get_delta_lock(table_uri):
                DeltaTable, write_deltalake = get_delta_runtime()
                sync_df = _align_control_keys_columns(_merge_control_frames(pending_state, incoming_df))
                if sync_df.is_empty():
                    return table_uri
                try:
                    existing_table = DeltaTable(table_uri, storage_options=storage_options)
                except Exception:
                    existing_table = None

                if existing_table is None:
                    write_deltalake(
                        table_uri,
                        sync_df.to_arrow(),
                        mode='overwrite',
                        schema_mode='merge',
                        engine='rust',
                        storage_options=storage_options,
                    )
                else:
                    merge_predicate = _control_merge_predicate(sync_df.columns)
                    change_predicate = _control_change_predicate(sync_df.columns)
                    merge_builder = existing_table.merge(
                        source=sync_df.to_arrow(),
                        predicate=merge_predicate,
                        source_alias='source',
                        target_alias='target',
                    )
                    if change_predicate:
                        merge_builder = merge_builder.when_matched_update_all(
                            predicate=change_predicate
                        )
                    merge_builder.when_not_matched_insert_all().execute()
            _write_pending_control_state(pl.DataFrame())
            _write_local_control_state(merged_local_state)
            return table_uri
        except Exception:
            logger.exception('No se pudo sincronizar control con MinIO; se conserva en cache local.')
            merged_pending_state = _merge_control_frames(pending_state, incoming_df)
            _write_pending_control_state(merged_pending_state)
            return str(_pending_control_state_path())


def sync_pending_control_state() -> dict[str, object]:
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
    lock_start = time.monotonic()
    try:
        with _control_guard():
            lock_acquired = time.monotonic()
            lock_wait = lock_acquired - lock_start
            if lock_wait > 1.0:  # Log if lock took >1 second to acquire
                logger.debug(f'Control lock acquired after {lock_wait:.2f}s')
            
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
                    with get_delta_lock(events_uri):
                        _, write_deltalake = get_delta_runtime()
                        write_deltalake(
                            events_uri,
                            incoming_events.to_arrow(),
                            mode='append',
                            schema_mode='merge',
                            engine='rust',
                            storage_options=settings.delta_storage_options,
                        )
                _write_pending_control_events(pl.DataFrame())
                return events_uri

            events_to_sync = _append_event_frames(pending_events, incoming_events)
            if events_to_sync.is_empty():
                return events_uri

            try:
                with get_delta_lock(events_uri):
                    _, write_deltalake = get_delta_runtime()
                    write_deltalake(
                        events_uri,
                        events_to_sync.to_arrow(),
                        mode='append',
                        schema_mode='merge',
                        engine='rust',
                        storage_options=settings.delta_storage_options,
                    )
                _write_pending_control_events(pl.DataFrame())
                return events_uri
            except Exception:
                logger.exception('No se pudo sincronizar el journal de eventos de control con MinIO; se conserva en cache local.')
                _write_pending_control_events(events_to_sync)
    finally:
        total_time = time.monotonic() - lock_start
        if total_time > 5.0:  # Log if entire operation took >5 seconds
            logger.debug(f'Control events write operation took {total_time:.2f}s total')


def sync_pending_control_events() -> dict[str, object]:
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
    with _control_guard():
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
