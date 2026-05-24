from __future__ import annotations

import math
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Callable, Generic, Iterable, TypeVar

from loguru import logger

T = TypeVar('T')
R = TypeVar('R')


@dataclass(frozen=True)
class QueryShard(Generic[T]):
    shard_id: str
    items: list[T]


def chunk_items(items: list[T], chunk_size: int) -> list[list[T]]:
    size = max(int(chunk_size or 1), 1)
    return [items[idx:idx + size] for idx in range(0, len(items), size)]


def build_grouped_shards(
    items: Iterable[T],
    *,
    group_key: Callable[[T], str],
    chunk_size: int,
    shard_prefix: str,
    max_shards: int | None = None,
) -> list[QueryShard[T]]:
    ordered_groups: dict[str, list[T]] = {}
    for item in items:
        key = group_key(item)
        ordered_groups.setdefault(key, []).append(item)

    grouped_items = list(ordered_groups.items())
    effective_chunk_size = max(int(chunk_size or 1), 1)
    if max_shards and max_shards > 0 and len(grouped_items) > max_shards:
        effective_chunk_size = max(
            effective_chunk_size,
            math.ceil(len(grouped_items) / max_shards),
        )
    grouped_chunks = chunk_items(grouped_items, effective_chunk_size)
    shards: list[QueryShard[T]] = []
    for idx, chunk in enumerate(grouped_chunks, start=1):
        shard_items: list[T] = []
        shard_keys: list[str] = []
        for key, group_items in chunk:
            shard_keys.append(key)
            shard_items.extend(group_items)
        shard_id = f'{shard_prefix}-{idx:03d}-{"_".join(shard_keys)}'
        shards.append(QueryShard(shard_id=shard_id, items=shard_items))
    return shards


def run_shards(
    shards: list[QueryShard[T]],
    worker: Callable[[QueryShard[T]], R],
    *,
    max_workers: int,
    label: str,
) -> list[R]:
    if not shards:
        return []

    workers = max(min(max_workers, len(shards)), 1)
    if workers == 1:
        return [worker(shard) for shard in shards]

    logger.info('Ejecutando {} shards en paralelo para {} con {} workers', len(shards), label, workers)
    results: list[R] = []
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix='sisap-shard') as executor:
        future_map = {
            executor.submit(worker, shard): shard
            for shard in shards
        }
        for future in as_completed(future_map):
            shard = future_map[future]
            try:
                results.append(future.result())
            except Exception:
                logger.exception('Fallo el shard {} de {}', shard.shard_id, label)
    return results
