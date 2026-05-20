from hashlib import md5
from pathlib import Path

from sisap_light.config import get_settings
from sisap_light.schemas import ModuloSisap, SisapQuery


def build_query_hash(query: SisapQuery) -> str:
    payload = query.model_dump_json().encode("utf-8")
    return md5(payload).hexdigest()


def save_html_snapshot(
    modulo: ModuloSisap,
    query: SisapQuery,
    html: str,
    suffix: str | None = None,
) -> Path | None:
    settings = get_settings()
    if not settings.sisap_save_debug_html:
        return None
    folder = settings.raw_html_dir / modulo.value
    folder.mkdir(parents=True, exist_ok=True)
    query_hash = build_query_hash(query)
    filename = f"{query_hash}_{suffix}.html" if suffix else f"{query_hash}.html"
    path = folder / filename
    path.write_text(html, encoding="utf-8")
    return path

