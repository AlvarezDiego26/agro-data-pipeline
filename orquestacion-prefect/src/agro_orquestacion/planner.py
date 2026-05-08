from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import runpy

from agro_orquestacion.config import get_settings


@dataclass(frozen=True)
class SisapWorkUnit:
    modulo: str
    scope_tipo: str
    scope_valor: str
    producto_codigo: str | None = None
    producto_nombre: str | None = None

    @property
    def instancia_id(self) -> str:
        scope_slug = self.scope_valor.strip().lower().replace(" ", "_")
        if self.producto_codigo:
            return f"{self.modulo}-{scope_slug}-{self.producto_codigo}"
        return f"{self.modulo}-{scope_slug}"


def _load_catalog_items(file_path: Path, variable_name: str) -> list[dict]:
    namespace = runpy.run_path(str(file_path))
    values = namespace.get(variable_name)
    if not isinstance(values, list):
        raise ValueError(f"No se pudo cargar el catalogo {variable_name} desde {file_path}.")
    return values


def _split_csv(raw_value: str | None) -> list[str]:
    if not raw_value:
        return []
    return [item.strip() for item in raw_value.split(",") if item.strip()]


def _is_all_keyword(values: list[str]) -> bool:
    normalized = {value.strip().lower() for value in values}
    return bool(normalized) and normalized <= {"all", "*", "todas", "todos"}


def _procedencias_catalog() -> list[dict]:
    settings = get_settings()
    return _load_catalog_items(
        settings.repo_root / "ingesta-datos" / "sisap" / "src" / "sisap_light" / "ingesta_datos" / "catalogos" / "procedencias.py",
        "PROCEDENCIAS_SISAP",
    )


def _productos_catalog() -> list[dict]:
    settings = get_settings()
    return _load_catalog_items(
        settings.repo_root / "ingesta-datos" / "sisap" / "src" / "sisap_light" / "ingesta_datos" / "catalogos" / "productos.py",
        "PRODUCTOS_AGRICOLAS_PRIORITARIOS",
    )


def _resolve_scopes(raw_value: str | None) -> list[str]:
    requested = _split_csv(raw_value)
    if requested and not _is_all_keyword(requested):
        return requested
    return [
        item["nombre"]
        for item in _procedencias_catalog()
        if item["nombre"].strip().lower() != "desconocida"
    ]


def _resolve_products(raw_value: str | None) -> list[dict]:
    catalog = _productos_catalog()
    requested = _split_csv(raw_value)
    if not requested or _is_all_keyword(requested):
        return catalog

    resolved: list[dict] = []
    requested_set = {item.strip().lower() for item in requested}
    for producto in catalog:
        codigo = str(producto["codigo"]).strip().lower()
        nombre = str(producto["nombre"]).strip().lower()
        if codigo in requested_set or nombre in requested_set:
            resolved.append(producto)
    if not resolved:
        raise ValueError("No se pudo resolver la lista de productos solicitada para SISAP.")
    return resolved


def build_sisap_work_units(
    estrategia: str,
    modulos: str | None,
    procedencias: str | None,
    regiones: str | None,
    productos: str | None,
) -> list[SisapWorkUnit]:
    modules = _split_csv(modulos) if modulos else get_settings().sisap_modulos.split(",")
    strategy = (estrategia or "por_scope").strip().lower()
    work_units: list[SisapWorkUnit] = []

    scope_map = {
        "volumen": ("procedencias", _resolve_scopes(procedencias)),
        "precios": ("procedencias", _resolve_scopes(procedencias)),
        "ciudades-mayoristas": ("regiones", _resolve_scopes(regiones)),
        "ciudades-minoristas": ("regiones", _resolve_scopes(regiones)),
    }

    if strategy == "por_scope":
        for modulo in modules:
            scope_tipo, scope_values = scope_map[modulo]
            for scope_value in scope_values:
                work_units.append(
                    SisapWorkUnit(
                        modulo=modulo,
                        scope_tipo=scope_tipo,
                        scope_valor=scope_value,
                    )
                )
        return work_units

    resolved_products = _resolve_products(productos)
    if strategy == "por_producto":
        for modulo in modules:
            scope_tipo, scope_values = scope_map[modulo]
            scope_value = scope_values[0]
            for producto in resolved_products:
                work_units.append(
                    SisapWorkUnit(
                        modulo=modulo,
                        scope_tipo=scope_tipo,
                        scope_valor=scope_value,
                        producto_codigo=str(producto["codigo"]),
                        producto_nombre=str(producto["nombre"]),
                    )
                )
        return work_units

    if strategy == "por_producto_scope":
        for modulo in modules:
            scope_tipo, scope_values = scope_map[modulo]
            for scope_value in scope_values:
                for producto in resolved_products:
                    work_units.append(
                        SisapWorkUnit(
                            modulo=modulo,
                            scope_tipo=scope_tipo,
                            scope_valor=scope_value,
                            producto_codigo=str(producto["codigo"]),
                            producto_nombre=str(producto["nombre"]),
                        )
                    )
        return work_units

    raise ValueError(
        "Estrategia de instanciacion no soportada. Usa por_scope, por_producto o por_producto_scope."
    )
