from datetime import date
from enum import Enum

from pydantic import BaseModel


class ModuloSisap(str, Enum):
    MAYORISTA_VOLUMEN = "mayorista_volumen"
    MAYORISTA_PRECIOS = "mayorista_precios"

    # Ambos módulos de ciudades escriben al mismo tablón Delta consolidado.
    CIUDADES_PRECIOS_MAYORISTAS = "precio_diario_regiones"
    CIUDADES_PRECIOS_MINORISTAS = "precio_diario_regiones"


class ProductoAgricola(BaseModel):
    codigo: str
    nombre: str
    categoria: str


class Procedencia(BaseModel):
    codigo: str
    nombre: str


class Region(BaseModel):
    codigo: str
    nombre: str


class QueryWindow(BaseModel):
    fecha_inicio: date
    fecha_fin: date


class SisapQuery(BaseModel):
    modulo: ModuloSisap
    producto_codigo: str
    producto_nombre: str
    fecha_inicio: date
    fecha_fin: date
    procedencia_codigo: str | None = None
    procedencia_nombre: str | None = None
    region_codigo: str | None = None
    region_nombre: str | None = None
    mercado_codigo: str | None = None
    mercado_nombre: str | None = None


class HtmlSnapshot(BaseModel):
    modulo: ModuloSisap
    nombre_archivo: str
    url: str
    query_hash: str