# Ingesta De Datos

Esta carpeta agrupa los pipelines de extraccion y transformacion por fuente.

## Subproyectos
### `sisap/`
- fuente web
- volumen
- precios mayoristas
- ciudades mayoristas
- ciudades minoristas
- carga historica e incremental

### `sunat/`
- fuente por archivos
- importacion de `zip/dbf`
- filtro de exportaciones agrarias frescas
- homologacion de `producto_id` con `SISAP`

## Criterio De Organizacion
Cada fuente mantiene:
- `src/`: codigo fuente
- `scripts/`: wrappers para ejecucion y scheduler
- `.env.example`: configuracion de ejemplo
- `README.md`: documentacion propia

## Convencion De Ejecucion
- cada fuente expone un `CLI` principal
- cada fuente expone un wrapper PowerShell principal
- la orquestacion no duplica logica; solo dispara wrappers

## Convencion De Storage
El bucket usa una carpeta raiz comun:
- `Landing/`

Dentro de `Landing/` cada fuente mantiene su propio espacio:
- `Landing/sisap/`
- `Landing/sunat/`

## Integracion Con Prefect
La carpeta hermana `orquestacion-prefect/` usa estos wrappers:
- `sisap/scripts/run_sisap_pipeline.ps1`
- `sunat/scripts/run_sunat_pipeline.ps1`

Eso permite mantener la logica dentro de cada pipeline y usar `Prefect` solo para scheduling, monitoreo y reintentos.
