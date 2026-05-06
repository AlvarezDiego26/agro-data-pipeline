# Ingesta De Datos

Esta carpeta agrupa los pipelines de extraccion del proyecto.

## Subproyectos
- `sisap/`
  - fuente web
  - volumen
  - precios mayoristas
  - ciudades mayoristas
  - ciudades minoristas
- `sunat/`
  - fuente por archivos
  - importacion de `zip/dbf`
  - filtro de exportaciones agrarias frescas

## Criterio de organizacion
Cada fuente mantiene:
- `src/`: codigo fuente
- `scripts/`: wrappers para ejecucion y scheduler
- `.env.example`: configuracion de ejemplo
- `README.md`: documentacion propia

## Convencion de trabajo
- `SISAP` y `SUNAT` son pipelines independientes
- ambos deben poder ejecutarse desde un `CLI` principal
- el scheduler debe invocar wrappers, no reimplementar logica

## Convencion de storage
- el bucket usa una carpeta raiz comun:
  - `Landing/`
- dentro de `Landing/` cada fuente mantiene su propio espacio:
  - `Landing/sisap/`
  - `Landing/sunat/`
