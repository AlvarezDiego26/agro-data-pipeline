# MIDAGRI Comercio Exterior Agrario

Pipeline liviano para descargar, controlar, extraer y normalizar los cuadros de comercio exterior agrario publicados por MIDAGRI.

Por defecto el pipeline prioriza salidas utiles para analisis y deja apagadas las capas tecnicas pesadas.

## Flujo

1. `remote_scan`: descubre archivos remotos `.xlsx`, `.xls` y `.zip`.
2. `download`: descarga solo versiones remotas nuevas.
3. `import`: procesa el archivo si su hash no fue importado antes.
4. `catalogo_cuadros_comercio_exterior`: resume las hojas detectadas y su clasificacion.
5. `comercio_exportacion_agrario` y `comercio_importacion_agrario`: capas analíticas normalizadas e independientes.
6. Capas tecnicas opcionales:
   - `fuentes_remotas_midagri`
   - `archivos_fuente_midagri`
   - `base_comercio_exterior`

## Salidas

- `comercio_exportacion_agrario`
- `comercio_importacion_agrario`
- `catalogo_cuadros_comercio_exterior`
- `control_state.parquet`
- `control_events_local.parquet`

## Capas tecnicas opcionales

Las siguientes capas estan desactivadas por defecto para no ensuciar MinIO:

- `fuentes_remotas_midagri`
- `archivos_fuente_midagri`
- `base_comercio_exterior`

## Periodos

La capa analitica conserva:

- `periodo_texto_fuente`
- `fecha_referencia_inicio`
- `fecha_referencia_fin`
- `fecha_particion`

`fecha_particion` representa el periodo del cuadro:

- mensual: ultimo dia del mes declarado
- anual: cierre del periodo anual declarado

## CLI

- `python -m midagri_comercio_exterior.cli sync-remote`
- `python -m midagri_comercio_exterior.cli run-import`
- `python -m midagri_comercio_exterior.cli run-main`
