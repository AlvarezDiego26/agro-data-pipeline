# MIDAGRI Comercio Exterior Agrario

Pipeline liviano para descargar, controlar, extraer y normalizar los cuadros de comercio exterior agrario publicados por MIDAGRI.

## Flujo

1. `remote_scan`: descubre archivos remotos `.xlsx`, `.xls` y `.zip`.
2. `download`: descarga solo versiones remotas nuevas.
3. `import`: guarda el binario original y procesa el archivo si su hash no fue importado antes.
4. `raw_archivos`: conserva el archivo fuente original.
5. `base_comercio_exterior`: persiste la estructura celda por celda por hoja.
6. `inventario_hojas_comercio_exterior`: clasifica hojas y encabezados detectados.
7. `comercio_exterior_agrario`: capa analitica normalizada.

## Salidas

- `raw_archivos`
- `base_comercio_exterior`
- `inventario_hojas_comercio_exterior`
- `comercio_exterior_agrario`
- `control_state.parquet`
- `control_events_local.parquet`

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
