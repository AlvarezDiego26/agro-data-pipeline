# SUNAT Light Pipeline

Pipeline ligero para archivos SUNAT orientado a exportaciones agrarias frescas, con control de ingesta y operacion automatizable.

## Objetivo
- recibir archivos fuente en `data/inbox/sunat`
- consolidar la base principal de exportaciones desde `zip/dbf` sin perder archivos ya importados
- filtrar solo registros agrarios frescos utiles
- homologar productos con IDs compartidos con SISAP
- conservar productos agrarios nuevos con IDs propios estables
- calcular `precio_fob_usd_por_kg`
- guardar la salida final en Parquet y Delta
- dejar el flujo listo para automatizacion posterior

## Estructura
- `src/sunat_file/catalogs/`: catalogos de productos y territorio
- `src/sunat_file/readers/`: lectura de archivos fuente
- `src/sunat_file/transformers/`: normalizacion y filtrado
- `src/sunat_file/storage/`: parquet, delta y control
- `src/sunat_file/jobs/`: importacion, filtrado y orquestacion
- `src/sunat_file/cli.py`: punto de entrada
- `scripts/run_sunat_pipeline.ps1`: wrapper para ejecucion automatica

## Flujo operativo
1. `scan-inbox`
   - detecta archivos nuevos en `data/inbox/sunat`
2. `run-import`
   - importa `zip/dbf`
   - consolida `sunat_exportaciones_base.parquet` acumulando nuevos archivos
   - mueve archivos procesados a `data/processed/sunat`
   - mueve fallidos a `data/error/sunat`
3. `run-filter-fresh`
   - filtra exportaciones agrarias frescas
   - genera `sunat_exportaciones_agrarias_frescas.parquet`
   - actualiza Delta por rango procesado
   - genera archivos de revision
4. `run-main`
   - ejecuta todo el flujo de extremo a extremo

## Modos de trabajo
- `backfill`: reproceso historico
- `incremental`: continua desde la ultima fecha exitosa registrada
- `manual`: usa exactamente el rango solicitado

## Defaults orientados a automatizacion
- `SUNAT_MODO_CARGA=incremental`
- `SUNAT_FECHA_CORTE_INICIO=2016-01-01`
- `SUNAT_USE_CONTROL_TABLE=true`

## Control de ingesta
SUNAT ahora mantiene dos niveles de control:

- `control/ingesta_control`: snapshot resumido por dataset y modulo
- `control/ingesta_control_eventos`: journal de eventos por importacion y filtrado

Esto permite registrar:
- archivos importados con exito
- archivos omitidos
- archivos con error
- la ultima fecha exitosa del dataset filtrado
- corridas de filtrado exitosas
- corridas vacias
- corridas fallidas

## Tolerancia a fallas
Si MinIO o la red fallan, el control se conserva localmente en:

- `data/control/control_state.parquet`
- `data/control/control_pending.parquet`
- `data/control/control_events_local.parquet`
- `data/control/control_events_pending.parquet`

En la siguiente ejecucion se intenta sincronizar automaticamente lo pendiente.

## Salidas principales
- `data/clean/sunat_exportaciones_base.parquet`
- `data/clean/sunat_exportaciones_agrarias_frescas.parquet`
- `data/raw/sunat_exportaciones_agrarias_frescas_raw.parquet`
- `data/clean_delta/sunat_exportaciones_agrarias_frescas`

## Archivos de revision
- `data/review/sunat_exportaciones_frescas_preview.csv`
- `data/review/sunat_exportaciones_frescas_resumen_productos.csv`
- `data/review/sunat_exportaciones_frescas_resumen_subpartidas.csv`
- `data/review/sunat_exportaciones_frescas_resumen_regiones.csv`
- `data/review/sunat_exportaciones_frescas_calidad_ubigeo.csv`
- `data/review/sunat_catalogo_productos_homologado.csv`
- `data/review/sunat_catalogo_territorial_base.csv`
- `data/review/sunat_exportaciones_frescas_diccionario.csv`

## Comando principal
Desde `ingesta-datos/sunat`:

```powershell
$env:PYTHONPATH = '.\src'
python -m sunat_file.cli run-main
```

## Wrapper principal
- `scripts/run_sunat_pipeline.ps1`

Ejemplo:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run_sunat_pipeline.ps1 -ModoCarga incremental -FechaInicio 2016-01-01
```

## Nota de arquitectura
SUNAT ya sigue la misma logica general que SISAP:

- `catalogs/readers/transformers/storage/jobs`
- control resumido
- journal de eventos
- fallback local para errores de sincronizacion
- CLI y wrapper para automatizacion
- fechas y modo de carga listos para ser enviados como parametros desde Prefect
