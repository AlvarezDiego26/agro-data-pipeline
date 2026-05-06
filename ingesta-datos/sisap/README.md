# SISAP Light Pipeline

Proyecto para la extraccion ligera de SISAP con enfoque orientado a produccion.

## Objetivo
- evitar automatizacion de navegador
- consumir HTML o requests directos del portal
- transformar con Polars
- guardar capa clean en Parquet y Delta
- dejar listas las bases para automatizacion y scheduler

## Stack
- `httpx`
- `selectolax`
- `lxml`
- `polars`
- `pyarrow`
- `deltalake`
- `pydantic`
- `pydantic-settings`
- `typer`
- `tenacity`
- `loguru`

## Estructura
- `src/sisap_light/ingesta_datos/`: catalogos, planeamiento mensual y extractores HTTP
- `src/sisap_light/procesamiento/`: parseo HTML, transformacion, validacion y escritura
- `src/sisap_light/jobs/`: jobs por modulo y runner maestro
- `scripts/`: wrappers para ejecucion manual o scheduler

## Logica operativa
- el pipeline recorre `modulo por modulo`
- en `volumen` y `precios` trabaja `procedencia por procedencia`
- en `ciudades` trabaja `region por region`
- dentro de cada bloque recorre todos los productos si no se fija uno
- el rango historico se parte por mes
- en modo `incremental` continua desde la ultima fecha cargada
- en modo `incremental` no rehace el historico completo: retoma desde la ultima fecha detectada para cada `procedencia/region + producto`

## Comando principal
Desde la raiz del proyecto:

```powershell
$env:PYTHONPATH='src'
python -m sisap_light.cli run-main
```

## Wrapper principal para automatizacion
- `scripts/run_sisap_pipeline.ps1`

Ejemplo:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run_sisap_pipeline.ps1
```

Ese wrapper ya deja listo el proyecto para scheduler porque solo necesita ejecutar un script.
Los wrappers `run_sisap_master.ps1` y `run_sisap_suite.ps1` quedan solo como compatibilidad y redirigen al wrapper principal.

## Configuracion principal
- `SISAP_MODULOS=volumen,precios,ciudades-mayoristas,ciudades-minoristas`
- `SISAP_PROCEDENCIAS=Arequipa,Lima`
- `SISAP_REGIONES=Arequipa,Lima`
- `SISAP_PAUSE_SECONDS=30`
- `SISAP_PRODUCTO_NOMBRE` o `SISAP_PRODUCTO_CODIGO`
- `SISAP_FECHA_INICIO` y `SISAP_FECHA_FIN`
- `SISAP_MODO_CARGA=backfill`
- `SISAP_MODO_CARGA=incremental`
- `SISAP_INCREMENTAL_OVERLAP_DIAS`
- `SISAP_MAX_QUERIES`

## Delta y MinIO
- `STORAGE_BACKEND=local` guarda Delta en `data/clean_delta/...`
- `STORAGE_BACKEND=minio` guarda Delta en `s3://<BUCKET_NAME>/Landing/sisap/...`
- `DELTA_ENABLED=true` activa escritura Delta

## Estructura esperada en bucket
- `<BUCKET_NAME>/Landing/sisap/volumen_diario/procedencia=<nombre>/producto=<nombre>/anio=YYYY/mes=MM/`
- `<BUCKET_NAME>/Landing/sisap/precios_diarios/procedencia=<nombre>/producto=<nombre>/anio=YYYY/mes=MM/`
- `<BUCKET_NAME>/Landing/sisap/ciudades_precios_mayoristas/region=<nombre>/producto=<nombre>/anio=YYYY/mes=MM/`
- `<BUCKET_NAME>/Landing/sisap/ciudades_precios_minoristas/region=<nombre>/producto=<nombre>/anio=YYYY/mes=MM/`

## Comandos auxiliares
```powershell
python -m sisap_light.cli run-volumen
python -m sisap_light.cli run-precios
python -m sisap_light.cli run-ciudades-mayoristas
python -m sisap_light.cli run-ciudades-minoristas
```

## Recomendacion operativa
- usar `backfill` solo para la carga historica inicial
- usar `run-main` en modo `incremental` para la automatizacion periodica
- dejar `SISAP_PRODUCTO_*` vacio si se quiere recorrer todo el catalogo por bloque geografico
