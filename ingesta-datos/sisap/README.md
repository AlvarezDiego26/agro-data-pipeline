# SISAP Light Pipeline

Pipeline ligero para extraer SISAP sin automatizacion de navegador, persistir la data por particiones y dejar el flujo listo para operacion automatica.

## Objetivo
- consumir SISAP mediante requests HTTP y parseo HTML
- transformar y validar con Polars
- guardar los datasets finales en Delta/Parquet segun backend y configuracion
- soportar control de ingesta, reintentos y continuidad incremental
- dejar una base simple para encapsular luego en Prefect

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
- `src/sisap_light/procesamiento/storage/control.py`: snapshot de control y journal de eventos
- `src/sisap_light/jobs/`: jobs por modulo y runner maestro
- `scripts/`: wrappers para ejecucion manual, scheduler o futura orquestacion

## Logica operativa
- el pipeline recorre `modulo por modulo`
- `volumen` y `precios` trabajan `procedencia por procedencia`
- `ciudades` trabaja `region por region`
- dentro de cada bloque recorre todos los productos si no se fija uno
- el rango historico se parte por mes
- cada consulta mensual persiste su data apenas sale bien
- cada consulta registra su evento de control inmediatamente
- cada consulta actualiza el snapshot de control inmediatamente
- en modo `incremental` retoma desde la ultima fecha exitosa registrada por `scope + producto`

## Modos de trabajo
- `backfill`: carga historica inicial
- `incremental`: retoma desde la ultima fecha exitosa y avanza automaticamente
- `manual`: usa exactamente el rango solicitado sin inferir continuidad

## Defaults orientados a automatizacion
La configuracion por defecto ya esta pensada para ejecucion automatica:

- `SISAP_MODO_CARGA=incremental`
- `SISAP_MODULOS=volumen,precios,regiones`
- `SISAP_PROCEDENCIAS=all`
- `SISAP_REGIONES=all`
- `SISAP_FECHA_INICIO=2016-01-01`

Con esos defaults, una ejecucion sin filtros ya recorre todo el universo SISAP y continua desde control.

## Comando principal
Desde la raiz del proyecto:

```powershell
$env:PYTHONPATH='src'
python -m sisap_light.cli run-main
```

## Wrapper principal
- `scripts/run_sisap_pipeline.ps1`

Ejemplo:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run_sisap_pipeline.ps1
```

Los wrappers `run_sisap_master.ps1` y `run_sisap_suite.ps1` se mantienen solo por compatibilidad.

## Configuracion principal
- `SISAP_MODULOS`
- `SISAP_PROCEDENCIAS`
- `SISAP_REGIONES`
- `SISAP_PRODUCTO_NOMBRE`
- `SISAP_PRODUCTO_CODIGO`
- `SISAP_FECHA_INICIO`
- `SISAP_FECHA_FIN`
- `SISAP_MODO_CARGA`
- `SISAP_INCREMENTAL_OVERLAP_DIAS`
- `SISAP_MAX_QUERIES`
- `SISAP_PAUSE_SECONDS`

## Delta y MinIO
- `STORAGE_BACKEND=local` escribe datasets de trabajo en `data/clean/...` y Delta en `data/clean_delta/...`
- `STORAGE_BACKEND=minio` escribe datasets finales directamente en `s3://<BUCKET>/<PREFIX>/...`
- `DELTA_ENABLED=true` guarda los datasets SISAP y las tablas de control como Delta Lake
- `DELTA_ENABLED=false` deja los datasets finales en parquet particionado y mantiene control local

En MinIO, con `DELTA_ENABLED=true`, cada dataset queda como tabla Delta con `_delta_log` y archivos `.parquet` particionados por `anio/mes`.

## Estructura esperada en bucket
- `<BUCKET_NAME>/Landing/sisap/volumen_diario_mercado_lima/procedencia=<nombre>/producto=<nombre>/anio=YYYY/mes=MM/`
- `<BUCKET_NAME>/Landing/sisap/precios_diarios_mercado_lima/procedencia=<nombre>/producto=<nombre>/anio=YYYY/mes=MM/`
- `<BUCKET_NAME>/Landing/sisap/precio_diario_regiones/region=<nombre>/producto=<nombre>/anio=YYYY/mes=MM/`
- `<BUCKET_NAME>/Landing/sisap/control/ingesta_control/`
- `<BUCKET_NAME>/Landing/sisap/control/ingesta_control_eventos/`

## Control de ingesta
El control tiene dos niveles:

- `control/ingesta_control`: estado resumido por modulo, scope y producto
- `control/ingesta_control_eventos`: journal detallado por consulta ejecutada

Esto permite saber:
- hasta que fecha llego cada producto
- que consultas salieron vacias
- que consultas fallaron
- si una corrida quedo parcial
- que una segunda corrida incremental retome desde el siguiente rango pendiente en vez de duplicar consultas ya exitosas

## Tolerancia a fallas
Si MinIO o la red no estan disponibles, el pipeline conserva control localmente en:

- `data/control/control_state.parquet`
- `data/control/control_pending.parquet`
- `data/control/control_events_local.parquet`
- `data/control/control_events_pending.parquet`

En la siguiente ejecucion intenta sincronizar automaticamente lo pendiente.

Para un despliegue real con Prefect o nube, conviene que `data/control` viva en almacenamiento persistente del worker para no perder el estado local en reinicios duros.

## Comandos auxiliares
```powershell
python -m sisap_light.cli run-volumen
python -m sisap_light.cli run-precios
python -m sisap_light.cli run-ciudades-mayoristas
python -m sisap_light.cli run-ciudades-minoristas
```

## Flujo recomendado
1. Ejecutar `backfill` una sola vez para poblar el historico.
2. Validar que el bucket y las tablas de control quedaron consistentes.
3. Pasar a `incremental` como modo permanente.
4. Encapsular `run_sisap_pipeline.ps1` en Prefect sin tener que cambiar codigo ni filtros manuales.
