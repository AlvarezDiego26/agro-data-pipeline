# Agro Proyecto

Repositorio principal de pipelines de ingesta y orquestacion para fuentes agrarias.

## Objetivo
- centralizar pipelines ligeros para fuentes agrarias
- extraer y transformar datos tecnicos en formatos `Parquet` y `Delta Parquet`
- almacenar datasets ordenados dentro del bucket bajo `Landing/`
- dejar la ejecucion lista para automatizacion con `Prefect`

## Estructura Del Repositorio
- `ingesta-datos/`
  - `sisap/`: pipeline de mercado mayorista y ciudades
  - `sunat/`: pipeline de exportaciones agrarias frescas
- `orquestacion-prefect/`
  - flows y scheduler para ejecutar `SISAP` y `SUNAT`

## Capas Del Sistema
### 1. Ingesta
Cada fuente tiene su propio proyecto, configuracion, wrappers y `CLI` principal.

### 2. Storage
Los datasets tecnicos se guardan en `Delta Parquet` sobre storage compatible con `S3/MinIO`.

### 3. Orquestacion
`Prefect` ejecuta los wrappers de cada pipeline, registra logs y permite programar corridas periodicas.

## Estructura Esperada En El Bucket
- `Landing/`
  - `sisap/`
  - `sunat/`

Dentro de cada fuente se guardan tablas `Delta Parquet` organizadas por dataset y sus particiones.

## Entrypoints Principales
### SISAP
```powershell
python -m sisap_light.cli run-main
```

Wrapper:
```powershell
powershell -ExecutionPolicy Bypass -File .\ingesta-datos\sisap\scripts\run_sisap_pipeline.ps1
```

### SUNAT
```powershell
python -m sunat_file.cli run-main
```

Wrapper:
```powershell
powershell -ExecutionPolicy Bypass -File .\ingesta-datos\sunat\scripts\run_sunat_pipeline.ps1
```

### Prefect
```powershell
python -m agro_orquestacion.flows agro
```

## Flujo General
1. La fuente entrega datos o responde consultas.
2. El pipeline de la fuente extrae y limpia.
3. El dataset resultante se escribe en `Delta Parquet` dentro de `Landing/<fuente>/`.
4. `Prefect` orquesta las corridas y registra estado, logs y reintentos.

## Notas De Trabajo
- no se versionan credenciales
- no se versionan archivos generados en `data/`
- `Prefect` no reemplaza la logica del pipeline; solo orquesta
- cada subproyecto contiene su propia documentacion operativa
