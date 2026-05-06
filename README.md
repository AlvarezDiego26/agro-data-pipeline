# Agro Proyecto

Repositorio principal de pipelines de ingesta para fuentes agrarias.

## Objetivo
- centralizar scripts ligeros de extraccion y transformacion
- guardar salidas tecnicas en `Parquet` y `Delta Parquet`
- dejar los pipelines listos para encapsulamiento y scheduler

## Estructura
- `ingesta-datos/`
  - `sisap/`: pipeline de volumen, precios y ciudades
  - `sunat/`: pipeline de importacion y filtrado de exportaciones agrarias frescas

## Principios del proyecto
- herramientas ligeras para servidor
- configuracion por `.env`
- wrappers listos para automatizacion
- separacion clara entre ingesta, procesamiento y ejecucion

## Entrypoints principales
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

## Notas
- no se versionan credenciales
- no se versionan archivos generados en `data/`
- cada subproyecto contiene su propia documentacion operativa
