# Benchmark SISAP 2026-05-24

## Objetivo

Validar el comportamiento de la extraccion historica de `2` anios despues de:

- corregir la continuidad incremental para vacios recientes
- desacoplar la escritura Delta final usando staging local por shard

## Configuracion ejecutada

Prueba lanzada dentro de `prefect-worker` con estos overrides:

```env
PYTHONPATH=src
APP_ENV=local
STORAGE_BACKEND=minio
DELTA_ENABLED=true
MINIO_PREFIX=Landing/sisap_benchmark_codex_20260524
SISAP_USE_CONTROL_TABLE=true
SISAP_MODULOS=volumen
SISAP_PROCEDENCIAS=Amazonas
SISAP_MERCADO_CODIGO=15011501
SISAP_MAX_PRODUCTOS=3
SISAP_MAX_SCOPES=1
SISAP_FECHA_INICIO=2024-01-01
SISAP_FECHA_FIN=2025-12-31
SISAP_MODO_CARGA=manual
SISAP_SCOPE_MAX_WORKERS=1
SISAP_SHARD_MAX_WORKERS=3
SISAP_PRODUCT_BATCH_SIZE=1
SISAP_TARGET_SHARDS_PER_SCOPE=3
SISAP_SAVE_DEBUG_HTML=false
```

## Resultado observado

- Duracion total observada por Codex: `78.4 s`
- Dataset probado: `volumen_diario_mercado_lima`
- Scope: `procedencia=Amazonas`
- Mercado: `15011501`
- Productos en paralelo: `3`
- Filas consolidadas al final: `2193`
- Ruta Delta generada:
  `s3://agro-productos/Landing/sisap_benchmark_codex_20260524/volumen_diario_mercado_lima`

## Evidencia clave

Durante la ejecucion se observaron estos eventos:

- `05:15:51` se levantan `3` shards en paralelo
- `05:15:58` se escribe un archivo staged del producto `1001`
- `05:16:02` se escribe un archivo staged del producto `0203`
- `05:16:03` se escribe un archivo staged del producto `0202`
- `05:16:07` comienza la consolidacion final de `3` archivos staged
- `05:16:10` termina la escritura Delta final

## Conclusiones

- La extraccion HTTP si estuvo trabajando en paralelo por producto.
- Ya no se vio un `merge Delta` por cada shard durante la extraccion.
- La escritura a MinIO quedo reducida a un solo `Delta overwrite` final para toda la corrida.
- El cuello de botella principal ya no fue una cola de merges intermedios, sino la extraccion misma y la consolidacion unica del cierre.

## Hallazgos funcionales

- El producto `1001` siguio devolviendo HTML sin data materializable y se normalizo con filas en `0`.
- Los productos `0202` y `0203` devolvieron tablas HTML con fechas y estructura, pero sin datos utiles; tambien se normalizaron.
- Esto confirma que el pipeline pudo completar el historico sin devolverse ni quedarse atrapado en fechas vacias antiguas.
