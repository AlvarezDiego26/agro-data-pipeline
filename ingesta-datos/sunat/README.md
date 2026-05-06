# sunat

Pipeline liviano para archivos SUNAT orientado a exportaciones agrarias frescas.

## Objetivo
- recibir archivos fuente en `data/inbox/sunat`
- consolidar la base principal de exportaciones desde `zip/dbf`
- filtrar solo registros agrarios frescos utiles
- homologar productos con IDs compartidos con SISAP
- conservar productos agrarios nuevos con IDs propios estables
- calcular precio FOB en USD por kilogramo
- guardar una sola salida final en `Parquet` y su tabla `Delta`
- escribir las tablas en `Landing/sunat/` dentro del bucket

## Flujo final
1. `inbox`
   - hoy la alimentacion es manual
   - se colocan los archivos nuevos en `data/inbox/sunat`
2. `run-main`
   - importa `zip/dbf`
   - consolida `sunat_exportaciones_base.parquet`
   - filtra solo exportaciones agrarias frescas
   - genera la salida final `sunat_exportaciones_agrarias_frescas.parquet`
   - actualiza `Delta`
   - genera archivos de revision

## Criterio final de la fuente
- solo se conserva lo que sirve para analisis agricola y estacional
- se priorizan frutas y hortalizas frescas o enfriadas
- se excluyen procesados, secos, congelados y derivados industriales
- si el producto ya existe en SISAP, se respeta su `producto_id`
- si el producto es nuevo y agrario fresco, recibe un ID estable nuevo

## Salidas principales
- base fuente consolidada:
  - `data/clean/sunat_exportaciones_base.parquet`
- salida final limpia:
  - `data/clean/sunat_exportaciones_agrarias_frescas.parquet`
- raw final:
  - `data/raw/sunat_exportaciones_agrarias_frescas_raw.parquet`
- delta final:
  - `data/clean_delta/sunat_exportaciones_agrarias_frescas`

## Estructura esperada en bucket
- `<BUCKET_NAME>/Landing/sunat/sunat_exportaciones_base`
- `<BUCKET_NAME>/Landing/sunat/sunat_exportaciones_agrarias_frescas`

## Archivos de revision
- `data/review/sunat_exportaciones_frescas_preview.csv`
- `data/review/sunat_exportaciones_frescas_resumen_productos.csv`
- `data/review/sunat_exportaciones_frescas_resumen_subpartidas.csv`
- `data/review/sunat_exportaciones_frescas_resumen_regiones.csv`
- `data/review/sunat_exportaciones_frescas_calidad_ubigeo.csv`
- `data/review/sunat_catalogo_productos_homologado.csv`
- `data/review/sunat_catalogo_territorial_base.csv`
- `data/review/sunat_exportaciones_frescas_diccionario.csv`

## Comandos
Desde `ingesta-datos/sunat`:

```powershell
$env:PYTHONPATH = '.\src'
python -m sunat_file.cli run-main
```

Comandos auxiliares:

```powershell
python -m sunat_file.cli scan-inbox
python -m sunat_file.cli run-import
python -m sunat_file.cli run-filter-fresh
```

## Wrapper para automatizacion
- `scripts/run_sunat_pipeline.ps1`

Ese wrapper ya deja listo el proyecto para scheduler porque solo necesita ejecutar un comando.

## Nota sobre precios
La columna final util es `precio_fob_usd_por_kg`.
No se convierte a soles dentro del pipeline porque eso requeriria una fuente externa de tipo de cambio y no conviene inventar conversiones dentro de esta etapa.
