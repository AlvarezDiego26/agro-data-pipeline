# Pipeline MIDAGRI Boletines

Submodulo para extraer, conservar y curar datos agrarios provenientes de:

- boletines diarios GMML en PDF desde SIEA
- boletines mensuales "El Agro en Cifras" en ZIP/Excel desde MIDAGRI

## Objetivo

La fuente se organiza en tres capas para no perder trazabilidad:

- `raw/`: archivo fuente descargado tal como vino del portal
- `base/`: estructura amplia y trazable del contenido extraido
- `curated/`: datasets limpios y listos para consumo agrario

## Estructura esperada en MinIO

Bajo el bucket `agro-productos` y el prefijo `Landing/midagri_boletines/`:

- `raw/gmml_diario/fecha_publicacion=YYYY-MM-DD/*.pdf`
- `raw/agro_en_cifras/anio_publicacion=YYYY/*.(zip|xlsx|xls)`
- `base/base_gmml_diario/fecha_particion=YYYY-MM-DD/`
- `base/base_agro_en_cifras/anio_publicacion=YYYY/`
- `curated/gmml_diario_agrario/fecha_particion=YYYY-MM-DD/`
- `curated/agro_en_cifras_agrario/anio_publicacion=YYYY/categoria_agraria=<categoria>/`

## Datasets

### Base diaria GMML

`base/base_gmml_diario`

Conserva la fila extraida del PDF con trazabilidad al archivo raw:

- `fecha`
- `mercado`
- `producto_raw`
- `unidad_medida_raw`
- `precio_minimo`
- `precio_maximo`
- `precio_promedio`
- `ingreso_t`
- `ruta_raw_origen`
- `registro_hash_fuente`
- `fecha_particion`

### Curated diaria GMML

`curated/gmml_diario_agrario`

Normaliza la fila diaria para consumo:

- `producto`
- `producto_raw`
- `unidad_medida`
- `abastecimiento_origen_1`
- `abastecimiento_origen_2`
- `abastecimiento_origen_3`
- `abastecimiento_total_reportado`
- precios e ingreso
- `ruta_raw_origen`

### Base mensual Agro en Cifras

`base/base_agro_en_cifras`

Guarda las hojas Excel como celdas largas con linaje:

- `hoja_nombre`
- `fila_idx`
- `columna_idx`
- `columna_nombre`
- `celda_valor`
- `archivo_origen`
- `archivo_miembro`
- `archivo_anio_publicacion`
- `archivo_firma_remota`
- `ruta_raw_origen`

### Curated mensual agrario

`curated/agro_en_cifras_agrario`

Enriquece la base mensual con clasificacion agraria:

- `categoria_agraria`
- `dominio_fuente`
- `mes_publicacion`
- `mes_publicacion_nombre`
- `es_hoja_indice`
- celdas y linaje del archivo

Categorias actuales:

- `produccion_agricola`
- `produccion_pecuaria_avicola`
- `agroindustria`
- `comercio_interno`
- `comercio_exterior`
- `insumos_y_servicios_agrarios`

## Prefect

La orquestacion usa un runtime aislado para este modulo. Las dependencias se instalan una sola vez por firma de `requirements.txt` en `.runtime-venvs/midagri-boletines` y solo se reinstalan si cambian los requerimientos.
