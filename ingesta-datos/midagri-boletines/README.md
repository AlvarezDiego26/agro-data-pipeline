# Pipeline Ingesta de Boletines Diarios MIDAGRI GMML

Submódulo independiente para la extracción, limpieza, parseo y almacenamiento estructurado de los boletines diarios de precios y abastecimiento del Gran Mercado Mayorista de Lima (GMML) publicados en SIEA.

## 🛠️ Requisitos Técnicos
Utiliza dependencias extremadamente ligeras para ejecutarse sin navegadores y con mínimo consumo de RAM (< 100MB):
- `requests` / `httpx` para descarga HTTP directa.
- `pdfplumber` para la extracción de tablas de texto dentro del PDF en memoria.
- `polars` para la normalización ultra-rápida y tipado estructurado.
- `deltalake` para almacenamiento particionado transaccional (MinIO).

## 📁 Estructura del Almacenamiento en MinIO
Los datos procesados se guardan en Delta Lake bajo la siguiente ruta del bucket `agro-productos`:
`Landing/midagri_boletines/gmml_diario/`

Particionado por `fecha_particion` derivada directamente de la fecha de publicación del reporte.
