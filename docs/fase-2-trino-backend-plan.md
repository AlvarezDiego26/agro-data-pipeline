# Fase 2: Capa De Consulta Sobre El Data Lake

## Objetivo

Construir una nueva capa del proyecto para consumir los datos ya cargados en `MinIO` de forma:

- rapida para el usuario final
- amigable para consultas de backend
- desacoplada de la logica de ingesta
- compatible con arquitectura hexagonal en `Express`

Esta fase no reemplaza los pipelines actuales. Los pipelines siguen siendo la fuente de escritura. La nueva capa se monta encima de los datasets ya materializados en `Landing/`.

## Estado Actual Del Sistema

Hoy el repositorio ya tiene tres fuentes productivas orientadas a `Delta Lake` sobre `MinIO`:

### SISAP
- `Landing/sisap/precios_diarios_mercado_lima`
- `Landing/sisap/volumen_diario_mercado_lima`
- `Landing/sisap/precio_diario_regiones`
- tablas de control y eventos de control

### SUNAT
- `Landing/sunat/exportaciones_filtradas`
- tablas base y control para importaciones por ZIP

### MIDAGRI Comercio Exterior
- `Landing/midagri_comercio_exterior/comercio_exterior_agrario`
- `Landing/midagri_comercio_exterior/catalogo_cuadros_comercio_exterior`
- tablas de control

### Orquestacion
- `Prefect` ejecuta la ingesta
- los pipelines escriben en `Delta Lake`
- `MinIO` ya es el storage central del proyecto

## Decision Tecnica

### Recomendacion

Empezar por `Trino` antes de desarrollar el backend.

### Motivo

El backend no debe conectarse directo a `MinIO` ni leer `Parquet/Delta` por su cuenta. Primero conviene validar una capa SQL intermedia que:

- vea los buckets como tablas
- exponga consultas SQL
- permita filtrar, ordenar, agregar y paginar
- simplifique el backend
- deje abierta la puerta a otras herramientas analiticas

### Por Que Trino Primero

- es `open source`
- se conecta bien a `S3/MinIO`
- es buen primer paso para consumir `Delta`
- sirve mejor como base para `Express`
- evita diseñar el backend a ciegas

### Decision De Fase

1. levantar `Trino`
2. validar lectura de tablas `Delta`
3. definir tablas y vistas de negocio
4. construir backend `Express` con arquitectura hexagonal
5. construir frontend `Next.js`

## Alcance De Esta Fase

Esta fase cubre:

- levantar una capa SQL sobre el data lake
- estandarizar nombres de tablas consumibles
- crear el backend que consulta esa capa
- preparar la base para frontend y reporting

Esta fase no cubre por ahora:

- reemplazar `Prefect`
- mover la logica de ingesta al backend
- usar ORM sobre buckets
- construir el frontend completo en la primera iteracion

## Arquitectura Objetivo

```text
Prefect -> Pipelines -> MinIO (Delta Lake)
                               |
                               v
                             Trino
                               |
                               v
                     Express (Hexagonal)
                               |
                               v
                           Next.js
```

## Fases De Implementacion

## Fase 2.1 - Spike Tecnico Con Trino

### Objetivo

Demostrar que `Trino` puede leer correctamente los datasets ya publicados en `MinIO`.

### Entregables

- `Trino` levantado en entorno local o VPS
- conexion a `MinIO`
- acceso a tablas `Delta`
- consultas reales funcionando
- tiempos de respuesta basicos observados

### Tablas Minimas A Validar

- `sisap.precios_diarios_mercado_lima`
- `sisap.volumen_diario_mercado_lima`
- `sisap.precio_diario_regiones`
- `sunat.exportaciones_filtradas`
- `midagri.comercio_exterior_agrario`

### Pruebas Minimas

- conteo de filas
- filtro por fecha
- filtro por producto
- agregacion mensual
- top `N` de productos
- join simple entre catalogos o vistas

### Criterio De Exito

Si `Trino` puede consultar estas tablas de forma consistente y suficientemente rapida, se aprueba como capa de lectura para el backend.

## Fase 2.2 - Modelo De Consumo

### Objetivo

Definir la capa de datos amigable para el backend y para el usuario.

### Propuesta Inicial De Tablas Expuestas

- `agro.sisap_precios`
- `agro.sisap_volumen`
- `agro.sisap_regiones`
- `agro.sunat_exportaciones`
- `agro.midagri_comercio_exterior`

### Propuesta Inicial De Vistas

- `agro.vw_productos_agrarios`
- `agro.vw_precios_resumen`
- `agro.vw_volumen_resumen`
- `agro.vw_exportaciones_resumen`
- `agro.vw_comercio_exterior_resumen`

### Criterios De Diseno

- nombres entendibles
- columnas estables
- fechas consistentes
- filtros comunes previsibles
- no exponer tablas tecnicas de control al frontend

## Fase 2.3 - Backend En Express Con Arquitectura Hexagonal

### Objetivo

Construir una API desacoplada del motor de consulta.

### Estructura Sugerida

```text
backend/
  src/
    domain/
      entities/
      repositories/
      services/
    application/
      use-cases/
      dto/
    infrastructure/
      trino/
      repositories/
      config/
    interfaces/
      http/
        controllers/
        routes/
        middlewares/
```

### Principios

- el dominio no conoce `Trino`
- los casos de uso no conocen HTTP
- la infraestructura implementa repositorios
- los controladores solo adaptan entrada y salida

### Casos De Uso Iniciales

- listar precios SISAP
- listar volumen SISAP
- listar exportaciones SUNAT
- listar comercio exterior MIDAGRI
- obtener series por fecha
- obtener top productos
- obtener resumen por procedencia, region o pais

## Fase 2.4 - Frontend En Next.js

### Objetivo

Consumir la API de `Express` y mostrar informacion de forma rapida y amigable.

### Primeras Pantallas

- dashboard general
- modulo SISAP precios
- modulo SISAP volumen
- modulo SUNAT exportaciones
- modulo MIDAGRI comercio exterior

### Regla De Integracion

El frontend nunca consulta `Trino` ni `MinIO` directo. Solo consume la API del backend.

## Orden Recomendado De Trabajo

1. `Trino` local o en VPS
2. conexion a `MinIO`
3. pruebas SQL sobre tablas reales
4. definicion de tablas y vistas de negocio
5. scaffold del backend `Express`
6. implementacion de primeros casos de uso
7. scaffold del frontend `Next.js`

## Riesgos Conocidos

- algunas tablas `Delta` pueden tener escritura concurrente desde ingesta
- puede haber diferencias semanticas entre datasets de distintas fuentes
- sera necesario controlar bien nombres, fechas y filtros
- si se quiere mas cache o experiencia analitica visual, mas adelante puede evaluarse `Dremio`

## Decision De Herramienta En Esta Etapa

### Elegida

`Trino`

### No Elegida Como Primera Opcion

`Dremio`

### Razon

`Trino` encaja mejor como primera capa de lectura para un backend `Express` con arquitectura hexagonal. `Dremio` puede reevaluarse mas adelante si el proyecto necesita una capa mas fuerte de exploracion visual, catalogo y aceleracion analitica para usuarios de negocio.

## Siguiente Paso Inmediato

Levantar un `spike` funcional de `Trino` conectado a `MinIO` y validar consultas sobre:

- `SISAP`
- `SUNAT`
- `MIDAGRI`

Cuando ese spike este listo, el backend ya se construye contra una interfaz de datos real y no contra supuestos.
