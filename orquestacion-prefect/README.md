# Orquestacion Prefect

Capa de orquestacion para ejecutar `SISAP` y `SUNAT` con `Prefect` sin duplicar la logica de negocio de cada pipeline.

## Objetivo
- encapsular las corridas de `SISAP` y `SUNAT` como flows independientes
- permitir una capa maestra para `SISAP` que lance unidades autonomas en paralelo
- dejar listo el despliegue sobre `Prefect Cloud` con `managed pool`

## Que Orquesta
### `sisap_main_flow`
Ejecuta una corrida compuesta de `SISAP`:
- `python -m sisap_light.cli run-main`

### `sisap_master_flow`
Planifica y lanza instancias independientes de `SISAP` por:
- `scope`
- `producto`
- `producto x scope`

Cada instancia corre su propio proceso Python y mantiene su control aislado.

### `sunat_main_flow`
Ejecuta:
- `python -m sunat_file.cli run-main`

### `agro_ingesta_flow`
Ejecuta en secuencia:
1. `SISAP` maestro
2. `SUNAT`

## Estructura
- `src/agro_orquestacion/config.py`
  - variables base, defaults y armado de `env`
- `src/agro_orquestacion/planner.py`
  - construccion de unidades de trabajo para `SISAP`
- `src/agro_orquestacion/runner.py`
  - ejecuta modulos Python y retransmite logs a `Prefect`
- `src/agro_orquestacion/flows.py`
  - flows de `SISAP`, `SUNAT` y combinado
- `src/agro_orquestacion/deploy.py`
  - helper para publicar deployments en `agro-managed-pool`
- `src/agro_orquestacion/serve.py`
  - scheduling local opcional

## Estrategias De Instanciacion SISAP
- `por_scope`
  - una instancia por procedencia o por region segun el modulo
- `por_producto`
  - una instancia por producto usando un scope fijo
- `por_producto_scope`
  - una instancia por combinacion `producto x procedencia` o `producto x region`

La recomendacion operativa para el primer cierre es:
- usar `por_scope`
- subir `SISAP_MAX_INSTANCIAS_PARALELAS` de forma gradual
- medir hasta donde aguanta `SISAP` sin bloquear la IP

## Variables Clave
Configurar al menos:

```env
PREFECT_WORK_POOL_NAME=agro-managed-pool
STORAGE_BACKEND=minio
DELTA_ENABLED=true
MINIO_ENDPOINT=http://38.210.246.165:30090
MINIO_ACCESS_KEY=...
MINIO_SECRET_KEY=...
MINIO_BUCKET=agro-productos
MINIO_REGION=us-east-1
SISAP_MINIO_PREFIX=Landing/sisap
SUNAT_MINIO_PREFIX=Landing/sunat
SISAP_ESTRATEGIA_INSTANCIACION=por_scope
SISAP_MAX_INSTANCIAS_PARALELAS=8
SISAP_PRODUCTOS=all
```

Tambien se pueden sobreescribir:
- fechas
- modo de carga
- modulos de `SISAP`
- procedencias y regiones
- paralelismo interno de shards
- cantidad de instancias paralelas del maestro

## Ejecucion Local
### SISAP maestro
```powershell
python -m agro_orquestacion.flows sisap
```

### SISAP corrida compuesta
```powershell
python -m agro_orquestacion.flows sisap-main
```

### SUNAT
```powershell
python -m agro_orquestacion.flows sunat
```

### Flujo combinado
```powershell
python -m agro_orquestacion.flows agro
```

## Scheduling Local
```powershell
python -m agro_orquestacion.serve
```

## Publicacion En Prefect Cloud
Con la sesion de `Prefect Cloud` ya autenticada:

```powershell
python -m agro_orquestacion.deploy
```

Eso intenta publicar:
- `sisap-managed`
- `sisap-master-managed`
- `sunat-managed`

en el work pool:
- `agro-managed-pool`

con source:
- `https://github.com/OazisLabs/agro-proyecto.git`

## Principio De Diseño
- `Prefect` orquesta
- `SISAP` y `SUNAT` siguen concentrando la logica de negocio
- `SISAP` maestro reparte unidades pequenas para que una falla no frene a las demas
- `MinIO` sigue siendo el destino operativo de `Delta`
- el control sigue viviendo dentro de cada pipeline, no en la capa de orquestacion
