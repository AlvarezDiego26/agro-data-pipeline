# Orquestacion Prefect

Capa de orquestacion para ejecutar `SISAP` y `SUNAT` con `Prefect` sin duplicar la logica de negocio de cada pipeline.

## Objetivo
- encapsular las corridas de `SISAP`, `SUNAT`, `MIDAGRI CE` y `MIDAGRI Boletines` como flows independientes
- permitir una capa maestra para `SISAP` que lance unidades autonomas en paralelo
- dejar listo el despliegue sobre `Prefect Cloud` tanto en `managed pool` como en `process worker` local

## Que Orquesta
### `sisap_main_flow`
Ejecuta una corrida compuesta de `SISAP`:
- `python -m sisap_light.cli run-main`

### `sisap_master_flow`
Planifica y lanza instancias independientes de `SISAP` por:
- `modulo`

Cada instancia corre su propio proceso Python y mantiene su control aislado.

### `sunat_main_flow`
Ejecuta:
- `python -m sunat_file.cli run-main`

### `agro_ingesta_flow`
Ejecuta la ingesta habilitada de `SISAP`, `SUNAT`, `MIDAGRI CE` y `MIDAGRI Boletines`.

## Estructura
- `src/agro_orquestacion/config.py`
  - variables base, defaults y armado de `env`
- `src/agro_orquestacion/runner.py`
  - ejecuta modulos Python y retransmite logs a `Prefect`
- `src/agro_orquestacion/flows.py`
  - flows de ingesta y combinado
- `src/agro_orquestacion/deploy.py`
  - helper para publicar deployments `managed` o `process`
- `src/agro_orquestacion/serve.py`
  - scheduling local opcional

## Estrategias De Instanciacion SISAP
- `por_modulo`
  - una instancia por modulo de `SISAP`

La recomendacion operativa para el primer cierre es:
- usar `por_modulo`
- subir `SISAP_MAX_INSTANCIAS_PARALELAS` de forma gradual
- medir hasta donde aguanta `SISAP` sin bloquear la IP

## Variables Clave
Configurar al menos:

```env
PREFECT_EXECUTION_MODE=process
PREFECT_WORK_POOL_NAME=
PREFECT_MANAGED_WORK_POOL_NAME=agro-managed-pool
PREFECT_PROCESS_WORK_POOL_NAME=agro-process-pool
PREFECT_REPO_URL=https://github.com/tu-organizacion/tu-repo.git
PREFECT_REPO_BRANCH=main
PREFECT_GITHUB_USERNAME=
PREFECT_GITHUB_ACCESS_TOKEN=
PREFECT_GITHUB_SECRET_BLOCK_NAME=github-repo-read-token
STORAGE_BACKEND=minio
DELTA_ENABLED=true
MINIO_ENDPOINT=http://minio-api:9000
MINIO_ACCESS_KEY=
MINIO_SECRET_KEY=
MINIO_BUCKET=nombre-del-bucket
MINIO_REGION=us-east-1
SISAP_MINIO_PREFIX=Landing/sisap
SUNAT_MINIO_PREFIX=Landing/sunat
SISAP_ESTRATEGIA_INSTANCIACION=por_modulo
SISAP_MAX_INSTANCIAS_PARALELAS=8
SISAP_PRODUCTOS=all
SISAP_SAVE_DEBUG_HTML=false
```

Tambien se pueden sobreescribir:
- fechas
- modo de carga
- modulos de `SISAP`
- procedencias y regiones
- paralelismo interno de shards
- cantidad de instancias paralelas del maestro

## Repo Privado En Prefect Cloud
Para `GitHub` privado hay dos caminos:
- recomendado a largo plazo: instalar el `GitHub App` de `Prefect Cloud`
- salida operativa inmediata: usar `PREFECT_GITHUB_ACCESS_TOKEN` con un `PAT` fino de solo lectura

Con el enfoque de token:
- `PREFECT_GITHUB_ACCESS_TOKEN` debe tener permiso de lectura al repositorio
- `PREFECT_GITHUB_SECRET_BLOCK_NAME` define el nombre del bloque `Secret` que `deploy.py` crea o actualiza automaticamente en `Prefect`

El helper `python -m agro_orquestacion.deploy` o `agro-orquestacion-deploy` no publica deployments managed si:
- `MINIO_ACCESS_KEY` esta vacio
- `MINIO_SECRET_KEY` esta vacio

Eso evita dejar un deployment `Ready` que en runtime vaya a caerse por configuracion incompleta.

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

## Prefect Server En Docker
Levantar el stack self-hosted de `Prefect` con:
- `PostgreSQL`
- `Redis`
- `prefect-server`
- `prefect-services`
- `prefect-worker`

desde esta carpeta:

```powershell
docker compose up -d
```

Verificar contenedores:

```powershell
docker compose ps
```

Ver logs si algo falla:

```powershell
docker compose logs -f
```

Configurar tu cliente local para apuntar al server:

```powershell
prefect config set PREFECT_API_URL="http://127.0.0.1:4200/api"
```

Abrir el dashboard:

```powershell
prefect dashboard open
```

Apagar el stack:

```powershell
docker compose down
```

Si tambien quieres eliminar el volumen de PostgreSQL:

```powershell
docker compose down -v
```

## Publicacion En Prefect Cloud
Con la sesion de `Prefect Cloud` ya autenticada:

```powershell
python -m agro_orquestacion.deploy
```

Alternativa equivalente si el paquete ya esta instalado:

```powershell
agro-orquestacion-deploy
```

### Modo `managed`
Si `PREFECT_EXECUTION_MODE=managed`, publica:
- `sisap-volumen-managed`
- `sisap-precios-managed`
- `sisap-regiones-managed`
- `sunat-managed`
- `midagri-ce-managed`
- `midagri-boletines-managed`

en el work pool configurado para `managed`.

### Modo `process`
Si `PREFECT_EXECUTION_MODE=process`, publica:
- `sisap-volumen-local`
- `sisap-precios-local`
- `sisap-regiones-local`
- `sunat-local`
- `midagri-ce-local`
- `midagri-boletines-local`

en el work pool configurado para `process`.

En este modo:
- no usa contenedor `managed`
- no depende del clone remoto en runtime
- corre en tu propia maquina mediante un worker `process`
- resuelve `PYTHONPATH` y `working_dir` desde el repo local del worker

### Worker Local Recomendado
Crear el pool `process`:

```powershell
prefect work-pool create "agro-process-pool" --type process
```

Levantar el worker local desde la raiz del repo:

```powershell
prefect worker start --pool "agro-process-pool" --type process --limit 4
```

Con esto, los runs de `sisap-volumen-local`, `sisap-precios-local`, `sisap-regiones-local`, `sunat-local`, `midagri-ce-local` y `midagri-boletines-local` ya generan logs reales del Python que se ejecuta en tu maquina.

### Publicar Deployments Dentro Del Worker Docker
Si quieres reaplicar deployments desde `prefect-worker`, usa uno de estos comandos:

```powershell
docker compose exec prefect-worker sh -lc "cd /app/orquestacion-prefect && python -m agro_orquestacion.deploy"
```

```powershell
docker compose exec prefect-worker agro-orquestacion-deploy
```

La segunda opcion es la mas estable porque usa el comando instalado por `pip install -e .` y no depende del directorio actual.

## Notas De Rendimiento
- `SISAP_SAVE_DEBUG_HTML=false` evita persistir HTML crudo en produccion
- si necesitas diagnosticar un cambio del portal, activa `SISAP_SAVE_DEBUG_HTML=true` temporalmente
- mantener `scope_workers`, `shard_workers` y `prefect_max_parallel_pipelines` en valores bajos ayuda bastante en VPS pequenas

## Principio De Diseño
- `Prefect` orquesta
- `SISAP` y `SUNAT` siguen concentrando la logica de negocio
- `SISAP` maestro reparte unidades pequenas para que una falla no frene a las demas
- `MinIO` sigue siendo el destino operativo de `Delta`
- el control sigue viviendo dentro de cada pipeline, no en la capa de orquestacion
