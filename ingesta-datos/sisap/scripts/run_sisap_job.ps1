param(
    [Parameter(Mandatory = $true)]
    [ValidateSet('volumen', 'precios', 'ciudades-mayoristas', 'ciudades-minoristas')]
    [string]$Job,
    [string]$ProductoNombre,
    [string]$ProductoCodigo,
    [string]$ProcedenciaNombre,
    [string]$ProcedenciaCodigo,
    [string]$RegionNombre,
    [string]$RegionCodigo,
    [string]$FechaInicio,
    [string]$FechaFin,
    [string]$ModoCarga,
    [string]$IncrementalOverlapDias,
    [string]$MercadoCodigo,
    [string]$MercadoNombre,
    [string]$MaxQueries,
    [string]$StorageBackend,
    [string]$DeltaEnabled,
    [string]$MinioEndpoint,
    [string]$MinioAccessKey,
    [string]$MinioSecretKey,
    [string]$MinioBucket,
    [string]$MinioRegion,
    [string]$MinioPrefix,
    [string]$ProjectRoot
)

function Set-OptionalEnv {
    param(
        [string]$Name,
        [string]$Value
    )

    if ($null -ne $Value -and $Value -ne '') {
        [Environment]::SetEnvironmentVariable($Name, $Value, 'Process')
    }
}

if (-not $ProjectRoot) {
    $ProjectRoot = Split-Path -Parent $PSScriptRoot
}

$env:PYTHONPATH = Join-Path $ProjectRoot 'src'
Set-Location $ProjectRoot

Set-OptionalEnv 'SISAP_PRODUCTO_NOMBRE' $ProductoNombre
Set-OptionalEnv 'SISAP_PRODUCTO_CODIGO' $ProductoCodigo
Set-OptionalEnv 'SISAP_PROCEDENCIA_NOMBRE' $ProcedenciaNombre
Set-OptionalEnv 'SISAP_PROCEDENCIA_CODIGO' $ProcedenciaCodigo
Set-OptionalEnv 'SISAP_REGION_NOMBRE' $RegionNombre
Set-OptionalEnv 'SISAP_REGION_CODIGO' $RegionCodigo
Set-OptionalEnv 'SISAP_FECHA_INICIO' $FechaInicio
Set-OptionalEnv 'SISAP_FECHA_FIN' $FechaFin
Set-OptionalEnv 'SISAP_MODO_CARGA' $ModoCarga
Set-OptionalEnv 'SISAP_INCREMENTAL_OVERLAP_DIAS' $IncrementalOverlapDias
Set-OptionalEnv 'SISAP_MERCADO_CODIGO' $MercadoCodigo
Set-OptionalEnv 'SISAP_MERCADO_NOMBRE' $MercadoNombre
Set-OptionalEnv 'SISAP_MAX_QUERIES' $MaxQueries
Set-OptionalEnv 'STORAGE_BACKEND' $StorageBackend
Set-OptionalEnv 'DELTA_ENABLED' $DeltaEnabled
Set-OptionalEnv 'MINIO_ENDPOINT' $MinioEndpoint
Set-OptionalEnv 'MINIO_ACCESS_KEY' $MinioAccessKey
Set-OptionalEnv 'MINIO_SECRET_KEY' $MinioSecretKey
Set-OptionalEnv 'MINIO_BUCKET' $MinioBucket
Set-OptionalEnv 'MINIO_REGION' $MinioRegion
Set-OptionalEnv 'MINIO_PREFIX' $MinioPrefix

$command = switch ($Job) {
    'volumen' { 'run-volumen' }
    'precios' { 'run-precios' }
    'ciudades-mayoristas' { 'run-ciudades-mayoristas' }
    'ciudades-minoristas' { 'run-ciudades-minoristas' }
}

Write-Host "Ejecutando SISAP job: $Job"
python -m sisap_light.cli $command
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}
