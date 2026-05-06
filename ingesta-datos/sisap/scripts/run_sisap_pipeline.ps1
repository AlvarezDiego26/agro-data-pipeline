param(
    [string]$Modulos,
    [string]$Procedencias,
    [string]$Regiones,
    [string]$PauseSeconds,
    [string]$ProductoNombre,
    [string]$ProductoCodigo,
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

Set-OptionalEnv 'SISAP_MODULOS' $Modulos
Set-OptionalEnv 'SISAP_PROCEDENCIAS' $Procedencias
Set-OptionalEnv 'SISAP_REGIONES' $Regiones
Set-OptionalEnv 'SISAP_PAUSE_SECONDS' $PauseSeconds
Set-OptionalEnv 'SISAP_PRODUCTO_NOMBRE' $ProductoNombre
Set-OptionalEnv 'SISAP_PRODUCTO_CODIGO' $ProductoCodigo
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

Write-Host 'Ejecutando SISAP pipeline principal'
python -m sisap_light.cli run-main
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}
