param(
    [string]$FechaInicio,
    [string]$FechaFin,
    [string]$ModoCarga,
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
    param([string]$Name, [string]$Value)
    if ($null -ne $Value -and $Value -ne '') {
        [Environment]::SetEnvironmentVariable($Name, $Value, 'Process')
    }
}

if (-not $ProjectRoot) {
    $ProjectRoot = Split-Path -Parent $PSScriptRoot
}

$env:PYTHONPATH = Join-Path $ProjectRoot 'src'
Set-Location $ProjectRoot

Set-OptionalEnv 'SUNAT_FECHA_CORTE_INICIO' $FechaInicio
Set-OptionalEnv 'SUNAT_FECHA_CORTE_FIN' $FechaFin
Set-OptionalEnv 'SUNAT_MODO_CARGA' $ModoCarga
Set-OptionalEnv 'SUNAT_STORAGE_BACKEND' $StorageBackend
Set-OptionalEnv 'SUNAT_DELTA_ENABLED' $DeltaEnabled
Set-OptionalEnv 'MINIO_ENDPOINT' $MinioEndpoint
Set-OptionalEnv 'MINIO_ACCESS_KEY' $MinioAccessKey
Set-OptionalEnv 'MINIO_SECRET_KEY' $MinioSecretKey
Set-OptionalEnv 'MINIO_BUCKET' $MinioBucket
Set-OptionalEnv 'MINIO_REGION' $MinioRegion
Set-OptionalEnv 'MINIO_PREFIX' $MinioPrefix

Write-Host 'Ejecutando SUNAT pipeline fresco'
python -m sunat_file.cli run-main
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}
