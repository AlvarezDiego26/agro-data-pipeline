param(
    [string[]]$Modulos = @('volumen', 'precios', 'ciudades-mayoristas', 'ciudades-minoristas'),
    [string[]]$Procedencias = @('Arequipa'),
    [string[]]$Regiones = @('Arequipa'),
    [string]$PauseSeconds = '30',
    [string]$ProductoNombre,
    [string]$ProductoCodigo,
    [string]$FechaInicio,
    [string]$FechaFin,
    [string]$ModoCarga = 'incremental',
    [string]$IncrementalOverlapDias = '0',
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

$runner = Join-Path $PSScriptRoot 'run_sisap_pipeline.ps1'

& $runner `
    -Modulos ($Modulos -join ',') `
    -Procedencias ($Procedencias -join ',') `
    -Regiones ($Regiones -join ',') `
    -PauseSeconds $PauseSeconds `
    -ProductoNombre $ProductoNombre `
    -ProductoCodigo $ProductoCodigo `
    -FechaInicio $FechaInicio `
    -FechaFin $FechaFin `
    -ModoCarga $ModoCarga `
    -IncrementalOverlapDias $IncrementalOverlapDias `
    -MercadoCodigo $MercadoCodigo `
    -MercadoNombre $MercadoNombre `
    -MaxQueries $MaxQueries `
    -StorageBackend $StorageBackend `
    -DeltaEnabled $DeltaEnabled `
    -MinioEndpoint $MinioEndpoint `
    -MinioAccessKey $MinioAccessKey `
    -MinioSecretKey $MinioSecretKey `
    -MinioBucket $MinioBucket `
    -MinioRegion $MinioRegion `
    -MinioPrefix $MinioPrefix `
    -ProjectRoot $ProjectRoot

exit $LASTEXITCODE
