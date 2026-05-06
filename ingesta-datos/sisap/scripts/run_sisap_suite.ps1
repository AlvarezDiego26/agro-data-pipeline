param(
    [string]$PauseSeconds = '30',
    [string]$ProductoNombre,
    [string]$ProductoCodigo,
    [string]$ProcedenciaNombre = 'Arequipa',
    [string]$ProcedenciaCodigo,
    [string]$RegionNombre = 'Arequipa',
    [string]$FechaInicio,
    [string]$FechaFin,
    [string]$ModoCarga,
    [string]$IncrementalOverlapDias,
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
    -Modulos 'volumen,precios,ciudades-mayoristas,ciudades-minoristas' `
    -Procedencias $ProcedenciaNombre `
    -Regiones $RegionNombre `
    -PauseSeconds $PauseSeconds `
    -ProductoNombre $ProductoNombre `
    -ProductoCodigo $ProductoCodigo `
    -FechaInicio $FechaInicio `
    -FechaFin $FechaFin `
    -ModoCarga $ModoCarga `
    -IncrementalOverlapDias $IncrementalOverlapDias `
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
