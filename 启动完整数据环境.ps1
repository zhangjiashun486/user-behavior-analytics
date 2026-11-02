param([ValidateSet('check','start','status','stop')][string]$Action = 'check', [string]$ProxyUrl = '')
$ErrorActionPreference = 'Stop'
Set-Location -LiteralPath $PSScriptRoot
if ($ProxyUrl) {
    $env:HTTP_PROXY = $ProxyUrl
    $env:HTTPS_PROXY = $ProxyUrl
    $env:NO_PROXY = 'localhost,127.0.0.1,::1,host.docker.internal'
}
$dockerCommand = Get-Command docker -ErrorAction SilentlyContinue
$dockerCandidates = @(
    (Join-Path $env:LOCALAPPDATA 'Programs\DockerDesktop\resources\bin\docker.exe'),
    'C:\Program Files\Docker\Docker\resources\bin\docker.exe'
)
if ($dockerCommand) { $dockerExe = $dockerCommand.Source }
else { $dockerExe = $dockerCandidates | Where-Object { Test-Path -LiteralPath $_ } | Select-Object -First 1 }
if (-not $dockerExe) { throw 'Docker Desktop is not installed yet. Continue the environment setup first.' }
function Invoke-ProjectDocker {
    & $dockerExe @args
    if ($LASTEXITCODE -ne 0) { throw ('Docker command failed with exit '+$LASTEXITCODE) }
}
$composeArguments = @('compose','--project-name','career-data','--file','docker-compose.windows.yml')
Invoke-ProjectDocker @composeArguments config --quiet
if ($Action -eq 'check') {
    Invoke-ProjectDocker version
    return
}
Invoke-ProjectDocker info --format '{{.ServerVersion}}'
if ($Action -eq 'start') {
    Invoke-ProjectDocker @composeArguments build
    Invoke-ProjectDocker @composeArguments up airflow-init
    Invoke-ProjectDocker @composeArguments up --detach postgres minio airflow-webserver airflow-scheduler
    Invoke-ProjectDocker @composeArguments ps
    Write-Output 'Airflow: http://127.0.0.1:18080 (local demo account airflow / airflow)'
    Write-Output 'MinIO console: http://127.0.0.1:19001 (local demo account minio / minio123)'
    Write-Output 'Container startup is not DAG completion. Trigger and validate user_analytics_dag separately.'
} elseif ($Action -eq 'status') {
    Invoke-ProjectDocker @composeArguments ps
} elseif ($Action -eq 'stop') {
    Invoke-ProjectDocker @composeArguments stop
}
