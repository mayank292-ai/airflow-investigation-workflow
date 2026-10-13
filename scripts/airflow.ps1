[CmdletBinding()]
param(
    [ValidateSet('Check', 'Start', 'RestartAirflow', 'Status', 'PrepareHistory', 'VerifyHistory', 'VerifyRestart', 'BuildValidation', 'Stop')]
    [string]$Action = 'Check'
)
$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
$pythonExe = Join-Path $projectRoot '.venv\Scripts\python.exe'
$dockerExe = $null
$dockerCommand = Get-Command docker.exe -ErrorAction SilentlyContinue
if ($dockerCommand) { $dockerExe = $dockerCommand.Source }
if (-not $dockerExe) {
    foreach ($candidate in @(
        (Join-Path $env:LOCALAPPDATA 'Programs\DockerDesktop\resources\bin\docker.exe'),
        'C:\Program Files\Docker\Docker\resources\bin\docker.exe'
    )) {
        if (Test-Path -LiteralPath $candidate -PathType Leaf) { $dockerExe = $candidate; break }
    }
}
if (-not $dockerExe) { throw 'Docker CLI not found. Install/open Docker Desktop first.' }
if (-not (Test-Path -LiteralPath $pythonExe)) { throw 'Project Python environment is missing. See README.' }
# Makes the CLI available to doctor and validation subprocesses in this process only.
$env:PATH = (Split-Path -Parent $dockerExe) + ';' + $env:PATH

function Invoke-Docker {
    param([string[]]$DockerArguments)
    & $dockerExe @DockerArguments
    if ($LASTEXITCODE -ne 0) { throw "Docker command failed (exit $LASTEXITCODE)." }
}
function Invoke-Investigator {
    param([string[]]$CliArguments)
    & $pythonExe -m afi.cli @CliArguments
    if ($LASTEXITCODE -ne 0) { throw "Project command failed (exit $LASTEXITCODE)." }
}
function Test-Engine {
    $infoText = & $dockerExe info --format '{{json .}}'
    if ($LASTEXITCODE -ne 0) { throw 'Docker engine unavailable. Wait for Docker Desktop to finish starting.' }
    $info = $infoText | ConvertFrom-Json
    if ($info.OSType -ne 'linux') { throw 'Switch Docker Desktop to Linux containers.' }
    Write-Host ('Docker server {0}; Linux engine RAM {1:N2} GiB; CPUs {2}' -f $info.ServerVersion, ($info.MemTotal / 1GB), $info.NCPU)
    return $info
}

function Start-AirflowServices {
    param([switch]$Recreate)
    $arguments = @('compose', 'up', '-d', '--wait', '--wait-timeout', '240')
    if ($Recreate) { $arguments += '--force-recreate' }
    $arguments += @('api', 'scheduler', 'dag-processor')
    try {
        Invoke-Docker -DockerArguments $arguments
    } catch {
        # Always include bounded diagnostics after a startup failure.
        & $dockerExe compose ps --all
        & $dockerExe compose logs --no-color --tail 80 api scheduler dag-processor
        throw
    }
    Invoke-Investigator -CliArguments @('demo', 'check-environment')
}

Push-Location -LiteralPath $projectRoot
try {
    if ($Action -eq 'Stop') {
        Invoke-Docker -DockerArguments @('compose', 'stop')
        Write-Host 'Project containers stopped. Metadata volume, logs, snapshots and investigations retained.'
        return
    }
    $engineInfo = Test-Engine
    switch ($Action) {
        'Check' {
            Invoke-Docker -DockerArguments @('version')
            Invoke-Investigator -CliArguments @('doctor')
        }
        'Start' {
            # These bounds are admission checks, not a claim that the workload is verified.
            if ($engineInfo.MemTotal -lt 3.5GB) { throw 'Allow at least 4 GB RAM for the Docker Linux engine before starting Airflow.' }
            $freeText = & $pythonExe -c "from afi.system import memory_status; print(memory_status()['available_bytes'])"
            if ($LASTEXITCODE -ne 0) { throw 'Unable to measure Windows memory.' }
            $freeBytes = [long]$freeText
            Write-Host ('Available Windows RAM: {0:N2} GiB' -f ($freeBytes / 1GB))
            if ($freeBytes -lt 2GB) { throw 'Save your work and close unused apps until at least 2 GB of Windows RAM is available, then retry. No apps were closed by this script.' }
            if (-not (Test-Path -LiteralPath 'deployment\manifest.json')) {
                Invoke-Investigator -CliArguments @('demo', 'setup')
            }
            Invoke-Docker -DockerArguments @('compose', 'config', '--quiet')
            Invoke-Docker -DockerArguments @('compose', 'pull', 'postgres', 'api', 'scheduler', 'dag-processor')
            Invoke-Docker -DockerArguments @('compose', 'up', '-d', '--wait', '--wait-timeout', '120', 'postgres')
            Invoke-Docker -DockerArguments @('compose', '--profile', 'setup', 'run', '--rm', 'init')
            Start-AirflowServices
            Write-Host 'Airflow: http://localhost:8080 (synthetic local credentials: airflow / airflow).'
            Write-Host 'DAGs remain paused. PrepareHistory creates the bounded scheduled history once; startup never triggers DAGs.'
        }
        'RestartAirflow' {
            # Existing installation: preserve PostgreSQL and run data, apply Compose changes.
            Invoke-Docker -DockerArguments @('compose', 'config', '--quiet')
            Start-AirflowServices -Recreate
        }
        'Status' {
            Invoke-Docker -DockerArguments @('compose', 'ps')
            Invoke-Investigator -CliArguments @('demo', 'check-environment')
        }
        'PrepareHistory' { Invoke-Investigator -CliArguments @('demo', 'prepare-history') }
        'VerifyHistory' { Invoke-Investigator -CliArguments @('demo', 'verify-history') }
        'VerifyRestart' {
            try {
                Invoke-Investigator -CliArguments @('demo', 'verify-history')
            } catch {
                Write-Warning 'Pre-restart history check incomplete. Preserved evidence/results remain; recovering services and requiring the post-restart check to pass.'
            }
            Invoke-Docker -DockerArguments @('compose', 'stop')
            Invoke-Docker -DockerArguments @('compose', 'up', '-d', '--wait', '--wait-timeout', '120', 'postgres')
            Start-AirflowServices
            Invoke-Investigator -CliArguments @('demo', 'verify-history')
            Write-Host 'Stop/start history checks passed. A NEW live investigation remains a separate gate; no DAG was unpaused or triggered.'
        }
        'BuildValidation' {
            Invoke-Docker -DockerArguments @('build', '-t', 'afi-validation:3.3.1', '-f', 'validation/Dockerfile', 'validation')
        }
    }
} finally { Pop-Location }
