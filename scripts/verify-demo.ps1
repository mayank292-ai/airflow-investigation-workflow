[CmdletBinding()]
param(
    [Parameter(Mandatory=$true)][guid]$Investigation,
    [Parameter(Mandatory=$true)][guid]$Patch,
    [string]$FreshRun,
    [guid]$ContinueInvestigation = [guid]::Empty,
    [switch]$SkipRestartVerification,
    [switch]$SkipBuild
)
$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
$airflowScript = Join-Path $PSScriptRoot 'airflow.ps1'
$pythonExe = Join-Path $projectRoot '.venv\Scripts\python.exe'
Push-Location -LiteralPath $projectRoot
try {
    Write-Host 'Step 1/3: stop/start services and verify preserved scheduled history.'
    if ($SkipRestartVerification) {
        Write-Host 'Previously completed stop/start gate retained; checking current history only.'
        & $airflowScript -Action VerifyHistory
    } else { & $airflowScript -Action VerifyRestart }
    $freshFailed = $false
    if ($ContinueInvestigation -ne [guid]::Empty) {
        Write-Host 'Continuing the incomplete post-restart investigation; prior failures/evidence retained.'
        & $pythonExe -m afi.cli continue $ContinueInvestigation.ToString()
        if ($LASTEXITCODE -ne 0) {
            $freshFailed = $true
            Write-Warning 'Investigation incomplete; preserving records and continuing independent validation.'
        }
    } elseif ($FreshRun) {
        Write-Host 'Post-restart live gate: NEW investigation of existing history; no DAG rerun.'
        & $pythonExe -m afi.cli investigate local_pipeline_03 $FreshRun --task ingest --attempt 2 --fresh
        if ($LASTEXITCODE -ne 0) {
            $freshFailed = $true
            Write-Warning 'Fresh investigation incomplete; preserving its records and continuing the independent validation check.'
        }
    }
    Write-Host 'Step 2/3: build the fixed validation environment.'
    Write-Host 'Temporarily stopping this project to free memory for build/validation; persisted data is retained.'
    & $airflowScript -Action Stop
    try {
        if ($SkipBuild) { Write-Host 'Using previously built prepared validation image; runner verifies package versions.' }
        else { & $airflowScript -Action BuildValidation }
        Write-Host 'Step 3/3: validate only the isolated proposed patch.'
        & $pythonExe -m afi.cli validate-fix $Investigation.ToString() $Patch.ToString()
        if ($LASTEXITCODE -ne 0) { throw 'Validation command failed; inspect saved output.' }
        $summaryPath = Join-Path $projectRoot ('state\investigations\' + $Investigation + '\' + $Patch + '\validation.json')
        $summary = Get-Content -LiteralPath $summaryPath -Raw | ConvertFrom-Json
        if ($summary.status -ne 'locally_validated') {
            throw ('Patch validation status: ' + $summary.status + '. Failed/blocked artifacts are preserved.')
        }
    } finally {
        Write-Host 'Restoring project services and checking preserved history again.'
        & $airflowScript -Action RestartAirflow
        & $airflowScript -Action VerifyHistory
    }
    if ($freshFailed) { throw 'Validation completed, but the new investigation was incomplete; inspect its preserved operation.' }
    Write-Host 'Requested verification steps completed. Local validation does not verify BigQuery or deployment.'
} finally { Pop-Location }
