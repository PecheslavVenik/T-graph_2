param(
    [string]$DbPath = "data/finbench_sf0_1.duckdb",
    [int]$Port = 18080,
    [string]$Scale = "serious",
    [string]$Archive = "",
    [string]$Distro = "",
    [switch]$SkipData,
    [switch]$ForceData,
    [switch]$NoBuild
)

$ErrorActionPreference = "Stop"

function Get-RepoFullPath {
    param([string]$Path)
    if ([System.IO.Path]::IsPathRooted($Path)) {
        return [System.IO.Path]::GetFullPath($Path)
    }
    return [System.IO.Path]::GetFullPath((Join-Path $RepoRoot $Path))
}

function Invoke-Checked {
    param(
        [string]$Command,
        [string[]]$Arguments
    )
    & $Command @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "$Command $($Arguments -join ' ') failed with exit code $LASTEXITCODE"
    }
}

function Assert-Command {
    param([string]$Name)
    if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
        throw "$Name is required but was not found in PATH."
    }
}

function Wait-Health {
    param(
        [string]$BaseUrl,
        [int]$TimeoutSeconds = 120
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    do {
        try {
            $health = Invoke-RestMethod -Uri "$BaseUrl/actuator/health" -TimeoutSec 5
            if ($health.status -eq "UP") {
                return
            }
            Start-Sleep -Seconds 2
        } catch {
            Start-Sleep -Seconds 2
        }
    } while ((Get-Date) -lt $deadline)

    throw "API did not become healthy at $BaseUrl within $TimeoutSeconds seconds."
}

$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
Set-Location $RepoRoot

Assert-Command "docker"
Invoke-Checked "docker" @("info")

if ($SkipData -and $ForceData) {
    throw "Use either -SkipData or -ForceData, not both."
}

$DataRoot = Get-RepoFullPath "data"
New-Item -ItemType Directory -Force -Path $DataRoot | Out-Null

$DbFullPath = Get-RepoFullPath $DbPath
$dataPrefix = $DataRoot.TrimEnd('\', '/') + [System.IO.Path]::DirectorySeparatorChar
if (-not $DbFullPath.StartsWith($dataPrefix, [System.StringComparison]::OrdinalIgnoreCase)) {
    throw "DbPath must be inside the repo data/ directory because Docker mounts ./data. Got: $DbPath"
}

if ($ForceData -or -not (Test-Path $DbFullPath)) {
    if ($SkipData) {
        throw "FinBench DuckDB file is missing: $DbPath. Run without -SkipData or prepare it with scripts/finbench-data.ps1."
    }

    if ($ForceData) {
        Write-Host "Stopping graph-api before rebuilding the DuckDB file..." -ForegroundColor Cyan
        & docker compose stop graph-api | Out-Host
    }

    $dataArgs = @{
        DbPath = $DbPath
        Scale = $Scale
    }
    if ($Archive -ne "") {
        $dataArgs.Archive = $Archive
    }
    if ($Distro -ne "") {
        $dataArgs.Distro = $Distro
    }

    & (Join-Path $PSScriptRoot "finbench-data.ps1") @dataArgs
    if ($LASTEXITCODE -ne 0) {
        exit $LASTEXITCODE
    }
}

$dbFile = Split-Path $DbFullPath -Leaf
$env:GRAPH_API_PORT = [string]$Port
$env:GRAPH_API_DUCKDB_FILE = $dbFile

$composeArgs = @("compose", "up", "-d", "--force-recreate")
if (-not $NoBuild) {
    $composeArgs += "--build"
}

Write-Host "Starting graph-api Docker service..." -ForegroundColor Cyan
Write-Host "Database: data/$dbFile"
Write-Host "Backend:  http://localhost:$Port"
Invoke-Checked "docker" $composeArgs

$baseUrl = "http://localhost:$Port"
Wait-Health $baseUrl

$SeedFile = Get-RepoFullPath "target/bench-seeds/finbench.json"
$node = $null
if (Test-Path $SeedFile) {
    $seedJson = Get-Content -Raw -Path $SeedFile | ConvertFrom-Json
    if ($null -ne $seedJson.party_rk -and $seedJson.party_rk -ne "") {
        $seedQuery = [uri]::EscapeDataString([string]$seedJson.party_rk)
        $seedUrl = "$baseUrl/api/v1/graph/nodes/search?query=$seedQuery&limit=1"
        $seed = Invoke-RestMethod -Uri $seedUrl -TimeoutSec 10
        $node = @($seed.nodes)[0]
    }
}
$attributeKeys = ""
if ($null -ne $node -and $null -ne $node.attributes) {
    $attributeKeys = (($node.attributes.PSObject.Properties | Select-Object -ExpandProperty Name) -join ", ")
}

Write-Host ""
Write-Host "FinBench API is ready." -ForegroundColor Green
Write-Host "Health:   $baseUrl/actuator/health"
Write-Host "API base: $baseUrl"
Write-Host ""
Write-Host "Frontend env:" -ForegroundColor Green
Write-Host "  VITE_API_BASE_URL=$baseUrl"
Write-Host "  NEXT_PUBLIC_API_BASE_URL=$baseUrl"
if ($attributeKeys -ne "") {
    Write-Host ""
    Write-Host "Smoke node attributes: $attributeKeys"
}
