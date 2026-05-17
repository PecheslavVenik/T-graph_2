param(
    [string]$DbPath = "data/finbench_sf0_1.duckdb",
    [string]$Scale = "serious",
    [string]$Archive = "",
    [string]$Distro = ""
)

$ErrorActionPreference = "Stop"

function Resolve-RepoPath {
    param([string]$Path)
    if ([System.IO.Path]::IsPathRooted($Path)) {
        return (Resolve-Path $Path).Path
    }
    return (Resolve-Path (Join-Path $RepoRoot $Path)).Path
}

function Convert-ToWslPath {
    param([string]$WindowsPath)
    $args = @()
    if ($Distro -ne "") {
        $args += @("-d", $Distro)
    }
    $args += @("wslpath", "-a", $WindowsPath)
    $converted = & wsl @args
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($converted)) {
        throw "Failed to convert path to WSL path: $WindowsPath"
    }
    return $converted.Trim()
}

function Quote-Bash {
    param([string]$Value)
    return "'" + ($Value -replace "'", "'\''") + "'"
}

$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
Set-Location $RepoRoot

if (-not (Get-Command wsl -ErrorAction SilentlyContinue)) {
    throw "WSL is required. Install WSL, then run this script again from PowerShell."
}

if ($Archive -eq "") {
    if (Test-Path (Join-Path $RepoRoot "bench/sf0.1.tar")) {
        $Archive = "bench/sf0.1.tar"
    } elseif (Test-Path (Join-Path $RepoRoot "bench/sf1.tar")) {
        $Archive = "bench/sf1.tar"
    } else {
        throw "FinBench archive not found. Put sf0.1.tar or sf1.tar into the bench/ directory, or pass -Archive path."
    }
}

$DbDir = Split-Path (Join-Path $RepoRoot $DbPath) -Parent
New-Item -ItemType Directory -Force -Path $DbDir | Out-Null

$RepoWsl = Convert-ToWslPath $RepoRoot
$DbWsl = Convert-ToWslPath (Join-Path $RepoRoot $DbPath)
$ArchiveWsl = Convert-ToWslPath (Resolve-RepoPath $Archive)

$wslArgs = @()
if ($Distro -ne "") {
    $wslArgs += @("-d", $Distro)
}

& wsl @wslArgs bash -lc "command -v duckdb >/dev/null 2>&1"
if ($LASTEXITCODE -ne 0) {
    Write-Host "DuckDB CLI is not installed inside WSL." -ForegroundColor Yellow
    Write-Host "Install it in WSL, for example:" -ForegroundColor Yellow
    Write-Host "  sudo apt-get update && sudo apt-get install -y curl unzip" -ForegroundColor Yellow
    Write-Host "  curl -L https://github.com/duckdb/duckdb/releases/download/v1.4.2/duckdb_cli-linux-amd64.zip -o /tmp/duckdb.zip" -ForegroundColor Yellow
    Write-Host "  unzip -o /tmp/duckdb.zip -d /tmp/duckdb" -ForegroundColor Yellow
    Write-Host "  sudo install /tmp/duckdb/duckdb /usr/local/bin/duckdb" -ForegroundColor Yellow
    exit 1
}

$command = @(
    "cd $(Quote-Bash $RepoWsl)",
    "BENCH_DB=$(Quote-Bash $DbWsl)",
    "BENCH_SCALE=$(Quote-Bash $Scale)",
    "FINBENCH_ARCHIVE=$(Quote-Bash $ArchiveWsl)",
    "./scripts/finbench-data.sh"
) -join " "

Write-Host "Preparing FinBench DuckDB via WSL..." -ForegroundColor Cyan
Write-Host "Archive: $Archive"
Write-Host "Output:  $DbPath"
& wsl @wslArgs bash -lc $command

if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}

Write-Host ""
Write-Host "Done. Start the API with:" -ForegroundColor Green
Write-Host "  GRAPH_API_PORT=18080 docker compose up --build -d"
