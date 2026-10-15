# Shared helpers for the Gas Upstream Digital Twin deploy scripts (Windows PowerShell).
# Dot-sourced by deploy.ps1 and generate_data.ps1. Not meant to be run directly.

# A stale DATABRICKS_AUTH_STORAGE in the environment can break OAuth; clear it for this session.
Remove-Item Env:\DATABRICKS_AUTH_STORAGE -ErrorAction SilentlyContinue

# On PowerShell 7.4+ native commands throw on a non-zero exit when ErrorActionPreference is 'Stop'.
# We intentionally inspect $LASTEXITCODE after CLI calls (e.g. a missing catalog returns non-zero),
# so opt out of that behaviour. Harmless on older PowerShell (just sets a normal variable).
$PSNativeCommandUseErrorActionPreference = $false

function Step($m) { Write-Host ""; Write-Host "==> $m" -ForegroundColor Cyan }
function Info($m) { Write-Host "    $m" }
function Ok($m)   { Write-Host "    [ok] $m" -ForegroundColor Green }
function Warn($m) { Write-Host "    [!] $m" -ForegroundColor Yellow }
function Die($m)  { Write-Host ""; Write-Host "ERROR: $m" -ForegroundColor Red; exit 1 }

# Run the databricks CLI and return parsed JSON (or $null on failure). Does not throw.
# ($Args is an automatic PowerShell variable, so the parameter is named $CliArgs.)
function Db-Json {
  param([Parameter(ValueFromRemainingArguments=$true)][string[]]$CliArgs)
  try {
    $out = & databricks @CliArgs 2>$null
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace(($out -join ''))) { return $null }
    return ($out -join "`n" | ConvertFrom-Json)
  } catch { return $null }
}

# Run one SQL statement on the warehouse, polling to completion. Returns $true on SUCCEEDED.
# Callers set $Warehouse and $Profile in scope. Shared by deploy.ps1 and cleanup.ps1.
function Run-Sql([string]$Stmt) {
  $body = @{ warehouse_id=$Warehouse; wait_timeout='30s'; statement=$Stmt } | ConvertTo-Json -Compress
  $out = Db-Json api post /api/2.0/sql/statements --json $body -p $Profile
  $state = if ($out) { $out.status.state } else { "" }
  $sid   = if ($out) { $out.statement_id } else { "" }
  $tries = 0
  while (($state -eq 'PENDING' -or $state -eq 'RUNNING') -and $tries -lt 25) {
    Start-Sleep -Seconds 6; $tries++
    $out = Db-Json api get "/api/2.0/sql/statements/$sid" -p $Profile
    $state = if ($out) { $out.status.state } else { "" }
  }
  return ($state -eq 'SUCCEEDED')
}

function Refresh-Path {
  $m = [System.Environment]::GetEnvironmentVariable('Path','Machine')
  $u = [System.Environment]::GetEnvironmentVariable('Path','User')
  $env:Path = ($m, $u -join ';')
}

function Ensure-Cli {
  if (-not (Get-Command databricks -ErrorAction SilentlyContinue)) {
    Step "Installing the Databricks CLI (winget)"
    try {
      winget install --id Databricks.DatabricksCLI -e --accept-source-agreements --accept-package-agreements
    } catch {
      Die "Could not install the Databricks CLI automatically. Install it, then re-run:
      winget install Databricks.DatabricksCLI
      (docs: https://docs.databricks.com/dev-tools/cli/install.html )"
    }
    Refresh-Path
    if (-not (Get-Command databricks -ErrorAction SilentlyContinue)) {
      Die "The CLI was installed but is not on PATH in this session. Open a NEW PowerShell window and re-run."
    }
  }
  # bundles need the modern unified CLI (v0.205+)
  & databricks bundle version *> $null
  if ($LASTEXITCODE -ne 0) {
    & databricks bundle --help *> $null
    if ($LASTEXITCODE -ne 0) {
      Die "Your 'databricks' command does not support bundles (legacy CLI?). Install the current CLI:
      https://docs.databricks.com/dev-tools/cli/install.html"
    }
  }
  Ok ("Databricks CLI: " + ((& databricks --version 2>$null) | Select-Object -First 1))
}

# A stable, per-workspace profile name derived from the host URL, e.g.
#   https://dbc-1234abcd-5678.cloud.databricks.com -> gastwin-dbc-1234abcd-5678
function Twin-Profile([string]$HostUrl) { 'gastwin-' + ((($HostUrl -replace '^https?://','') -split '[./:]')[0]) }

function Ensure-Auth {
  param([string]$Profile, [string]$WorkspaceUrl)
  # If a workspace URL is given, sign in to exactly that workspace (a browser opens). The profile is
  # unique to that workspace, so this never conflicts. If no URL is given, rely on an existing sign-in.
  if ($WorkspaceUrl) {
    Step "Logging in to $WorkspaceUrl (profile: $Profile; a browser window will open)"
    & databricks auth login --host $WorkspaceUrl --profile $Profile
    & databricks current-user me -p $Profile *> $null
    if ($LASTEXITCODE -ne 0) { Die "OAuth login did not complete. Confirm the workspace URL and your admin access." }
    $me = Db-Json current-user me -p $Profile
    Ok ("Authenticated as " + $me.userName + " (profile: $Profile)")
    return
  }
  & databricks current-user me -p $Profile *> $null
  if ($LASTEXITCODE -ne 0) {
    Die "Profile '$Profile' is not signed in and no -WorkspaceUrl was given.
      Re-run with your workspace URL, e.g.:  -WorkspaceUrl https://your-workspace.cloud.databricks.com"
  }
  $me = Db-Json current-user me -p $Profile
  Ok ("Authenticated as " + $me.userName + " (profile: $Profile)")
}
