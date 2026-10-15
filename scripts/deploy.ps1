<#
.SYNOPSIS
  Gas Upstream Digital Twin — one-command deployment (Windows PowerShell).

.DESCRIPTION
  From a raw Windows computer with an internet connection and a Databricks workspace where you
  have workspace-admin rights, this script installs the Databricks CLI (if missing), signs you in,
  creates the catalog/schema/landing volume, finds or creates a serverless SQL warehouse, creates
  the Lakebase instance, deploys the bundle, generates the data, runs the medallion, builds the
  Genie space + dashboard, starts the app and prints the app URL + catalog + Genie link.

.EXAMPLE
  .\scripts\deploy.ps1 -WorkspaceUrl https://your-workspace.cloud.databricks.com
#>
param(
  [string]$WorkspaceUrl = "",
  [string]$Catalog      = "gas_upstream_digital_twin",
  [string]$Schema       = "digital_twin",
  [string]$Warehouse    = "",
  [string]$DbInstance   = "gas-upstream-twin-db",
  [string]$AppName      = "gas-upstream-twin",
  [string]$Profile      = "",
  [switch]$SkipData
)
$ErrorActionPreference = 'Stop'
$Here = Split-Path -Parent $MyInvocation.MyCommand.Path
$Root = Split-Path -Parent $Here
. (Join-Path $Here 'lib\common.ps1')
Set-Location $Root
$GenieTitle = "Gas Upstream Digital Twin"   # must match TITLE in serving/build_genie.py
if ($WorkspaceUrl) { $m = [regex]::Match($WorkspaceUrl, '^(https?://[^/?]+)'); if ($m.Success) { $WorkspaceUrl = $m.Groups[1].Value } }

Write-Host "== Gas Upstream Digital Twin - deploy ==" -ForegroundColor White
Info "catalog=$Catalog  schema=$Schema  app=$AppName  db=$DbInstance"

function Vars { "catalog=$Catalog,schema=$Schema,warehouse_id=$Warehouse,db_instance=$DbInstance,app_name=$AppName" }
function BDeploy { & databricks bundle deploy -t main -p $Profile --var (Vars); if ($LASTEXITCODE -ne 0) { Die "bundle deploy failed" } }
# BRun streams CLI output to the host (not captured) and leaves the exit code in $LASTEXITCODE,
# which the caller checks. Do not capture BRun's output, or the CLI progress would be swallowed.
function BRun { param([string]$Key,[string[]]$Extra) & databricks bundle run $Key -t main -p $Profile --var (Vars) @Extra }
# Run one SQL statement on the warehouse, polling to completion. Returns $true on SUCCEEDED.
# Using SQL for catalog/schema/volume DDL works on standard metastores AND Default Storage workspaces.
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

# ---- Phase 0: prerequisites + CLI -----------------------------------------------------------
Step "Phase 0 - prerequisites"
Ensure-Cli

# ---- Phase 1: authentication ----------------------------------------------------------------
Step "Phase 1 - authentication"
# A workspace URL is the only required input. Prompt for it if not given, so a bare
# `.\scripts\deploy.ps1` still works.
if ((-not $WorkspaceUrl) -and (-not $Profile)) {
  $WorkspaceUrl = (Read-Host "    Enter your Databricks workspace URL (e.g. https://your-workspace.cloud.databricks.com)")
  $m = [regex]::Match($WorkspaceUrl, '^(https?://[^/?]+)'); if ($m.Success) { $WorkspaceUrl = $m.Groups[1].Value }
}
# Derive a per-workspace profile from the host unless one was given explicitly with -Profile.
if (-not $Profile) { if ($WorkspaceUrl) { $Profile = Twin-Profile $WorkspaceUrl } else { $Profile = 'gas-twin' } }
Ensure-Auth -Profile $Profile -WorkspaceUrl $WorkspaceUrl

# Resolve the workspace host once (for the stale-state guard below and the final summary).
$WsHost = $WorkspaceUrl
if (-not $WsHost) {
  try { $WsHost = (& databricks auth describe -p $Profile 2>$null | Select-String -Pattern '^\s*Host:\s*(.+)$' | Select-Object -First 1).Matches.Groups[1].Value } catch { $WsHost = "" }
}
# Guard: local bundle state (.databricks) from a deploy to a DIFFERENT workspace can crash the CLI's
# deployment engine. If the recorded host differs from this one, clear the stale state first.
$StateMarker = ".databricks/.deploy_host"
if ((Test-Path $StateMarker) -and $WsHost -and ((Get-Content $StateMarker -Raw -ErrorAction SilentlyContinue).Trim() -ne $WsHost)) {
  Warn "workspace changed since the last local deploy - clearing stale bundle state (.databricks)"
  Remove-Item -Recurse -Force .databricks -ErrorAction SilentlyContinue
}

# ---- Phase 2: SQL warehouse (used for DDL below + by the app) --------------------------------
Step "Phase 2 - SQL warehouse"
if (-not $Warehouse) {
  $whs = Db-Json warehouses list -o json -p $Profile
  if ($whs) {
    $run = $whs | Where-Object { $_.state -eq 'RUNNING' } | Select-Object -First 1
    if ($run) { $Warehouse = $run.id } else { $Warehouse = ($whs | Select-Object -First 1).id }
  }
}
if (-not $Warehouse) {
  Info "no warehouse found - creating a small serverless warehouse..."
  $body = '{"name":"Gas Twin Warehouse","cluster_size":"2X-Small","max_num_clusters":1,"auto_stop_mins":10,"enable_serverless_compute":true,"warehouse_type":"PRO"}'
  $wh = Db-Json warehouses create --json $body -o json -p $Profile
  if ($wh) { $Warehouse = $wh.id }
  if (-not $Warehouse) { Die "Could not auto-create a SQL warehouse. Create one in the UI (SQL > SQL Warehouses) and pass -Warehouse <id>." }
}
Ok "warehouse: $Warehouse"

# ---- Phase 2b: catalog, schema, landing volume (via SQL - Default-Storage-safe) --------------
# The landing volume must exist before data generation: batch_sources writes files into it, and
# os.makedirs cannot create the UC volume object itself.
Step "Phase 2 - catalog, schema and landing volume"
if (Run-Sql "CREATE CATALOG IF NOT EXISTS ``$Catalog``") { Ok "catalog '$Catalog' ready" }
else { Die "Could not create catalog '$Catalog'. If your account uses Default Storage, create it once in the UI
      (Catalog > Create catalog, Default Storage) or pass -Catalog <existing>, then re-run." }
if (Run-Sql "CREATE SCHEMA IF NOT EXISTS ``$Catalog``.``$Schema``") { Ok "schema '$Schema' ready" } else { Die "Could not create schema '$Catalog.$Schema'." }
if (Run-Sql "CREATE VOLUME IF NOT EXISTS ``$Catalog``.``$Schema``.landing") { Ok "volume 'landing' ready" } else { Die "Could not create volume '$Catalog.$Schema.landing'." }

# ---- Phase 3: Lakebase instance -------------------------------------------------------------
Step "Phase 3 - Lakebase instance (write-back)"
$inst = Db-Json api get "/api/2.0/database/instances/$DbInstance" -p $Profile
$PgHost = if ($inst) { $inst.read_write_dns } else { "" }
if (-not $PgHost) {
  Info "creating Lakebase instance '$DbInstance' (provisioning takes a few minutes)..."
  & databricks database create-database-instance $DbInstance --capacity CU_1 -p $Profile *> $null
  for ($i=0; $i -lt 40; $i++) {
    $inst = Db-Json api get "/api/2.0/database/instances/$DbInstance" -p $Profile
    $PgHost = if ($inst) { $inst.read_write_dns } else { "" }
    if ($PgHost) { break }
    Start-Sleep -Seconds 15
  }
}
if (-not $PgHost) { Die "Lakebase instance '$DbInstance' did not become ready. Lakebase (Database Instances) must be
      available in this workspace/region. See docs/TROUBLESHOOTING.md, then re-run." }
Ok "Lakebase host: $PgHost"

# ---- Phase 4: render app.yaml + deploy bundle -----------------------------------------------
Step "Phase 4 - deploy bundle (pipeline, jobs, app)"
function Render-AppYaml([string]$GenieId) {
  (Get-Content (Join-Path $Root 'app\app.yaml.tmpl') -Raw).
    Replace('__CATALOG__',$Catalog).Replace('__SCHEMA__',$Schema).
    Replace('__DB_INSTANCE__',$DbInstance).Replace('__PGHOST__',$PgHost).
    Replace('__GENIE_SPACE__',$GenieId) | Set-Content (Join-Path $Root 'app\app.yaml') -NoNewline
}
Render-AppYaml ""
BDeploy
Ok "bundle deployed"
# Record which workspace this local state belongs to (used by the stale-state guard above).
New-Item -ItemType Directory -Force -Path .databricks | Out-Null
Set-Content -Path $StateMarker -Value $WsHost -NoNewline -ErrorAction SilentlyContinue

$app = Db-Json api get "/api/2.0/apps/$AppName" -p $Profile
$AppSp = if ($app) { $app.service_principal_client_id } else { "" }
if ($AppSp) {
  Run-Sql "GRANT USE CATALOG ON CATALOG ``$Catalog`` TO ``$AppSp``" | Out-Null
  Run-Sql "GRANT USE SCHEMA ON SCHEMA ``$Catalog``.``$Schema`` TO ``$AppSp``" | Out-Null
  Run-Sql "GRANT SELECT ON SCHEMA ``$Catalog``.``$Schema`` TO ``$AppSp``" | Out-Null
  Ok "granted app service principal SELECT on $Catalog.$Schema"
} else { Warn "could not resolve the app service principal yet; grants retried on next deploy" }

# ---- Phase 5: data generation ---------------------------------------------------------------
if (-not $SkipData) {
  Step "Phase 5 - generating data (longest step, ~10-20 min)"
  & (Join-Path $Here 'generate_data.ps1') -Profile $Profile -Catalog $Catalog -Schema $Schema -Warehouse $Warehouse -DbInstance $DbInstance -AppName $AppName
  if ($LASTEXITCODE -ne 0) { Die "data generation failed" }
  Ok "data generated"
} else { Warn "Phase 5 - skipping data generation (-SkipData)" }

# ---- Phase 6: medallion ---------------------------------------------------------------------
Step "Phase 6 - medallion pipeline (bronze -> silver -> gold)"
BRun 'medallion' @('--full-refresh-all')
if ($LASTEXITCODE -ne 0) { Die "medallion pipeline failed" }
Ok "medallion complete"

# ---- Phase 7: surfaces ----------------------------------------------------------------------
Step "Phase 7 - Genie space + AI/BI dashboard"
BRun 'surfaces' @()
if ($LASTEXITCODE -eq 0) { Ok "surfaces built" } else { Warn "surfaces hit an issue; the app still works. See docs/TROUBLESHOOTING.md." }
# A newly created Genie space can take a few seconds to appear in the spaces list, so poll for it.
$GenieId = ""
for ($i = 0; $i -lt 12; $i++) {
  $spaces = Db-Json api get /api/2.0/genie/spaces -p $Profile
  if ($spaces -and $spaces.spaces) {
    $match = $spaces.spaces | Where-Object { $_.title -eq $GenieTitle } | Select-Object -First 1
    if ($match) { $GenieId = if ($match.space_id) { $match.space_id } else { $match.id }; break }
  }
  Start-Sleep -Seconds 6
}
if ($GenieId) { Ok "Genie space: $GenieId" } else { Warn "Genie space id not found; the Field Copilot will show a 'not wired yet' note." }

# ---- Phase 8: start the app, wired to Genie -------------------------------------------------
Step "Phase 8 - start the app"
Render-AppYaml $GenieId
BDeploy
BRun 'app' @()
if ($LASTEXITCODE -ne 0) { Die "app failed to start" }
if ($GenieId -and $AppSp) {
  $pb = @{ access_control_list=@(@{ service_principal_name=$AppSp; permission_level='CAN_RUN' }) } | ConvertTo-Json -Compress -Depth 5
  & databricks api patch "/api/2.0/permissions/genie/$GenieId" --json $pb -p $Profile *> $null
  if ($LASTEXITCODE -eq 0) { Ok "app can query the Genie space" } else { Warn "could not grant the app CAN_RUN on Genie (set it in the UI if the Copilot errors)" }
}

# ---- Phase 9: Lakebase seed -----------------------------------------------------------------
Step "Phase 9 - seed Lakebase (write-back table + grants)"
BRun 'lakebase_seed' @()
if ($LASTEXITCODE -eq 0) { Ok "Lakebase seeded" } else { Warn "Lakebase seed hit an issue; write-back may be read-only. See docs/TROUBLESHOOTING.md." }

# ---- Phase 10: summary ----------------------------------------------------------------------
$app = Db-Json api get "/api/2.0/apps/$AppName" -p $Profile
$AppUrl = if ($app) { $app.url } else { "" }
# $WsHost was resolved in Phase 1.
Write-Host ""
Write-Host "============================================================" -ForegroundColor Green
Write-Host " Deployment complete." -ForegroundColor White
Write-Host "============================================================" -ForegroundColor Green
Write-Host ("  App:        " + ($(if ($AppUrl) { $AppUrl } else { "(open Compute > Apps > $AppName)" })))
Write-Host ("  Catalog:    $Catalog.$Schema   (open Catalog to browse the marts)")
if ($GenieId -and $WsHost) { Write-Host ("  Genie:      $WsHost/genie/rooms/$GenieId") }
Write-Host ("  Dashboard:  open Dashboards > `"$GenieTitle`"")
Write-Host ("  Warehouse:  $Warehouse")
Write-Host ""
Write-Host "  New here? Read docs/NAVIGATION.md for a guided tour of the app."
Write-Host ""
