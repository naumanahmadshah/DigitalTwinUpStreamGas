<#
.SYNOPSIS
  Gas Upstream Digital Twin: one-command deployment (Windows PowerShell).

.DESCRIPTION
  From a raw Windows computer with an internet connection and a Databricks workspace where you
  have workspace-admin rights, this script installs the Databricks CLI (if missing), signs you in,
  creates the catalog/schema/landing volume, finds or creates a serverless SQL warehouse, creates
  the Lakebase project (scale to zero; the app's only database), deploys the bundle, generates the
  data, runs the medallion, lands the gold marts in Lakebase (synced tables, re-synced every 5
  minutes), builds the Genie space + dashboard, starts the app and prints the app URL + catalog +
  Genie link.

.EXAMPLE
  .\scripts\deploy.ps1 -WorkspaceUrl https://your-workspace.cloud.databricks.com
#>
param(
  [string]$WorkspaceUrl = "",
  [string]$Catalog      = "gas_upstream_digital_twin",
  [string]$Schema       = "digital_twin",
  [string]$Warehouse    = "",
  [string]$Lakebase     = "gas-upstream-twin-lakebase",
  [string]$SyncCron     = "0 0/5 * * * ?",
  [string]$AppName      = "gas-upstream-twin",
  [string]$Profile      = "",
  [switch]$SkipData
)
$ErrorActionPreference = 'Stop'
$Here = Split-Path -Parent $MyInvocation.MyCommand.Path
$Root = Split-Path -Parent $Here
. (Join-Path $Here 'lib\common.ps1')
Set-Location $Root
$GenieTitle = "Asset Intelligence Genie"    # must match TITLE in serving/build_genie.py
$DashName   = "Gas Upstream Digital Twin"   # must match NAME in serving/build_dashboard.py
if ($WorkspaceUrl) { $m = [regex]::Match($WorkspaceUrl, '^(https?://[^/?]+)'); if ($m.Success) { $WorkspaceUrl = $m.Groups[1].Value } }

Write-Host "== Gas Upstream Digital Twin - deploy ==" -ForegroundColor White
Info "catalog=$Catalog  schema=$Schema  app=$AppName  lakebase=$Lakebase"

function Vars { "catalog=$Catalog,schema=$Schema,warehouse_id=$Warehouse,lakebase_project=$Lakebase,sync_cron=$SyncCron,app_name=$AppName" }
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
Step "Phase 0: prerequisites"
Ensure-Cli

# ---- Phase 1: authentication ----------------------------------------------------------------
Step "Phase 1: authentication"
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

# ---- Phase 2: SQL warehouse (used for DDL below, Genie and the dashboard; not by the app) -----
Step "Phase 2: SQL warehouse"
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

# ---- Phase 3: catalog, schema, landing volume (via SQL - Default-Storage-safe) --------------
# The landing volume must exist before data generation: batch_sources writes files into it, and
# os.makedirs cannot create the UC volume object itself.
Step "Phase 3: catalog, schema and landing volume"
# Use an existing catalog / schema as is: CREATE ... IF NOT EXISTS still needs the CREATE privilege
# (checked before existence), which a user given an existing catalog often does not have.
function Uc-Exists([string]$Kind, [string]$Name) { $o = Db-Json api get "/api/2.1/unity-catalog/$Kind/$Name" -p $Profile; return [bool]($o -and $o.name) }
if (Uc-Exists 'catalogs' $Catalog) { Ok "using existing catalog '$Catalog'" }
elseif (Run-Sql "CREATE CATALOG IF NOT EXISTS ``$Catalog``") { Ok "catalog '$Catalog' created" }
else { Die "Could not create catalog '$Catalog'. If your account uses Default Storage, create it once in the UI
      (Catalog > Create catalog, Default Storage) or pass -Catalog <existing>, then re-run." }
if (Uc-Exists 'schemas' "$Catalog.$Schema") { Ok "using existing schema '$Schema'" }
elseif (Run-Sql "CREATE SCHEMA IF NOT EXISTS ``$Catalog``.``$Schema``") { Ok "schema '$Schema' ready" }
else { Die "Could not create schema '$Catalog.$Schema'. You need USE CATALOG and CREATE SCHEMA on '$Catalog'." }
if (Run-Sql "CREATE VOLUME IF NOT EXISTS ``$Catalog``.``$Schema``.landing") { Ok "volume 'landing' ready" } else { Die "Could not create volume '$Catalog.$Schema.landing'." }

# ---- Phase 4: Lakebase project (the app's only database; needed before the app binds to it) --
# Lakebase Autoscaling: 0.5 to 4 CU, and the compute scales to zero after 5 idle minutes.
Step "Phase 4: Lakebase project (scale to zero)"
$PgSchema = "${Schema}_app"   # the gold marts land here as Lakebase synced tables (same names as in gold)
function Lb-Endpoint {
  $eps = Db-Json api get "/api/2.0/postgres/projects/$Lakebase/branches/production/endpoints" -p $Profile
  if ($eps -and $eps.endpoints) {
    $e = $eps.endpoints | Where-Object { $_.status.endpoint_type -eq 'ENDPOINT_TYPE_READ_WRITE' } | Select-Object -First 1
    if ($e) { return $e.name }
  }
  return ""
}
$PgEndpoint = Lb-Endpoint
if (-not $PgEndpoint) {
  Info "creating Lakebase project '$Lakebase' (takes a minute)..."
  # a project deleted earlier is kept soft-deleted and still holds its name: when no live project has the
  # name, purge any soft-deleted one so the name is free (a live, still-provisioning project is never touched)
  $live = Db-Json api get "/api/2.0/postgres/projects/$Lakebase" -p $Profile
  if (-not ($live -and $live.project_id)) { & databricks api delete "/api/2.0/postgres/projects/$Lakebase`?purge=true" -p $Profile *> $null }
  $body = @{ spec = @{ display_name = $Lakebase; pg_version = 16; default_endpoint_settings = @{
            autoscaling_limit_min_cu = 0.5; autoscaling_limit_max_cu = 4; suspend_timeout_duration = '300s' } } } | ConvertTo-Json -Depth 5 -Compress
  & databricks api post "/api/2.0/postgres/projects?project_id=$Lakebase" --json $body -p $Profile *> $null
  for ($i=0; $i -lt 40; $i++) {
    $PgEndpoint = Lb-Endpoint
    if ($PgEndpoint) { break }
    Start-Sleep -Seconds 10
  }
}
if (-not $PgEndpoint) { Die "Lakebase project '$Lakebase' did not become ready. Lakebase Autoscaling must be
      available in this workspace/region. See docs/TROUBLESHOOTING.md, then re-run." }
Ok "Lakebase endpoint: $PgEndpoint"

# ---- Phase 5: render app.yaml + deploy bundle -----------------------------------------------
Step "Phase 5: deploy bundle (pipeline, jobs, app)"
function Render-AppYaml([string]$GenieId) {
  (Get-Content (Join-Path $Root 'app\app.yaml.tmpl') -Raw).
    Replace('__SCHEMA__',$Schema).Replace('__PG_SCHEMA__',$PgSchema).
    Replace('__PG_ENDPOINT__',$PgEndpoint).
    Replace('__GENIE_SPACE__',$GenieId) | Set-Content (Join-Path $Root 'app\app.yaml') -NoNewline
}
Render-AppYaml ""
BDeploy
Ok "bundle deployed"
# Record which workspace this local state belongs to (used by the stale-state guard above).
New-Item -ItemType Directory -Force -Path .databricks | Out-Null
Set-Content -Path $StateMarker -Value $WsHost -NoNewline -ErrorAction SilentlyContinue

# ---- Phase 6: data generation ---------------------------------------------------------------
if (-not $SkipData) {
  Step "Phase 6: generating data (longest step, ~10-20 min)"
  & (Join-Path $Here 'generate_data.ps1') -Profile $Profile -Catalog $Catalog -Schema $Schema -Warehouse $Warehouse -Lakebase $Lakebase -AppName $AppName
  if ($LASTEXITCODE -ne 0) { Die "data generation failed" }
  Ok "data generated"
} else { Warn "Phase 6: skipping data generation (-SkipData)" }

# ---- Phase 7: medallion ---------------------------------------------------------------------
Step "Phase 7: medallion pipeline (bronze -> silver -> gold)"
BRun 'medallion' @('--full-refresh-all')
if ($LASTEXITCODE -ne 0) { Die "medallion pipeline failed" }
Ok "medallion complete"

# ---- Phase 8: Lakebase sync, right after the medallion -----------------------------------------
# One synced table per gold mart (one sync pipeline), synced now; seeds the recommendation write-back
# table, grants the app, and creates the "Lakebase sync" job that re-runs the sync every 5 minutes.
Step "Phase 8: land the gold marts in Lakebase (synced tables) + schedule the re-sync"
BRun 'lakebase_sync' @()
if ($LASTEXITCODE -ne 0) { Die "The Lakebase sync failed, so the app would have no data. See docs/TROUBLESHOOTING.md, then re-run." }
Ok "gold marts synced to Lakebase (Postgres schema $PgSchema); re-sync scheduled"

# ---- Phase 9: surfaces (Genie + dashboard, over gold in the lakehouse) -----------------------
Step "Phase 9: Genie space + AI/BI dashboard"
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
if ($GenieId) { Ok "Genie space: $GenieId" } else { Warn "Genie space id not found; the app's Genie lamp link will be hidden until a redeploy finds it." }

# ---- Phase 10: start the app, wired to Genie + Lakebase -------------------------------------
Step "Phase 10: start the app"
Render-AppYaml $GenieId
BDeploy
BRun 'app' @()
if ($LASTEXITCODE -ne 0) { Die "app failed to start" }

# ---- Summary -------------------------------------------------------------------------------
$app = Db-Json api get "/api/2.0/apps/$AppName" -p $Profile
$AppUrl = if ($app) { $app.url } else { "" }
# $WsHost was resolved in Phase 1.
Write-Host ""
Write-Host "============================================================" -ForegroundColor Green
Write-Host " Deployment complete." -ForegroundColor White
Write-Host "============================================================" -ForegroundColor Green
Write-Host ("  App:        " + ($(if ($AppUrl) { $AppUrl } else { "(open Compute > Apps > $AppName)" })))
Write-Host ("  Catalog:    $Catalog.$Schema   (open Catalog to browse the marts)")
if ($GenieId -and $WsHost) { Write-Host ("  Genie:      $WsHost/one/agents/$GenieId") }
Write-Host ("  Dashboard:  open Dashboards > `"$DashName`"")
Write-Host ("  Lakebase:   $Lakebase  (Postgres schema $PgSchema; re-sync job `"[$Catalog.$Schema] Gas Twin - Lakebase sync`")")
Write-Host ("  Warehouse:  $Warehouse  (Genie + dashboard)")
Write-Host ""
Write-Host "  New here? Read docs/NAVIGATION.md for a guided tour of the app."
Write-Host ""
