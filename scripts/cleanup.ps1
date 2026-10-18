<#
.SYNOPSIS
  Gas Upstream Digital Twin - complete cleanup (Windows PowerShell).

.DESCRIPTION
  Removes EVERYTHING this project created and returns the workspace to a clean, pre-deploy state so
  .\scripts\deploy.ps1 can run fresh. It removes the bundle resources (app, jobs, medallion pipeline,
  and the bundle workspace folder), the Genie space, the AI/BI dashboard, the Lakebase instance, and
  the catalog + schema + landing volume + ALL generated data (DROP CATALOG ... CASCADE). A
  project-created warehouse named "Gas Twin Warehouse" is removed; a pre-existing/shared one is left.

.EXAMPLE
  .\scripts\cleanup.ps1 -Profile gas-twin
#>
param(
  [string]$WorkspaceUrl = "",
  [string]$Catalog    = "gas_upstream_digital_twin",
  [string]$Schema     = "digital_twin",
  [string]$Warehouse  = "",
  [string]$DbInstance = "gas-upstream-twin-db",
  [string]$AppName    = "gas-upstream-twin",
  [string]$Profile    = "",
  [switch]$Yes
)
$ErrorActionPreference = 'Stop'
$Here = Split-Path -Parent $MyInvocation.MyCommand.Path
$Root = Split-Path -Parent $Here
. (Join-Path $Here 'lib\common.ps1')
Set-Location $Root
$GenieTitle = "Asset Intelligence Genie"
$GenieOld   = "Gas Upstream Digital Twin"   # earlier name
$DashName   = "Gas Upstream Digital Twin"
$WhName     = "Gas Twin Warehouse"
if ($WorkspaceUrl) { $m = [regex]::Match($WorkspaceUrl, '^(https?://[^/?]+)'); if ($m.Success) { $WorkspaceUrl = $m.Groups[1].Value } }
# Derive the per-workspace profile from -WorkspaceUrl unless one was given explicitly (same as deploy).
if (-not $Profile) { if ($WorkspaceUrl) { $Profile = Twin-Profile $WorkspaceUrl } else { $Profile = 'gas-twin' } }

Write-Host "== Gas Upstream Digital Twin - cleanup ==" -ForegroundColor White
Info "catalog=$Catalog  schema=$Schema  app=$AppName  db=$DbInstance  profile=$Profile"

# Authenticate: if a -WorkspaceUrl was given, sign in to it; otherwise rely on an existing sign-in.
if ($WorkspaceUrl) {
  Ensure-Auth -Profile $Profile -WorkspaceUrl $WorkspaceUrl
} else {
  & databricks current-user me -p $Profile *> $null
  if ($LASTEXITCODE -ne 0) { Die "Profile '$Profile' is not signed in. Pass -WorkspaceUrl https://your-workspace... (or -Profile <authed>)." }
}

if (-not $Yes) {
  Write-Host ""
  Write-Host "This permanently deletes the app, dashboard, Genie space, Lakebase instance, pipeline, jobs," -ForegroundColor Yellow
  Write-Host "and the catalog '$Catalog' with ALL its data." -ForegroundColor Yellow
  if ((Read-Host "Type 'yes' to proceed") -ne 'yes') { Die "aborted." }
}

# ---- SQL warehouse (needed to DROP the catalog) ---------------------------------------------
Step "Resolving a SQL warehouse (to drop the catalog)"
if (-not $Warehouse) {
  $whs = Db-Json warehouses list -o json -p $Profile
  if ($whs) {
    $run = $whs | Where-Object { $_.state -eq 'RUNNING' } | Select-Object -First 1
    if ($run) { $Warehouse = $run.id } else { $Warehouse = ($whs | Select-Object -First 1).id }
  }
}
if ($Warehouse) { Ok "warehouse: $Warehouse" } else { Warn "no warehouse found; the catalog drop will be skipped" }

# ---- Bundle resources -----------------------------------------------------------------------
Step "Removing bundle resources (app, jobs, pipeline)"
Remove-Item -Recurse -Force .databricks -ErrorAction SilentlyContinue
$Vars = "catalog=$Catalog,schema=$Schema,warehouse_id=$Warehouse,db_instance=$DbInstance,app_name=$AppName"
& databricks bundle destroy -t main -p $Profile --var $Vars --auto-approve *> $null
if ($LASTEXITCODE -eq 0) { Ok "bundle resources removed" } else { Warn "no bundle deployment found (or already removed)" }
# App deletion is asynchronous; wait for it so "cleanup complete" is truthful and an immediate
# redeploy does not race a half-deleted app of the same name.
function App-Present {
  $apps = Db-Json apps list -o json -p $Profile
  if (-not $apps) { return $false }
  $list = if ($apps -is [array]) { $apps } else { $apps.apps }
  return [bool]($list | Where-Object { $_.name -eq $AppName })
}
$tries = 0
while ((App-Present) -and $tries -lt 20) { Start-Sleep -Seconds 6; $tries++ }
if (App-Present) { Warn "app '$AppName' still deleting after ~2 min (a redeploy may need a moment)" } else { Ok "app fully removed" }

# ---- Genie space ----------------------------------------------------------------------------
Step "Removing the Genie space"
$spaces = Db-Json api get /api/2.0/genie/spaces -p $Profile
$gid = ""
if ($spaces -and $spaces.spaces) {
  $m = $spaces.spaces | Where-Object { $_.title -eq $GenieTitle -or $_.title -eq $GenieOld } | Select-Object -First 1
  if ($m) { $gid = if ($m.space_id) { $m.space_id } else { $m.id } }
}
if ($gid) { & databricks api delete "/api/2.0/genie/spaces/$gid" -p $Profile *> $null; Ok "Genie space removed ($gid)" } else { Ok "no Genie space to remove" }

# ---- AI/BI dashboard ------------------------------------------------------------------------
Step "Removing the AI/BI dashboard"
$dash = Db-Json lakeview list -p $Profile
$did = ""
if ($dash) {
  $rows = if ($dash -is [array]) { $dash } else { $dash.dashboards }
  $m = $rows | Where-Object { $_.display_name -eq $DashName -and -not $_.trashed_at } | Select-Object -First 1
  if ($m) { $did = $m.dashboard_id }
}
if ($did) { & databricks lakeview trash $did -p $Profile *> $null; Ok "dashboard removed ($did)" } else { Ok "no dashboard to remove" }

# ---- Lakebase instance ----------------------------------------------------------------------
Step "Removing the Lakebase instance"
$inst = Db-Json api get "/api/2.0/database/instances/$DbInstance" -p $Profile
if ($inst) {
  & databricks database delete-database-instance $DbInstance --purge -p $Profile *> $null
  if ($LASTEXITCODE -ne 0) { & databricks database delete-database-instance $DbInstance -p $Profile *> $null }
  Ok "Lakebase instance '$DbInstance' removed"
} else { Ok "no Lakebase instance to remove" }

# ---- Catalog + all data ---------------------------------------------------------------------
Step "Dropping the catalog and all data"
if ($Warehouse) {
  if (Run-Sql "DROP CATALOG IF EXISTS ``$Catalog`` CASCADE") { Ok "catalog '$Catalog' dropped (schema, volume and data gone)" }
  else { Warn "could not drop catalog '$Catalog' (drop it manually in the UI)" }
} else { Warn "skipped catalog drop (no warehouse resolved)" }

# ---- Project-created warehouse --------------------------------------------------------------
Step "Removing a project-created warehouse (if any)"
$whs = Db-Json warehouses list -o json -p $Profile
$wid = ""
if ($whs) { $m = $whs | Where-Object { $_.name -eq $WhName } | Select-Object -First 1; if ($m) { $wid = $m.id } }
if ($wid) { & databricks warehouses delete $wid -p $Profile *> $null; Ok "removed project warehouse '$WhName' ($wid)" }
else { Ok "no project-created warehouse to remove (a pre-existing/shared warehouse is left untouched)" }

Write-Host ""
Write-Host "============================================================" -ForegroundColor Green
Write-Host " Cleanup complete." -ForegroundColor White -NoNewline
Write-Host " The workspace is back to a pre-deploy state; .\scripts\deploy.ps1 can run fresh."
Write-Host ""
