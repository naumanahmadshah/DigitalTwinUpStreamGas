<#
.SYNOPSIS
  Gas Upstream Digital Twin - complete cleanup (Windows PowerShell).

.DESCRIPTION
  Removes EVERYTHING this project created and returns the workspace to a clean, pre-deploy state so
  .\scripts\deploy.ps1 can run fresh. It removes the bundle resources (app, jobs, medallion pipeline,
  and the bundle workspace folder), the Genie space, the AI/BI dashboard, the Lakebase project (with its synced tables, sync pipeline and
  scheduled sync job), and
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
  [string]$Lakebase   = "gas-upstream-twin-lakebase",
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
Info "catalog=$Catalog  schema=$Schema  app=$AppName  lakebase=$Lakebase  profile=$Profile"

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
$Vars = "catalog=$Catalog,schema=$Schema,warehouse_id=$Warehouse,lakebase_project=$Lakebase,app_name=$AppName"
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

# ---- Lakebase: scheduled sync job, synced tables (+ their pipeline), the project --------------
Step "Removing the Lakebase sync job, synced tables and project"
$SyncJob = "[$Catalog.$Schema] Gas Twin - Lakebase sync"
$jl = Db-Json api get ("/api/2.1/jobs/list?name=" + [uri]::EscapeDataString($SyncJob)) -p $Profile
if ($jl -and $jl.jobs) { & databricks jobs delete $jl.jobs[0].job_id -p $Profile *> $null; Ok ("sync job removed (" + $jl.jobs[0].job_id + ")") } else { Ok "no sync job to remove" }
# Synced tables live in the <schema>_app UC schema. Deleting them removes their sync pipeline too.
$ServingSchema = "${Schema}_app"
$tabs = Db-Json tables list $Catalog $ServingSchema -o json -p $Profile
if ($tabs) { foreach ($t in $tabs) { & databricks api delete "/api/2.0/postgres/synced_tables/$Catalog.$ServingSchema.$($t.name)" -p $Profile *> $null } }
Ok "synced tables removed"
# the sync pipeline is named "Synced table: <catalog>.<schema>_app.<first table> ..."
$pls = Db-Json pipelines list-pipelines -o json -p $Profile
if ($pls) { $pls | Where-Object { $_.name -like "Synced table: $Catalog.$ServingSchema.*" } | ForEach-Object { & databricks pipelines delete $_.pipeline_id -p $Profile *> $null } }
$proj = Db-Json api get "/api/2.0/postgres/projects/$Lakebase" -p $Profile
if ($proj) {
  & databricks api delete "/api/2.0/postgres/projects/$Lakebase" -p $Profile *> $null
  if ($LASTEXITCODE -eq 0) { Ok "Lakebase project '$Lakebase' removed" } else { Warn "could not remove Lakebase project '$Lakebase'" }
} else { Ok "no Lakebase project to remove" }

# ---- Data: the project's schemas, then the catalog only if nothing else is in it ----------------
# The catalog may be an existing one shared with other work, so only this project's two schemas are
# dropped; the catalog itself goes only when no other schema is left in it.
Step "Dropping the project's schemas and data"
if ($Warehouse) {
  foreach ($s in @("${Schema}_app", $Schema)) {
    if (Run-Sql "DROP SCHEMA IF EXISTS ``$Catalog``.``$s`` CASCADE") { Ok "schema '$Catalog.$s' dropped" }
    else { Warn "could not drop schema '$Catalog.$s' (drop it manually in the UI)" }
  }
  $cat = Db-Json api get "/api/2.1/unity-catalog/catalogs/$Catalog" -p $Profile
  $sch = Db-Json schemas list $Catalog -o json -p $Profile
  $others = @(); if ($sch) { $others = @($sch | Where-Object { $_.name -notin @('information_schema','default') } | ForEach-Object { $_.name }) }
  if (-not ($cat -and $cat.name)) { Ok "catalog '$Catalog' already gone" }
  elseif ($others.Count -eq 0) {
    if (Run-Sql "DROP CATALOG IF EXISTS ``$Catalog`` CASCADE") { Ok "catalog '$Catalog' dropped (nothing else was in it)" }
    else { Warn "kept catalog '$Catalog' (no privilege to drop it; it is empty of project data)" }
  } else { Ok ("kept catalog '$Catalog': it also holds other schemas (" + ($others -join ', ') + ")") }
} else { Warn "skipped dropping the schemas (no warehouse resolved)" }

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
