<#
.SYNOPSIS
  Gas Upstream Digital Twin - data generation (Windows PowerShell).

.DESCRIPTION
  Fires the "setup" job in the deployed bundle: builds the semantic backbone and backfills ~2 years
  of telemetry/events plus the batch, reference and file sources. deploy.ps1 calls this automatically;
  you can also run it on its own to regenerate the data after a config change.
  Prerequisite: the bundle is already deployed (scripts\deploy.ps1) and you are signed in.

.EXAMPLE
  .\scripts\generate_data.ps1 -Profile gas-twin
#>
param(
  [string]$Catalog    = "gas_upstream_digital_twin",
  [string]$Schema     = "digital_twin",
  [string]$Warehouse  = "",
  [string]$DbInstance = "gas-upstream-twin-db",
  [string]$AppName    = "gas-upstream-twin",
  [string]$Profile    = "gas-twin"
)
$ErrorActionPreference = 'Stop'
$Here = Split-Path -Parent $MyInvocation.MyCommand.Path
$Root = Split-Path -Parent $Here
. (Join-Path $Here 'lib\common.ps1')
Set-Location $Root

$Vars = "catalog=$Catalog,schema=$Schema,warehouse_id=$Warehouse,db_instance=$DbInstance,app_name=$AppName"
Step "Running the data-generation job (setup)"
Info "catalog=$Catalog schema=$Schema - this takes ~10-20 minutes on serverless"
& databricks bundle run setup -t main -p $Profile --var $Vars
if ($LASTEXITCODE -ne 0) { Die "data generation failed" }
Ok "data generation finished"
