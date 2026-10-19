#!/usr/bin/env bash
# ============================================================================================
# Gas Upstream Digital Twin: data generation (macOS / Linux).
#
# Fires the "setup" job in the deployed bundle: it builds the semantic backbone and backfills
# ~2 years of telemetry/events plus the batch, reference and file sources. deploy.sh calls this
# automatically; you can also run it on its own to regenerate the data after a config change. After
# regenerating, re-run the medallion; the scheduled Lakebase sync then lands the new gold in the app
# within 5 minutes.
#
# Prerequisite: the bundle has already been deployed (scripts/deploy.sh) and you are signed in.
#
# Usage:
#   scripts/generate_data.sh --profile gas-twin [--catalog NAME] [--schema NAME]
#                            [--warehouse ID] [--lakebase NAME] [--app-name NAME]
# ============================================================================================
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
# shellcheck source=lib/common.sh
. "$HERE/lib/common.sh"

CATALOG="gas_upstream_digital_twin"; SCHEMA="digital_twin"; PROFILE="gas-twin"
WAREHOUSE_ID=""; LAKEBASE_PROJECT="gas-upstream-twin-lakebase"; APP_NAME="gas-upstream-twin"
while [ $# -gt 0 ]; do case "$1" in
  --catalog) CATALOG="$2"; shift 2;;
  --schema) SCHEMA="$2"; shift 2;;
  --warehouse) WAREHOUSE_ID="$2"; shift 2;;
  --lakebase) LAKEBASE_PROJECT="$2"; shift 2;;
  --app-name) APP_NAME="$2"; shift 2;;
  --profile) PROFILE="$2"; shift 2;;
  -h|--help) sed -n '2,22p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0;;
  *) die "Unknown argument: $1";;
esac; done

cd "$ROOT"
VARS="catalog=${CATALOG},schema=${SCHEMA},warehouse_id=${WAREHOUSE_ID},lakebase_project=${LAKEBASE_PROJECT},app_name=${APP_NAME}"
step "Running the data-generation job (setup)"
info "catalog=$CATALOG schema=$SCHEMA, this takes ~10-20 minutes on serverless"
databricks bundle run setup -t main -p "$PROFILE" --var "$VARS"
ok "data generation finished"
