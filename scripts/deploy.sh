#!/usr/bin/env bash
# ============================================================================================
# Gas Upstream Digital Twin: one-command deployment (macOS / Linux).
#
# From a raw computer with an internet connection and a Databricks workspace where you have
# workspace-admin rights, this script:
#   0. installs the Databricks CLI if it is missing and signs you in (OAuth, opens a browser)
#   1. creates the target catalog, schema and landing volume
#   2. finds or creates a serverless SQL warehouse (for the SQL steps, Genie and the dashboard)
#   3. creates the Lakebase project (scale to zero): the app's only database
#   4. deploys the asset bundle (pipeline, jobs, app)
#   5. fires the data-generation job (scripts/generate_data.sh)
#   6. runs the medallion pipeline (bronze -> silver -> gold)
#   7. lands the gold marts in Lakebase (synced tables), then schedules a re-sync every 5 minutes
#   8. builds the Genie space + AI/BI dashboard (these read gold in the lakehouse)
#   9. starts the app and wires it to Genie + Lakebase
#  10. prints the app URL, catalog and Genie link
#
# Usage:
#   scripts/deploy.sh --host https://<workspace>.cloud.databricks.com [options]
#
# Options:
#   --host URL          Workspace URL (required the first time, to sign in).
#   --catalog NAME      Target catalog (default: gas_upstream_digital_twin).
#   --schema NAME       Target schema  (default: digital_twin).
#   --warehouse ID      Use an existing SQL warehouse id (default: auto-detect or create one).
#   --lakebase NAME     Lakebase project id (default: gas-upstream-twin-lakebase).
#   --sync-cron CRON    Quartz cron for the gold-to-Lakebase re-sync (default "0 0/5 * * * ?",
#                       every 5 minutes; pass "" for no schedule).
#   --app-name NAME     Databricks App name (default: gas-upstream-twin).
#   --profile NAME      CLI auth profile to use/create (default: derived from the workspace URL,
#                       e.g. gastwin-dbc-xxxx, so each workspace gets its own profile).
#   --skip-data         Deploy only; do not (re)generate data. Use for code-only redeploys.
#   -h | --help         Show this help.
# ============================================================================================
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
# shellcheck source=lib/common.sh
. "$HERE/lib/common.sh"

# ---- defaults + arg parsing -----------------------------------------------------------------
CATALOG="gas_upstream_digital_twin"
SCHEMA="digital_twin"
PROFILE=""   # empty = derive a per-workspace profile from --host (see Phase 1)
HOST=""
WAREHOUSE_ID=""
LAKEBASE_PROJECT="gas-upstream-twin-lakebase"
SYNC_CRON="0 0/5 * * * ?"
APP_NAME="gas-upstream-twin"
GENIE_TITLE="Asset Intelligence Genie"    # must match TITLE in serving/build_genie.py
DASH_NAME="Gas Upstream Digital Twin"       # must match NAME in serving/build_dashboard.py
RUN_DATA=1

usage() { sed -n '2,40p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0; }
while [ $# -gt 0 ]; do case "$1" in
  --host) HOST="$(printf '%s' "$2" | sed -E 's#^(https?://[^/?]+).*#\1#')"; shift 2;;
  --catalog) CATALOG="$2"; shift 2;;
  --schema) SCHEMA="$2"; shift 2;;
  --warehouse) WAREHOUSE_ID="$2"; shift 2;;
  --lakebase) LAKEBASE_PROJECT="$2"; shift 2;;
  --sync-cron) SYNC_CRON="$2"; shift 2;;
  --app-name) APP_NAME="$2"; shift 2;;
  --profile) PROFILE="$2"; shift 2;;
  --skip-data) RUN_DATA=0; shift;;
  -h|--help) usage;;
  *) die "Unknown argument: $1  (use --help)";;
esac; done

cd "$ROOT"
printf "%s\n" "$C_B== Gas Upstream Digital Twin: deploy ==$C_0"
info "catalog=$CATALOG  schema=$SCHEMA  app=$APP_NAME  lakebase=$LAKEBASE_PROJECT"

# VARS passed to every bundle command. Rebuilt after the warehouse is resolved.
build_vars() { VARS="catalog=${CATALOG},schema=${SCHEMA},warehouse_id=${WAREHOUSE_ID},lakebase_project=${LAKEBASE_PROJECT},sync_cron=${SYNC_CRON},app_name=${APP_NAME}"; }
bdeploy() { databricks bundle deploy -t main -p "$PROFILE" --var "$VARS"; }
brun()    { databricks bundle run "$1" -t main -p "$PROFILE" --var "$VARS" "${@:2}"; }
# dbapi always exits 0 (callers tolerate a missing resource) so `set -e` never aborts on a 404.
dbapi()   { databricks api "$1" "$2" -p "$PROFILE" 2>/dev/null || true; }
# run_sql: execute one SQL statement on the warehouse, polling to completion. Returns 0 on SUCCEEDED.
# The body is built with python so the statement (backticks, quotes) needs no shell escaping. Using
# SQL for catalog/schema/volume DDL works on standard metastores AND Default Storage workspaces.
run_sql() {
  local tf out sid state tries=0
  tf="$(mktemp)"
  python3 -c 'import json,sys;open(sys.argv[3],"w").write(json.dumps({"warehouse_id":sys.argv[1],"wait_timeout":"30s","statement":sys.argv[2]}))' "$WAREHOUSE_ID" "$1" "$tf"
  out="$(databricks api post /api/2.0/sql/statements -p "$PROFILE" --json "@$tf" 2>/dev/null || true)"
  rm -f "$tf"
  state="$(printf '%s' "$out" | pyjson 'd.get("status",{}).get("state","")')"
  sid="$(printf '%s' "$out" | pyjson 'd.get("statement_id","")')"
  while { [ "$state" = "PENDING" ] || [ "$state" = "RUNNING" ]; } && [ "$tries" -lt 25 ]; do
    sleep 6; tries=$((tries + 1))
    out="$(dbapi get "/api/2.0/sql/statements/${sid}")"
    state="$(printf '%s' "$out" | pyjson 'd.get("status",{}).get("state","")')"
  done
  [ "$state" = "SUCCEEDED" ]
}

# ---- Phase 0: prerequisites + CLI -----------------------------------------------------------
step "Phase 0: prerequisites"
ensure_python
ensure_cli

# ---- Phase 1: authentication ----------------------------------------------------------------
step "Phase 1: authentication"
# A workspace URL is the only required input. Prompt for it if not given (interactive terminals),
# so a bare `scripts/deploy.sh` still works.
if [ -z "$HOST" ] && [ -z "$PROFILE" ] && [ -t 0 ]; then
  printf "    Enter your Databricks workspace URL (e.g. https://your-workspace.cloud.databricks.com): "
  read -r HOST
  HOST="$(printf '%s' "$HOST" | sed -E 's#^(https?://[^/?]+).*#\1#')"
fi
# Derive a per-workspace profile from the host unless one was given explicitly with --profile.
if [ -z "$PROFILE" ]; then
  [ -n "$HOST" ] && PROFILE="$(twin_profile "$HOST")" || PROFILE="gas-twin"
fi
ensure_auth "$PROFILE" "$HOST"
# Resolve the workspace host once (for the stale-state guard below and the final summary).
WS_HOST="$HOST"
[ -n "$WS_HOST" ] || WS_HOST="$(databricks auth describe -p "$PROFILE" 2>/dev/null | sed -n 's/^[[:space:]]*Host:[[:space:]]*//p' | head -1 || true)"
# Guard: local bundle state (.databricks) from a deploy to a DIFFERENT workspace makes the CLI's
# deployment engine diff against resources that do not exist in this target, which can crash it.
# If the recorded host differs from this one, clear the stale state before deploying.
STATE_MARKER=".databricks/.deploy_host"
if [ -f "$STATE_MARKER" ] && [ -n "$WS_HOST" ] && [ "$(cat "$STATE_MARKER" 2>/dev/null)" != "$WS_HOST" ]; then
  warn "workspace changed since the last local deploy, clearing stale bundle state (.databricks)"
  rm -rf .databricks
fi

# ---- Phase 2: SQL warehouse (used for DDL below, Genie and the dashboard; not by the app) -----
step "Phase 2: SQL warehouse"
if [ -z "$WAREHOUSE_ID" ]; then
  WAREHOUSE_ID="$(databricks warehouses list -o json -p "$PROFILE" 2>/dev/null | pyjson 'next((w["id"] for w in d if w.get("state")=="RUNNING"), (d[0]["id"] if d else ""))' || true)"
fi
if [ -z "$WAREHOUSE_ID" ]; then
  info "no warehouse found, creating a small serverless warehouse…"
  WAREHOUSE_ID="$(databricks warehouses create --json '{"name":"Gas Twin Warehouse","cluster_size":"2X-Small","max_num_clusters":1,"auto_stop_mins":10,"enable_serverless_compute":true,"warehouse_type":"PRO"}' -o json -p "$PROFILE" 2>/dev/null | pyjson 'd.get("id","")' || true)"
  [ -n "$WAREHOUSE_ID" ] || die "Could not auto-create a SQL warehouse. Create one in the UI (SQL > SQL Warehouses) and pass --warehouse <id>."
fi
ok "warehouse: $WAREHOUSE_ID"
build_vars

# ---- Phase 3: catalog, schema, landing volume (via SQL: Default-Storage-safe) --------------
# The landing volume must exist before data generation: batch_sources writes files into it, and
# os.makedirs cannot create the UC volume object itself.
step "Phase 3: catalog, schema and landing volume"
# Use an existing catalog / schema as is: CREATE ... IF NOT EXISTS still needs the CREATE privilege
# (checked before existence), which a user given an existing catalog often does not have.
uc_exists() { dbapi get "/api/2.1/unity-catalog/$1/$2" | pyjson '"1" if d.get("name") else ""'; }
if [ -n "$(uc_exists catalogs "$CATALOG")" ]; then
  ok "using existing catalog '$CATALOG'"
else
  run_sql "CREATE CATALOG IF NOT EXISTS \`${CATALOG}\`" \
    || die "Could not create catalog '$CATALOG'. If your account uses Default Storage you may need to
      create it once in the UI (Catalog > Create catalog, Default Storage), or pass --catalog <existing>, then re-run."
  ok "catalog '$CATALOG' created"
fi
if [ -n "$(uc_exists schemas "${CATALOG}.${SCHEMA}")" ]; then
  ok "using existing schema '$SCHEMA'"
else
  run_sql "CREATE SCHEMA IF NOT EXISTS \`${CATALOG}\`.\`${SCHEMA}\`" \
    || die "Could not create schema '${CATALOG}.${SCHEMA}'. You need USE CATALOG and CREATE SCHEMA on '$CATALOG'."
  ok "schema '$SCHEMA' ready"
fi
run_sql "CREATE VOLUME IF NOT EXISTS \`${CATALOG}\`.\`${SCHEMA}\`.landing" || die "Could not create volume '${CATALOG}.${SCHEMA}.landing'."
ok "volume 'landing' ready"

# ---- Phase 4: Lakebase project (the app's only database; needed before the app binds to it) --
# Lakebase Autoscaling: 0.5 to 4 CU, and the compute scales to zero after 5 idle minutes.
step "Phase 4: Lakebase project (scale to zero)"
PG_SCHEMA="${SCHEMA}_app"   # the gold marts land here as Lakebase synced tables (same names as in gold)
lb_endpoint() { dbapi get "/api/2.0/postgres/projects/${LAKEBASE_PROJECT}/branches/production/endpoints" \
  | pyjson 'next((e["name"] for e in d.get("endpoints",[]) if e.get("status",{}).get("endpoint_type")=="ENDPOINT_TYPE_READ_WRITE"),"")'; }
PG_ENDPOINT="$(lb_endpoint)"
if [ -z "$PG_ENDPOINT" ]; then
  info "creating Lakebase project '$LAKEBASE_PROJECT' (takes a minute)…"
  databricks api post "/api/2.0/postgres/projects?project_id=${LAKEBASE_PROJECT}" -p "$PROFILE" --json \
    "{\"spec\":{\"display_name\":\"${LAKEBASE_PROJECT}\",\"pg_version\":16,\"default_endpoint_settings\":{\"autoscaling_limit_min_cu\":0.5,\"autoscaling_limit_max_cu\":4,\"suspend_timeout_duration\":\"300s\"}}}" \
    >/dev/null 2>&1 || warn "create call returned non-zero (it may already be provisioning)"
  for _ in $(seq 1 40); do
    PG_ENDPOINT="$(lb_endpoint)"
    [ -n "$PG_ENDPOINT" ] && break
    sleep 10
  done
fi
[ -n "$PG_ENDPOINT" ] || die "Lakebase project '$LAKEBASE_PROJECT' did not become ready. Lakebase Autoscaling must be
      available in this workspace/region. See docs/TROUBLESHOOTING.md, then re-run."
ok "Lakebase endpoint: $PG_ENDPOINT"

# ---- Phase 5: render app.yaml + deploy bundle -----------------------------------------------
step "Phase 5: deploy bundle (pipeline, jobs, app)"
render_app_yaml() {  # $1 = genie space id (may be empty on first pass)
  sed -e "s|__SCHEMA__|${SCHEMA}|g" -e "s|__PG_SCHEMA__|${PG_SCHEMA}|g" \
      -e "s|__PG_ENDPOINT__|${PG_ENDPOINT}|g" \
      -e "s|__GENIE_SPACE__|${1:-}|g" "$ROOT/app/app.yaml.tmpl" > "$ROOT/app/app.yaml"
}
render_app_yaml ""
bdeploy
ok "bundle deployed"
# Record which workspace this local state belongs to (used by the stale-state guard above).
mkdir -p .databricks && printf '%s' "$WS_HOST" > "$STATE_MARKER" 2>/dev/null || true

# ---- Phase 6: data generation ---------------------------------------------------------------
if [ "$RUN_DATA" -eq 1 ]; then
  step "Phase 6: generating data (this is the longest step, ~10-20 min)"
  "$HERE/generate_data.sh" --profile "$PROFILE" --catalog "$CATALOG" --schema "$SCHEMA" \
      --warehouse "$WAREHOUSE_ID" --lakebase "$LAKEBASE_PROJECT" --app-name "$APP_NAME"
  ok "data generated"
else
  warn "Phase 6: skipping data generation (--skip-data)"
fi

# ---- Phase 7: medallion ---------------------------------------------------------------------
step "Phase 7: medallion pipeline (bronze -> silver -> gold)"
brun medallion --full-refresh-all
ok "medallion complete"

# ---- Phase 8: Lakebase sync, right after the medallion -----------------------------------------
# One synced table per gold mart (one sync pipeline), synced now; seeds the recommendation write-back
# table, grants the app, and creates the "Lakebase sync" job that re-runs the sync every 5 minutes.
step "Phase 8: land the gold marts in Lakebase (synced tables) + schedule the re-sync"
brun lakebase_sync || die "The Lakebase sync failed, so the app would have no data. See docs/TROUBLESHOOTING.md, then re-run."
ok "gold marts synced to Lakebase (Postgres schema ${PG_SCHEMA}); re-sync scheduled"

# ---- Phase 9: surfaces (Genie + dashboard, over gold in the lakehouse) -----------------------
step "Phase 9: Genie space + AI/BI dashboard"
if brun surfaces; then ok "surfaces built"; else warn "surfaces job hit an issue (Genie/dashboard). The app still works; see docs/TROUBLESHOOTING.md."; fi
# A newly created Genie space can take a few seconds to appear in the spaces list, so poll for it.
find_genie() { dbapi get /api/2.0/genie/spaces | python3 -c 'import sys,json
try: d=json.load(sys.stdin)
except Exception: sys.exit(0)
t=sys.argv[1]
print(next((s.get("space_id") or s.get("id") for s in d.get("spaces",[]) if s.get("title")==t),""))' "$GENIE_TITLE" 2>/dev/null || true; }
GENIE_ID=""
for _ in $(seq 1 12); do GENIE_ID="$(find_genie)"; [ -n "$GENIE_ID" ] && break; sleep 6; done
[ -n "$GENIE_ID" ] && ok "Genie space: $GENIE_ID" || warn "Genie space id not found; the app's Genie lamp link will be hidden until a redeploy finds it."

# ---- Phase 10: start the app, wired to Genie + Lakebase -------------------------------------
step "Phase 10: start the app"
render_app_yaml "$GENIE_ID"
bdeploy
brun app

# ---- Summary -------------------------------------------------------------------------------
APP_URL="$(dbapi get "/api/2.0/apps/${APP_NAME}" | pyjson 'd.get("url","")')"
# WS_HOST was resolved in Phase 1.
printf "\n%s============================================================%s\n" "$C_G" "$C_0"
printf "%s Deployment complete.%s\n" "$C_B" "$C_0"
printf "%s============================================================%s\n" "$C_G" "$C_0"
printf "  App:        %s\n" "${APP_URL:-"(open Compute > Apps > ${APP_NAME})"}"
printf "  Catalog:    %s.%s   (open Catalog to browse the marts)\n" "$CATALOG" "$SCHEMA"
[ -n "$GENIE_ID" ] && [ -n "$WS_HOST" ] && printf "  Genie:      %s/one/agents/%s\n" "$WS_HOST" "$GENIE_ID"
printf "  Dashboard:  open Dashboards > \"%s\"\n" "$DASH_NAME"
printf "  Lakebase:   %s  (Postgres schema %s; re-sync job \"[%s.%s] Gas Twin - Lakebase sync\")\n" "$LAKEBASE_PROJECT" "$PG_SCHEMA" "$CATALOG" "$SCHEMA"
printf "  Warehouse:  %s  (Genie + dashboard)\n" "$WAREHOUSE_ID"
printf "\n  New here? Read docs/NAVIGATION.md for a guided tour of the app.\n\n"
