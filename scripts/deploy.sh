#!/usr/bin/env bash
# ============================================================================================
# Gas Upstream Digital Twin — one-command deployment (macOS / Linux).
#
# From a raw computer with an internet connection and a Databricks workspace where you have
# workspace-admin rights, this script:
#   0. installs the Databricks CLI if it is missing and signs you in (OAuth, opens a browser)
#   1. creates the target catalog, schema and landing volume
#   2. finds or creates a serverless SQL warehouse
#   3. creates the Lakebase (Postgres) instance used for write-back
#   4. deploys the asset bundle (pipeline, jobs, app)
#   5. fires the data-generation job (scripts/generate_data.sh)
#   6. runs the medallion pipeline (bronze -> silver -> gold)
#   7. builds the Genie space + AI/BI dashboard
#   8. starts the app and wires it to Genie + Lakebase
#   9. prints the app URL, catalog and Genie link
#
# Usage:
#   scripts/deploy.sh --host https://<workspace>.cloud.databricks.com [options]
#
# Options:
#   --host URL          Workspace URL (required the first time, to sign in).
#   --catalog NAME      Target catalog (default: gas_upstream_digital_twin).
#   --schema NAME       Target schema  (default: digital_twin).
#   --warehouse ID      Use an existing SQL warehouse id (default: auto-detect or create one).
#   --db-instance NAME  Lakebase instance name (default: gas-upstream-twin-db).
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
DB_INSTANCE="gas-upstream-twin-db"
APP_NAME="gas-upstream-twin"
GENIE_TITLE="Gas Upstream Digital Twin"   # must match TITLE in serving/build_genie.py
RUN_DATA=1

usage() { sed -n '2,40p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0; }
while [ $# -gt 0 ]; do case "$1" in
  --host) HOST="$(printf '%s' "$2" | sed -E 's#^(https?://[^/?]+).*#\1#')"; shift 2;;
  --catalog) CATALOG="$2"; shift 2;;
  --schema) SCHEMA="$2"; shift 2;;
  --warehouse) WAREHOUSE_ID="$2"; shift 2;;
  --db-instance) DB_INSTANCE="$2"; shift 2;;
  --app-name) APP_NAME="$2"; shift 2;;
  --profile) PROFILE="$2"; shift 2;;
  --skip-data) RUN_DATA=0; shift;;
  -h|--help) usage;;
  *) die "Unknown argument: $1  (use --help)";;
esac; done

cd "$ROOT"
printf "%s\n" "$C_B== Gas Upstream Digital Twin — deploy ==$C_0"
info "catalog=$CATALOG  schema=$SCHEMA  app=$APP_NAME  db=$DB_INSTANCE"

# VARS passed to every bundle command. Rebuilt after the warehouse is resolved.
build_vars() { VARS="catalog=${CATALOG},schema=${SCHEMA},warehouse_id=${WAREHOUSE_ID},db_instance=${DB_INSTANCE},app_name=${APP_NAME}"; }
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
step "Phase 0 — prerequisites"
ensure_python
ensure_cli

# ---- Phase 1: authentication ----------------------------------------------------------------
step "Phase 1 — authentication"
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
  warn "workspace changed since the last local deploy — clearing stale bundle state (.databricks)"
  rm -rf .databricks
fi

# ---- Phase 2: SQL warehouse (used for DDL below + by the app) --------------------------------
step "Phase 2 — SQL warehouse"
if [ -z "$WAREHOUSE_ID" ]; then
  WAREHOUSE_ID="$(databricks warehouses list -o json -p "$PROFILE" 2>/dev/null | pyjson 'next((w["id"] for w in d if w.get("state")=="RUNNING"), (d[0]["id"] if d else ""))' || true)"
fi
if [ -z "$WAREHOUSE_ID" ]; then
  info "no warehouse found — creating a small serverless warehouse…"
  WAREHOUSE_ID="$(databricks warehouses create --json '{"name":"Gas Twin Warehouse","cluster_size":"2X-Small","max_num_clusters":1,"auto_stop_mins":10,"enable_serverless_compute":true,"warehouse_type":"PRO"}' -o json -p "$PROFILE" 2>/dev/null | pyjson 'd.get("id","")' || true)"
  [ -n "$WAREHOUSE_ID" ] || die "Could not auto-create a SQL warehouse. Create one in the UI (SQL > SQL Warehouses) and pass --warehouse <id>."
fi
ok "warehouse: $WAREHOUSE_ID"
build_vars

# ---- Phase 2b: catalog, schema, landing volume (via SQL — Default-Storage-safe) --------------
# The landing volume must exist before data generation: batch_sources writes files into it, and
# os.makedirs cannot create the UC volume object itself.
step "Phase 2 — catalog, schema and landing volume"
run_sql "CREATE CATALOG IF NOT EXISTS \`${CATALOG}\`" \
  || die "Could not create catalog '$CATALOG'. If your account uses Default Storage you may need to
      create it once in the UI (Catalog > Create catalog, Default Storage), or pass --catalog <existing>, then re-run."
ok "catalog '$CATALOG' ready"
run_sql "CREATE SCHEMA IF NOT EXISTS \`${CATALOG}\`.\`${SCHEMA}\`" || die "Could not create schema '${CATALOG}.${SCHEMA}'."
ok "schema '$SCHEMA' ready"
run_sql "CREATE VOLUME IF NOT EXISTS \`${CATALOG}\`.\`${SCHEMA}\`.landing" || die "Could not create volume '${CATALOG}.${SCHEMA}.landing'."
ok "volume 'landing' ready"

# ---- Phase 2c: Lakebase instance (needed before the app binds to it) ------------------------
step "Phase 3 — Lakebase instance (write-back)"
PGHOST="$(dbapi get "/api/2.0/database/instances/${DB_INSTANCE}" | pyjson 'd.get("read_write_dns","")')"
if [ -z "$PGHOST" ]; then
  info "creating Lakebase instance '$DB_INSTANCE' (provisioning takes a few minutes)…"
  databricks database create-database-instance "$DB_INSTANCE" --capacity CU_1 -p "$PROFILE" >/dev/null 2>&1 \
    || databricks database create-database-instance "$DB_INSTANCE" --capacity CU_1 --node-count 1 -p "$PROFILE" >/dev/null 2>&1 \
    || warn "create call returned non-zero (it may already be provisioning)"
  for _ in $(seq 1 40); do
    PGHOST="$(dbapi get "/api/2.0/database/instances/${DB_INSTANCE}" | pyjson 'd.get("read_write_dns","")')"
    [ -n "$PGHOST" ] && break
    sleep 15
  done
fi
[ -n "$PGHOST" ] || die "Lakebase instance '$DB_INSTANCE' did not become ready. Lakebase (Database Instances) must be
      available in this workspace/region. See docs/TROUBLESHOOTING.md, then re-run."
ok "Lakebase host: $PGHOST"

# ---- Phase 3: render app.yaml + deploy bundle -----------------------------------------------
step "Phase 4 — deploy bundle (pipeline, jobs, app)"
render_app_yaml() {  # $1 = genie space id (may be empty on first pass)
  sed -e "s|__CATALOG__|${CATALOG}|g" -e "s|__SCHEMA__|${SCHEMA}|g" \
      -e "s|__DB_INSTANCE__|${DB_INSTANCE}|g" -e "s|__PGHOST__|${PGHOST}|g" \
      -e "s|__GENIE_SPACE__|${1:-}|g" "$ROOT/app/app.yaml.tmpl" > "$ROOT/app/app.yaml"
}
render_app_yaml ""
bdeploy
ok "bundle deployed"
# Record which workspace this local state belongs to (used by the stale-state guard above).
mkdir -p .databricks && printf '%s' "$WS_HOST" > "$STATE_MARKER" 2>/dev/null || true

# grant the app's service principal read access on the catalog/schema
APP_SP="$(dbapi get "/api/2.0/apps/${APP_NAME}" | pyjson 'd.get("service_principal_client_id","")')"
if [ -n "$APP_SP" ]; then
  run_sql "GRANT USE CATALOG ON CATALOG \`${CATALOG}\` TO \`${APP_SP}\`" || true
  run_sql "GRANT USE SCHEMA ON SCHEMA \`${CATALOG}\`.\`${SCHEMA}\` TO \`${APP_SP}\`" || true
  run_sql "GRANT SELECT ON SCHEMA \`${CATALOG}\`.\`${SCHEMA}\` TO \`${APP_SP}\`" || true
  ok "granted app service principal SELECT on ${CATALOG}.${SCHEMA}"
else
  warn "could not resolve the app service principal yet; grants will be retried on the next deploy"
fi

# ---- Phase 4: data generation ---------------------------------------------------------------
if [ "$RUN_DATA" -eq 1 ]; then
  step "Phase 5 — generating data (this is the longest step, ~10-20 min)"
  "$HERE/generate_data.sh" --profile "$PROFILE" --catalog "$CATALOG" --schema "$SCHEMA" \
      --warehouse "$WAREHOUSE_ID" --db-instance "$DB_INSTANCE" --app-name "$APP_NAME"
  ok "data generated"
else
  warn "Phase 5 — skipping data generation (--skip-data)"
fi

# ---- Phase 5: medallion ---------------------------------------------------------------------
step "Phase 6 — medallion pipeline (bronze -> silver -> gold)"
brun medallion --full-refresh-all
ok "medallion complete"

# ---- Phase 6: surfaces (Genie + dashboard) --------------------------------------------------
step "Phase 7 — Genie space + AI/BI dashboard"
if brun surfaces; then ok "surfaces built"; else warn "surfaces job hit an issue (Genie/dashboard). The app still works; see docs/TROUBLESHOOTING.md."; fi
# A newly created Genie space can take a few seconds to appear in the spaces list, so poll for it.
find_genie() { dbapi get /api/2.0/genie/spaces | python3 -c 'import sys,json
try: d=json.load(sys.stdin)
except Exception: sys.exit(0)
t=sys.argv[1]
print(next((s.get("space_id") or s.get("id") for s in d.get("spaces",[]) if s.get("title")==t),""))' "$GENIE_TITLE" 2>/dev/null || true; }
GENIE_ID=""
for _ in $(seq 1 12); do GENIE_ID="$(find_genie)"; [ -n "$GENIE_ID" ] && break; sleep 6; done
[ -n "$GENIE_ID" ] && ok "Genie space: $GENIE_ID" || warn "Genie space id not found; the Field Copilot will show a 'not wired yet' note."

# ---- Phase 7: start the app, wired to Genie -------------------------------------------------
step "Phase 8 — start the app"
render_app_yaml "$GENIE_ID"
bdeploy
brun app
if [ -n "$GENIE_ID" ] && [ -n "$APP_SP" ]; then
  databricks api patch "/api/2.0/permissions/genie/${GENIE_ID}" -p "$PROFILE" \
    --json "{\"access_control_list\":[{\"service_principal_name\":\"${APP_SP}\",\"permission_level\":\"CAN_RUN\"}]}" >/dev/null 2>&1 \
    && ok "app can query the Genie space" || warn "could not grant the app CAN_RUN on Genie (set it in the UI if the Copilot errors)"
fi

# ---- Phase 8: Lakebase seed -----------------------------------------------------------------
step "Phase 9 — seed Lakebase (recommendation write-back table + grants)"
if brun lakebase_seed; then ok "Lakebase seeded"; else warn "Lakebase seed hit an issue; recommendation write-back may be read-only. See docs/TROUBLESHOOTING.md."; fi

# ---- Phase 9: summary -----------------------------------------------------------------------
APP_URL="$(dbapi get "/api/2.0/apps/${APP_NAME}" | pyjson 'd.get("url","")')"
# WS_HOST was resolved in Phase 1.
printf "\n%s============================================================%s\n" "$C_G" "$C_0"
printf "%s Deployment complete.%s\n" "$C_B" "$C_0"
printf "%s============================================================%s\n" "$C_G" "$C_0"
printf "  App:        %s\n" "${APP_URL:-"(open Compute > Apps > ${APP_NAME})"}"
printf "  Catalog:    %s.%s   (open Catalog to browse the marts)\n" "$CATALOG" "$SCHEMA"
[ -n "$GENIE_ID" ] && [ -n "$WS_HOST" ] && printf "  Genie:      %s/genie/rooms/%s\n" "$WS_HOST" "$GENIE_ID"
printf "  Dashboard:  open Dashboards > \"%s\"\n" "$GENIE_TITLE"
printf "  Warehouse:  %s\n" "$WAREHOUSE_ID"
printf "\n  New here? Read docs/NAVIGATION.md for a guided tour of the app.\n\n"
