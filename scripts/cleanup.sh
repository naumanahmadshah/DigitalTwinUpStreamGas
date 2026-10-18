#!/usr/bin/env bash
# ============================================================================================
# Gas Upstream Digital Twin - complete cleanup (macOS / Linux).
#
# Removes EVERYTHING this project created and returns the workspace to a clean, pre-deploy state so
# scripts/deploy.sh can run fresh. It removes:
#   - the bundle resources (the app, the jobs, the medallion pipeline, and the bundle workspace folder)
#   - the Genie space
#   - the AI/BI dashboard
#   - the Lakebase instance
#   - the catalog, schema, landing volume and ALL generated data (DROP CATALOG ... CASCADE)
#   - a project-created SQL warehouse named "Gas Twin Warehouse" (a pre-existing/shared warehouse is left)
#
# Usage:
#   scripts/cleanup.sh --host https://<workspace-url> [options]
#
# Options:
#   --host URL          Workspace URL. Signs in to it; the profile is derived from the URL. The same
#                       --host you deployed with cleans up that workspace.
#   --profile NAME      Use a specific CLI auth profile instead (must already be signed in). Overrides
#                       the host-derived default.
#   --catalog NAME      Catalog to drop (default: gas_upstream_digital_twin).
#   --schema NAME       Schema (default: digital_twin).
#   --warehouse ID      SQL warehouse to run the DROP on (default: auto-detect).
#   --db-instance NAME  Lakebase instance to delete (default: gas-upstream-twin-db).
#   --app-name NAME     App name (default: gas-upstream-twin).
#   --yes               Skip the confirmation prompt (required for non-interactive use).
#   -h | --help         Show this help.
# ============================================================================================
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
# shellcheck source=lib/common.sh
. "$HERE/lib/common.sh"

CATALOG="gas_upstream_digital_twin"; SCHEMA="digital_twin"; PROFILE=""; HOST=""
WAREHOUSE_ID=""; DB_INSTANCE="gas-upstream-twin-db"; APP_NAME="gas-upstream-twin"
GENIE_TITLE="Asset Intelligence Genie"; GENIE_OLD="Gas Upstream Digital Twin"; DASH_NAME="Gas Upstream Digital Twin"
WH_NAME="Gas Twin Warehouse"   # the warehouse deploy.sh creates when a workspace has none
ASSUME_YES=0

usage() { sed -n '2,30p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0; }
while [ $# -gt 0 ]; do case "$1" in
  --host) HOST="$(printf '%s' "$2" | sed -E 's#^(https?://[^/?]+).*#\1#')"; shift 2;;
  --profile) PROFILE="$2"; shift 2;;
  --catalog) CATALOG="$2"; shift 2;;
  --schema) SCHEMA="$2"; shift 2;;
  --warehouse) WAREHOUSE_ID="$2"; shift 2;;
  --db-instance) DB_INSTANCE="$2"; shift 2;;
  --app-name) APP_NAME="$2"; shift 2;;
  --yes|--force) ASSUME_YES=1; shift;;
  -h|--help) usage;;
  *) die "Unknown argument: $1  (use --help)";;
esac; done

# Derive the per-workspace profile from --host unless one was given explicitly (same rule as deploy.sh).
if [ -z "$PROFILE" ]; then
  [ -n "$HOST" ] && PROFILE="$(twin_profile "$HOST")" || PROFILE="gas-twin"
fi

cd "$ROOT"
printf "%s\n" "$C_B== Gas Upstream Digital Twin - cleanup ==$C_0"
info "catalog=$CATALOG  schema=$SCHEMA  app=$APP_NAME  db=$DB_INSTANCE  profile=$PROFILE"

# Authenticate: if a --host was given, sign in to it; otherwise rely on the profile already being signed in.
if [ -n "$HOST" ]; then
  ensure_auth "$PROFILE" "$HOST"
else
  databricks current-user me -p "$PROFILE" >/dev/null 2>&1 \
    || die "Profile '$PROFILE' is not signed in. Pass --host https://your-workspace... (or --profile <authed>)."
fi

# Confirm (destructive). Bypass with --yes; refuse to run blind in a non-interactive shell.
if [ "$ASSUME_YES" -ne 1 ]; then
  if [ -t 0 ]; then
    printf "\n%sThis permanently deletes the app, dashboard, Genie space, Lakebase instance, pipeline, jobs,\nand the catalog '%s' with ALL its data.%s\nType 'yes' to proceed: " "$C_Y" "$CATALOG" "$C_0"
    read -r ans; [ "$ans" = "yes" ] || die "aborted."
  else
    die "Refusing to run non-interactively without --yes (this is destructive)."
  fi
fi

# ---- SQL warehouse (needed to DROP the catalog) ---------------------------------------------
step "Resolving a SQL warehouse (to drop the catalog)"
if [ -z "$WAREHOUSE_ID" ]; then
  WAREHOUSE_ID="$(databricks warehouses list -o json -p "$PROFILE" 2>/dev/null | pyjson 'next((w["id"] for w in d if w.get("state")=="RUNNING"), (d[0]["id"] if d else ""))' || true)"
fi
[ -n "$WAREHOUSE_ID" ] && ok "warehouse: $WAREHOUSE_ID" || warn "no warehouse found; the catalog drop will be skipped (drop it manually if needed)"
VARS="catalog=${CATALOG},schema=${SCHEMA},warehouse_id=${WAREHOUSE_ID},db_instance=${DB_INSTANCE},app_name=${APP_NAME}"

# ---- Bundle resources (app, jobs, pipeline + workspace folder) ------------------------------
# Clear local bundle state so `destroy` re-fetches the deployment from the workspace. This avoids the
# CLI direct-engine panic that can occur when local state references a different workspace.
step "Removing bundle resources (app, jobs, pipeline)"
rm -rf .databricks
if databricks bundle destroy -t main -p "$PROFILE" --var "$VARS" --auto-approve 2>/dev/null; then
  ok "bundle resources removed"
else
  warn "no bundle deployment found (or already removed)"
fi
# App deletion is asynchronous; wait for it so "cleanup complete" is truthful and an immediate
# redeploy does not race a half-deleted app of the same name.
app_present() { databricks apps list -o json -p "$PROFILE" 2>/dev/null | python3 -c 'import sys,json
try: d=json.load(sys.stdin)
except Exception: print("0"); sys.exit()
a=d if isinstance(d,list) else d.get("apps",[])
print("1" if any(x.get("name")==sys.argv[1] for x in a) else "0")' "$APP_NAME" 2>/dev/null || echo 0; }
tries=0
while [ "$(app_present)" = "1" ] && [ "$tries" -lt 20 ]; do sleep 6; tries=$((tries + 1)); done
[ "$(app_present)" = "1" ] && warn "app '$APP_NAME' still deleting after ~2 min (a redeploy may need a moment)" || ok "app fully removed"

# ---- Genie space ----------------------------------------------------------------------------
step "Removing the Genie space"
GID="$(dbapi get /api/2.0/genie/spaces | python3 -c 'import sys,json
try: d=json.load(sys.stdin)
except Exception: sys.exit(0)
print(next((s.get("space_id") or s.get("id") for s in d.get("spaces",[]) if s.get("title") in sys.argv[1:]),""))' "$GENIE_TITLE" "$GENIE_OLD" 2>/dev/null || true)"
if [ -n "$GID" ]; then
  databricks api delete "/api/2.0/genie/spaces/$GID" -p "$PROFILE" >/dev/null 2>&1 && ok "Genie space removed ($GID)" || warn "could not remove Genie space $GID"
else ok "no Genie space to remove"; fi

# ---- AI/BI dashboard ------------------------------------------------------------------------
step "Removing the AI/BI dashboard"
DID="$(databricks lakeview list -p "$PROFILE" 2>/dev/null | python3 -c 'import sys,json
try: d=json.load(sys.stdin)
except Exception: sys.exit(0)
rows = d if isinstance(d,list) else d.get("dashboards",[])
print(next((x.get("dashboard_id") for x in rows if x.get("display_name")==sys.argv[1] and not x.get("trashed_at")),""))' "$DASH_NAME" 2>/dev/null || true)"
if [ -n "$DID" ]; then
  databricks lakeview trash "$DID" -p "$PROFILE" >/dev/null 2>&1 && ok "dashboard removed ($DID)" || warn "could not remove dashboard $DID"
else ok "no dashboard to remove"; fi

# ---- Lakebase instance ----------------------------------------------------------------------
step "Removing the Lakebase instance"
if dbapi get "/api/2.0/database/instances/${DB_INSTANCE}" | grep -q "\"name\"\|read_write_dns"; then
  databricks database delete-database-instance "$DB_INSTANCE" --purge -p "$PROFILE" >/dev/null 2>&1 \
    || databricks database delete-database-instance "$DB_INSTANCE" -p "$PROFILE" >/dev/null 2>&1 || true
  ok "Lakebase instance '$DB_INSTANCE' removed"
else ok "no Lakebase instance to remove"; fi

# ---- Catalog + all data ---------------------------------------------------------------------
step "Dropping the catalog and all data"
if [ -n "$WAREHOUSE_ID" ]; then
  run_sql "DROP CATALOG IF EXISTS \`${CATALOG}\` CASCADE" && ok "catalog '$CATALOG' dropped (schema, volume and data gone)" \
    || warn "could not drop catalog '$CATALOG' (drop it manually in the UI)"
else warn "skipped catalog drop (no warehouse resolved)"; fi

# ---- Project-created warehouse (only the one deploy.sh creates by name) ----------------------
step "Removing a project-created warehouse (if any)"
WID="$(databricks warehouses list -o json -p "$PROFILE" 2>/dev/null | python3 -c 'import sys,json
try: d=json.load(sys.stdin)
except Exception: sys.exit(0)
print(next((w["id"] for w in d if w.get("name")==sys.argv[1]),""))' "$WH_NAME" 2>/dev/null || true)"
if [ -n "$WID" ]; then
  databricks warehouses delete "$WID" -p "$PROFILE" >/dev/null 2>&1 && ok "removed project warehouse '$WH_NAME' ($WID)" || warn "could not remove warehouse $WID"
else ok "no project-created warehouse to remove (a pre-existing/shared warehouse is left untouched)"; fi

printf "\n%s============================================================%s\n" "$C_G" "$C_0"
printf "%s Cleanup complete.%s The workspace is back to a pre-deploy state; scripts/deploy.sh can run fresh.\n\n" "$C_B" "$C_0"
