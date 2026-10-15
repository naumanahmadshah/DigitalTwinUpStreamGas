#!/usr/bin/env bash
# Shared helpers for the Gas Upstream Digital Twin deploy scripts (macOS / Linux).
# Sourced by deploy.sh and generate_data.sh. Not meant to be run directly.

# A stale DATABRICKS_AUTH_STORAGE=plaintext in the environment points the CLI at an old token
# cache and breaks OAuth. Clear it so the CLI uses its normal (keyring/OAuth) storage.
unset DATABRICKS_AUTH_STORAGE 2>/dev/null || true

# ---- pretty logging -------------------------------------------------------------------------
if [ -t 1 ]; then C_B=$'\033[1m'; C_G=$'\033[32m'; C_Y=$'\033[33m'; C_R=$'\033[31m'; C_D=$'\033[2m'; C_0=$'\033[0m'
else C_B=""; C_G=""; C_Y=""; C_R=""; C_D=""; C_0=""; fi
step() { printf "\n%s==>%s %s%s%s\n" "$C_B" "$C_0" "$C_B" "$*" "$C_0"; }
info() { printf "    %s\n" "$*"; }
ok()   { printf "    %s✓%s %s\n" "$C_G" "$C_0" "$*"; }
warn() { printf "    %s!%s %s\n" "$C_Y" "$C_0" "$*"; }
die()  { printf "\n%sERROR:%s %s\n" "$C_R" "$C_0" "$*" >&2; exit 1; }

# ---- JSON helper: evaluate a Python expression against parsed stdin (`d` = the parsed JSON) --
# e.g.  echo "$json" | pyjson 'd.get("read_write_dns","")'
pyjson() { python3 -c 'import sys,json
try: d=json.load(sys.stdin)
except Exception: sys.exit(0)
print(eval(sys.argv[1]))' "$1" 2>/dev/null || true; }

# ---- Databricks API + SQL helpers (shared by deploy.sh and cleanup.sh) ----------------------
# Callers set $PROFILE (and $WAREHOUSE_ID for run_sql) before using these.
# dbapi always exits 0 so `set -e` never aborts on a missing resource (404).
dbapi() { databricks api "$1" "$2" -p "$PROFILE" 2>/dev/null || true; }
# run_sql: execute one SQL statement on the warehouse, polling to completion. Returns 0 on SUCCEEDED.
# The body is built with python so the statement (backticks, quotes) needs no shell escaping.
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

# ---- prerequisite checks + CLI bootstrap ----------------------------------------------------
ensure_python() {
  command -v python3 >/dev/null 2>&1 || die "python3 is required (used to read JSON responses). Install it:
      macOS:  install the Xcode Command Line Tools (xcode-select --install) or 'brew install python'
      Linux:  'sudo apt-get install -y python3'  (Debian/Ubuntu)  or your distro's package manager"
}

ensure_cli() {
  if ! command -v databricks >/dev/null 2>&1; then
    step "Installing the Databricks CLI"
    curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh \
      || die "Could not install the Databricks CLI automatically.
      Install it manually, then re-run: https://docs.databricks.com/dev-tools/cli/install.html
        macOS/Linux:  brew tap databricks/tap && brew install databricks
        or:           curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sudo sh"
    command -v databricks >/dev/null 2>&1 || { export PATH="/usr/local/bin:$PATH"; }
    command -v databricks >/dev/null 2>&1 || die "The Databricks CLI was installed but is not on PATH. Open a new terminal and re-run."
  fi
  # The bundle commands require the modern unified CLI (v0.205+). The legacy pip 'databricks-cli' lacks them.
  databricks bundle version >/dev/null 2>&1 || databricks bundle --help >/dev/null 2>&1 \
    || die "Your 'databricks' command does not support bundles. You likely have the legacy CLI.
      Install the current CLI: https://docs.databricks.com/dev-tools/cli/install.html"
  ok "Databricks CLI: $(databricks --version 2>/dev/null | head -1)"
}

# ---- authentication -------------------------------------------------------------------------
# A stable, per-workspace profile name derived from the host URL, e.g.
#   https://dbc-1234abcd-5678.cloud.databricks.com -> gastwin-dbc-1234abcd-5678
# One profile per workspace means a login to any host never conflicts with another profile.
twin_profile() { printf 'gastwin-%s' "$(printf '%s' "$1" | sed -E 's#^https?://##; s#[./:].*##')"; }

# If a workspace URL is given, sign in to exactly that workspace (a browser opens). The profile is
# unique to that workspace, so this never conflicts. If no URL is given, rely on an existing sign-in.
ensure_auth() {
  local profile="$1" host="$2"
  if [ -n "$host" ]; then
    step "Logging in to $host (profile: $profile; a browser window will open)"
    databricks auth login --host "$host" --profile "$profile" \
      || die "OAuth login failed. Confirm the workspace URL and that you can sign in with admin rights."
    databricks current-user me -p "$profile" >/dev/null 2>&1 || die "Authentication did not complete."
    ok "Authenticated as $(databricks current-user me -p "$profile" | pyjson 'd.get("userName","")') (profile: $profile)"
    return 0
  fi
  databricks current-user me -p "$profile" >/dev/null 2>&1 \
    || die "Profile '$profile' is not signed in and no --host was given.
      Re-run with your workspace URL, e.g.:  --host https://your-workspace.cloud.databricks.com"
  ok "Authenticated as $(databricks current-user me -p "$profile" | pyjson 'd.get("userName","")') (profile: $profile)"
}
