"""Land the gold marts in Lakebase (the App's only database) and keep them in sync.

Runs right after the medallion. Idempotent: safe to re-run at any time.
  1. One Lakebase synced table per gold mart the App reads (SNAPSHOT mode, all bin-packed into ONE
     sync pipeline). Each lands in Postgres as <serving_schema>.<mart>, same name as the gold mart.
  2. Runs that sync pipeline now and waits for every synced table to come online.
  3. The write-back tables (recommendation decisions, operator rounds), seeded from gold.
  4. Grants the App's service principal read on the synced marts and read/write on the write-back.
  5. A scheduled job that re-runs the sync pipeline every few minutes (default every 5 minutes).
     A snapshot sync skips a mart whose gold source has not changed, so an idle run is short.
Genie and the AI/BI dashboard keep reading gold in the lakehouse; only the App reads Lakebase.
"""
from __future__ import annotations
import argparse
import time
import psycopg
from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession

# Every gold mart the App reads, with its primary key (verified unique and non-null in gold).
SYNC = {
    "gld_asset_state": ["asset_id"],
    "gld_asset_master": ["asset_id"],
    "gld_asset_signal_series": ["asset_id", "signal", "gas_day"],
    "gld_capital_plan": ["project_id"],
    "gld_carbon_compliance": ["facility", "reporting_year"],
    "gld_commercial": ["gas_day"],
    "gld_compression_health": ["asset_id", "gas_day"],
    "gld_constraint_ranking": ["asset_id", "constraint_type"],
    "gld_custody_allocation": ["asset_id", "gas_day"],
    "gld_customer_supply": ["customer_id"],
    "gld_deferment": ["deferment_id"],
    "gld_economics": ["asset_id", "gas_day"],
    "gld_emissions_ledger": ["asset_id", "gas_day"],
    "gld_energy_efficiency": ["gas_day"],
    "gld_failure_history": ["failure_id"],
    "gld_field_production": ["gas_day"],
    "gld_flow_assurance": ["asset_id", "gas_day"],
    "gld_functional_location": ["fl_id"],
    "gld_integrity_rbi": ["asset_id"],
    "gld_lab_assay": ["sample_id"],
    "gld_maintenance_reliability": ["asset_id"],
    "gld_margin": ["gas_day"],
    "gld_methane_ldar": ["asset_id"],
    "gld_notifications": ["notif_id"],
    "gld_plant_kpi": ["asset_id", "gas_day"],
    "gld_pred_liquid_loading": ["well_id"],
    "gld_pred_rul": ["asset_id"],
    "gld_produced_water": ["well_id", "gas_day"],
    "gld_recommendations": ["rec_id"],
    "gld_safety_alarms": ["area_code", "gas_day"],
    "gld_safety_events": ["event_id"],
    "gld_sites": ["area_code"],
    "gld_spares_risk": ["asset_id"],
    "gld_supply_network": ["from_id", "to_id"],
    "gld_supply_nodes": ["node_id"],
    "gld_tag_context": ["tag_id"],
    "gld_weather": ["area_code", "obs_date"],
    "gld_well_scorecard": ["well_id", "gas_day"],
    "gld_workorders": ["wo_id"],
}

# Indexes for the App's per-asset / per-site lookups (synced tables only index the primary key).
INDEXES = {
    "gld_tag_context": ["asset_id"],
    "gld_workorders": ["asset_id"],
    "gld_notifications": ["asset_id"],
    "gld_failure_history": ["asset_id"],
    "gld_lab_assay": ["asset_id"],
    "gld_deferment": ["asset_id"],
    "gld_well_scorecard": ["asset_id"],
    "gld_weather": ["area_code"],
    "gld_asset_state": ["scene_id"],
    "gld_asset_master": ["area_code"],
}

WRITEBACK_DDL = """
CREATE SCHEMA IF NOT EXISTS gas_twin;
CREATE TABLE IF NOT EXISTS gas_twin.recommendation_decisions (
  rec_id text PRIMARY KEY,
  asset_id text,
  constraint_type text,
  production_at_risk_e3m3d double precision,
  recommendation text,
  maturity_level text,
  evidence text,
  status text DEFAULT 'proposed',
  note text,
  decided_by text,
  decided_at timestamptz,
  created_at timestamptz DEFAULT now()
);
CREATE TABLE IF NOT EXISTS gas_twin.operator_rounds (
  round_id bigserial PRIMARY KEY, asset_id text, observation text, flag text,
  observed_by text, observed_at timestamptz DEFAULT now()
);
"""

# Spark SQL's ROUND(double, n) has no Postgres equivalent (Postgres rounds only numeric to n places).
# This overload keeps the App's SQL portable and returns a double, as the lakehouse did. It goes through
# the exact (shortest round-trip) text form so a value such as 8023.4999999999995 rounds down, as in Spark.
ROUND_SHIM = """CREATE OR REPLACE FUNCTION public.round(double precision, integer) RETURNS double precision
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS 'SELECT pg_catalog.round($1::text::numeric, $2)::double precision'"""


def log(*a):
    print(time.strftime("%H:%M:%S"), *a, flush=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--project", required=True, help="Lakebase project id")
    ap.add_argument("--catalog", required=True)
    ap.add_argument("--schema", required=True)
    ap.add_argument("--serving-schema", default="", help="UC + Postgres schema for the synced marts (default <schema>_app)")
    ap.add_argument("--app-name", default="gas-upstream-twin")
    ap.add_argument("--sync-cron", default="0 0/5 * * * ?", help="Quartz cron for the scheduled sync ('' = no schedule)")
    args = ap.parse_args()
    cat, sch = args.catalog, args.schema
    serving = args.serving_schema or f"{sch}_app"

    w = WorkspaceClient()
    api = w.api_client.do
    spark = SparkSession.builder.getOrCreate()

    proj = api("GET", f"/api/2.0/postgres/projects/{args.project}")
    branch = proj["status"]["default_branch"]
    eps = api("GET", f"/api/2.0/postgres/{branch}/endpoints").get("endpoints", [])
    ep = next(e for e in eps if e["status"].get("endpoint_type") == "ENDPOINT_TYPE_READ_WRITE")
    host = ep["status"]["hosts"]["host"]
    log("Lakebase", branch, "->", host)

    # ---- 1. synced tables ------------------------------------------------------------------
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{cat}`.`{serving}` COMMENT 'Lakebase synced tables: the App-serving copy of the gold marts in `{cat}`.`{sch}` (one per mart, same name). Read by the App from Postgres schema {serving}.'")
    existing = {}
    for t in SYNC:
        try:
            existing[t] = api("GET", f"/api/2.0/postgres/synced_tables/{cat}.{serving}.{t}")
        except Exception:
            pass
    pipeline_id = next((s["status"].get("pipeline_id") for s in existing.values() if s.get("status", {}).get("pipeline_id")), None)
    for t, pk in SYNC.items():
        if t in existing:
            continue
        spec = {"source_table_full_name": f"{cat}.{sch}.{t}", "branch": branch, "postgres_database": "databricks_postgres",
                "primary_key_columns": pk, "scheduling_policy": "SNAPSHOT", "create_database_objects_if_missing": True}
        if pipeline_id:
            spec["existing_pipeline_id"] = pipeline_id
        else:
            spec["new_pipeline_spec"] = {"storage_catalog": cat, "storage_schema": sch}
        api("POST", f"/api/2.0/postgres/synced_tables?synced_table_id={cat}.{serving}.{t}", body={"spec": spec})
        log("created synced table", t)
        if not pipeline_id:   # the first one creates the sync pipeline; the rest bin-pack into it
            for _ in range(60):
                pipeline_id = api("GET", f"/api/2.0/postgres/synced_tables/{cat}.{serving}.{t}").get("status", {}).get("pipeline_id")
                if pipeline_id:
                    break
                time.sleep(5)
    log("sync pipeline:", pipeline_id)

    # ---- 2. sync now (right after the medallion) and wait until every mart is online --------
    def pipeline_state():
        p = api("GET", f"/api/2.0/pipelines/{pipeline_id}")
        u = (p.get("latest_updates") or [{}])[0]
        return u.get("state"), u.get("update_id")

    def wait_idle():
        for _ in range(240):
            st, _uid = pipeline_state()
            if st not in ("QUEUED", "CREATED", "WAITING_FOR_RESOURCES", "INITIALIZING", "SETTING_UP_TABLES", "RUNNING", "STOPPING", "RESETTING"):
                return st
            time.sleep(10)
        return "TIMEOUT"

    wait_idle()   # creations kick off their own update; let it finish first
    for attempt in (1, 2, 3):
        try:
            api("POST", f"/api/2.0/pipelines/{pipeline_id}/updates", body={})
            break
        except Exception as e:   # an update started by a just-created table may still hold the pipeline
            log("start sync note:", str(e)[:160])
            time.sleep(20)
            wait_idle()
    st = wait_idle()
    log("sync pipeline update:", st)
    bad = []
    for _ in range(90):
        bad = []
        for t in SYNC:
            s = api("GET", f"/api/2.0/postgres/synced_tables/{cat}.{serving}.{t}").get("status", {})
            if not str(s.get("detailed_state", "")).startswith("SYNCED_TABLE_ONLINE"):
                bad.append((t, s.get("detailed_state"), (s.get("message") or "")[:160]))
        if not bad or any("FAILED" in str(b[1]) for b in bad):
            break
        time.sleep(10)
    if bad:
        for b in bad:
            log("NOT ONLINE:", *b)
        raise SystemExit(f"{len(bad)} synced table(s) did not come online")
    log(f"all {len(SYNC)} synced tables online in Postgres schema {serving}")

    # ---- 3. Postgres: helper function, indexes, write-back tables ------------------------------
    user = w.current_user.me().user_name
    tok = api("POST", "/api/2.0/postgres/credentials", body={"endpoint": ep["name"]})["token"]
    conn = psycopg.connect(host=host, port=5432, dbname="databricks_postgres", user=user, password=tok,
                           sslmode="require", autocommit=True)
    cur = conn.cursor()
    cur.execute(ROUND_SHIM)
    for t, cols in INDEXES.items():
        for c in cols:
            try:
                cur.execute(f'CREATE INDEX IF NOT EXISTS "ix_{t}_{c}" ON "{serving}"."{t}" ("{c}")')
            except Exception as e:
                log("index note:", t, c, str(e)[:120])
    cur.execute(WRITEBACK_DDL)
    recs = spark.read.table(f"`{cat}`.`{sch}`.gld_recommendations").collect()
    current = []
    for r in recs:
        rid = r["rec_id"]
        current.append(rid)
        cur.execute(
            """INSERT INTO gas_twin.recommendation_decisions
               (rec_id, asset_id, constraint_type, production_at_risk_e3m3d, recommendation, maturity_level, evidence, status)
               VALUES (%s,%s,%s,%s,%s,%s,%s,'proposed')
               ON CONFLICT (rec_id) DO UPDATE SET
                 constraint_type=EXCLUDED.constraint_type, recommendation=EXCLUDED.recommendation,
                 maturity_level=EXCLUDED.maturity_level, evidence=EXCLUDED.evidence,
                 production_at_risk_e3m3d=EXCLUDED.production_at_risk_e3m3d""",
            (rid, r["asset_id"], r["constraint_type"],
             float(r["production_at_risk_e3m3d"]) if r["production_at_risk_e3m3d"] is not None else None,
             r["recommendation"], r["maturity_level"], r["evidence"]))
    if current:
        cur.execute("DELETE FROM gas_twin.recommendation_decisions WHERE rec_id <> ALL(%s)", (current,))
    log(f"seeded {len(current)} recommendations")

    # ---- 4. the App's service principal: read the marts, read/write the write-back -------------
    app_sp = None
    try:
        app_sp = api("GET", f"/api/2.0/apps/{args.app_name}").get("service_principal_client_id")
    except Exception as e:
        log("app SP lookup note:", str(e)[:160])
    if app_sp:
        cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (app_sp,))
        if not cur.fetchone():
            try:
                api("POST", f"/api/2.0/postgres/{branch}/roles?role_id={app_sp}",
                    body={"spec": {"postgres_role": app_sp, "identity_type": "SERVICE_PRINCIPAL", "auth_method": "LAKEBASE_OAUTH_V1"}})
                log("created the App's Postgres role")
            except Exception as e:
                log("app role note:", str(e)[:160])
        for stmt in (f'GRANT USAGE ON SCHEMA "{serving}" TO "{app_sp}"',
                     f'GRANT SELECT ON ALL TABLES IN SCHEMA "{serving}" TO "{app_sp}"',
                     f'GRANT USAGE ON SCHEMA gas_twin TO "{app_sp}"',
                     f'GRANT SELECT, INSERT, UPDATE ON gas_twin.recommendation_decisions TO "{app_sp}"',
                     f'GRANT SELECT, INSERT ON gas_twin.operator_rounds TO "{app_sp}"',
                     f'GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA gas_twin TO "{app_sp}"'):
            try:
                cur.execute(stmt)
            except Exception as e:
                log("grant note:", stmt[:70], str(e)[:160])
        log("granted the App read on the marts and read/write on the write-back")
    conn.close()

    # ---- 5. scheduled re-sync -------------------------------------------------------------------
    name = f"[{cat}.{sch}] Gas Twin - Lakebase sync"
    settings = {"name": name, "max_concurrent_runs": 1,
                "description": "Re-syncs the gold marts into Lakebase for the App. A mart whose gold source has not changed is skipped.",
                "tasks": [{"task_key": "sync_gold_to_lakebase", "pipeline_task": {"pipeline_id": pipeline_id}}]}
    if args.sync_cron:
        settings["schedule"] = {"quartz_cron_expression": args.sync_cron, "timezone_id": "UTC", "pause_status": "UNPAUSED"}
    jobs = api("GET", "/api/2.1/jobs/list", query={"name": name}).get("jobs", [])
    if jobs:
        api("POST", "/api/2.1/jobs/reset", body={"job_id": jobs[0]["job_id"], "new_settings": settings})
        log("updated job", name, jobs[0]["job_id"])
    else:
        jid = api("POST", "/api/2.1/jobs/create", body=settings)["job_id"]
        log("created job", name, jid)


if __name__ == "__main__":
    main()
