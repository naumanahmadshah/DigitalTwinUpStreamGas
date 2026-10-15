"""Phase 6 GLD-03 — seed the Lakebase (Postgres) recommendation-decisions table + grant the app SP.

Reads the advisory recommendations from gold and upserts them into Postgres so the app's Recommendation
Inbox can accept/reject/annotate with full audit (the shadow-mode, no-autonomy surface, Design Part 4).
"""
from __future__ import annotations
import argparse
import psycopg
from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession

DDL = """
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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--instance", required=True)
    ap.add_argument("--catalog", required=True)
    ap.add_argument("--schema", required=True)
    ap.add_argument("--app-name", default="gas-upstream-twin")
    args = ap.parse_args()

    w = WorkspaceClient()
    app_sp = None
    try:
        app_sp = w.api_client.do("GET", f"/api/2.0/apps/{args.app_name}").get("service_principal_client_id")
    except Exception as e:
        print("app SP lookup note:", str(e)[:120])
    inst = w.api_client.do("GET", f"/api/2.0/database/instances/{args.instance}")
    host = inst["read_write_dns"]
    user = w.current_user.me().user_name
    tok = w.api_client.do("POST", "/api/2.0/database/credentials",
                          body={"request_id": "twinv2-setup", "instance_names": [args.instance]})["token"]

    conn = psycopg.connect(host=host, port=5432, dbname="databricks_postgres",
                           user=user, password=tok, sslmode="require", autocommit=True)
    cur = conn.cursor()
    cur.execute(DDL)

    spark = SparkSession.builder.getOrCreate()
    recs = spark.read.table(f"`{args.catalog}`.`{args.schema}`.gld_recommendations").collect()
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
    print(f"seeded {len(current)} recommendations")

    if app_sp:
        try:
            w.api_client.do("POST", f"/api/2.0/database/instances/{args.instance}/roles",
                            body={"name": app_sp, "identity_type": "SERVICE_PRINCIPAL"})
            print("created app SP Postgres role")
        except Exception as e:
            print("app SP role note:", str(e)[:120])
        try:
            cur.execute(f'GRANT USAGE ON SCHEMA gas_twin TO "{app_sp}"')
            cur.execute(f'GRANT SELECT, INSERT, UPDATE ON gas_twin.recommendation_decisions TO "{app_sp}"')
            cur.execute(f'GRANT SELECT, INSERT ON gas_twin.operator_rounds TO "{app_sp}"')
            cur.execute(f'GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA gas_twin TO "{app_sp}"')
            print("granted app SP read/write")
        except Exception as e:
            print("grant note:", str(e)[:120])
    conn.close()


if __name__ == "__main__":
    main()
