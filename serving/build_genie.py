"""Phase 6 — build the Genie space over the curated GOLD marts (Design 3.2).

Genie queries gold metric views only, never raw telemetry. Idempotent by title (create or patch).
Guidance bakes in the unit convention (e3m3 -> 10^3 m^3), the advisory/no-autonomy framing, the spec
limits, and an explicit instruction never to invent a confidence level.
"""
from __future__ import annotations
import argparse, hashlib, json
from databricks.sdk import WorkspaceClient

TITLE = "Gas Upstream Digital Twin"

MARTS = ["gld_field_production", "gld_well_scorecard", "gld_compression_health", "gld_plant_kpi",
         "gld_flow_assurance", "gld_emissions_ledger", "gld_integrity_rbi", "gld_deferment",
         "gld_safety_alarms", "gld_custody_allocation", "gld_maintenance_reliability", "gld_economics",
         "gld_constraint_ranking", "gld_recommendations", "gld_asset_state",
         "gld_pred_liquid_loading", "gld_pred_rul",
         "gld_lab_assay", "gld_commercial", "gld_weather"]

QUESTIONS = [
    "Which five wells lost the most gas to deferment last month, and what was the coded cause and dollar impact?",
    "What is the field's current gap to nomination, and which constraint is driving it?",
    "Reconcile allocated vs metered volumes and flag the largest unaccounted-for gas by area.",
    "List wells at high liquid-loading risk this week and the recommended intervention, ranked by gas upside.",
    "Which wells are underperforming vs their nodal potential, and what is the likely cause?",
    "Show every compressor whose vibration trend is rising and that is currently the field production constraint.",
    "Which streams are closest to breaching their dewpoint or H2S/CO2 spec right now?",
    "What equipment is due for inspection in the next 30 days and what production does it put at risk if it slips?",
    "Which rotating machines will fail next, and what is the production-at-risk?",
    "What is my carbon intensity per 10^3 m^3 this quarter by facility, and which flare or vent sources drove the change?",
    "What is lifting cost per GJ by facility and how has it trended?",
    "Which alarms are standing or in flood right now, and on which units?",
    "Show all ESD trips and safety-system bypasses in the last 30 days by area.",
    "Across the gathering network, where is backpressure or a bottleneck limiting throughput right now?",
    "What is the gas composition, H2S and water content from the latest lab assays by well?",
    "How have gas and carbon prices trended, and what is our nomination delivery versus contract?",
    "How do ambient temperature, wind and flood risk relate to our flaring and HSE exposure?",
    "What are the methane and NOx emissions by source this quarter for regulatory reporting?",
]

GUIDANCE = (
    "This space answers operational questions for an upstream gas digital twin over curated GOLD marts only, never raw telemetry. "
    "Grain is mostly per asset per gas day. Money is in the deferred_dollars, revenue_dollars, and cost columns. "
    "Production volumes are in 10^3 m^3 (thousand cubic metres); the e3m3 suffix denotes this unit, and a trailing d "
    "(for example production_at_risk_e3m3d) means per day. Refer to the unit as 10^3 m^3 in answers, not e3m3. "
    "'The field constraint' is in gld_constraint_ranking (ranked by production_at_risk_e3m3d). "
    "Asset health and the 3D pin colour are in gld_asset_state (green/amber/red). "
    "Recommendations in gld_recommendations are advisory only and human-in-the-loop; the twin never actuates. "
    "Spec limits: water dewpoint <= -10 C, H2S <= 4 ppm, CO2 <= 3 mol%. "
    "Do not state, estimate, or invent any confidence level, confidence score, or certainty percentage in answers; answer from the data only."
)


def _id(s):
    return hashlib.md5(s.encode()).hexdigest()


def build(cat, sch):
    tables = sorted([{"identifier": f"{cat}.{sch}.{n}"} for n in MARTS], key=lambda x: x["identifier"])
    return {
        "version": 2,
        "data_sources": {"tables": tables},
        "config": {"sample_questions": [{"id": _id(q), "question": [q]} for q in QUESTIONS]},
        "instructions": {"text_instructions": [{"id": _id("guidance"), "content": [GUIDANCE]}]},
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog", required=True)
    ap.add_argument("--schema", required=True)
    args = ap.parse_args()

    w = WorkspaceClient()
    wh = None
    for x in w.warehouses.list():
        wh = x.id
        if str(getattr(x, "state", "")).upper().endswith("RUNNING"):
            break
    if not wh:
        raise SystemExit("no SQL warehouse available")

    body = {"title": TITLE, "description": "Natural-language analytics over the upstream gas twin v2 gold marts.",
            "warehouse_id": wh, "serialized_space": json.dumps(build(args.catalog, args.schema))}
    existing = None
    for s in (w.api_client.do("GET", "/api/2.0/genie/spaces").get("spaces") or []):
        if s.get("title") == TITLE:
            existing = s.get("space_id") or s.get("id")
            break
    if existing:
        w.api_client.do("PATCH", f"/api/2.0/genie/spaces/{existing}", body=body)
        print(f"Updated Genie space {existing}")
    else:
        r = w.api_client.do("POST", "/api/2.0/genie/spaces", body=body)
        print(f"Created Genie space {r.get('space_id') or r.get('id')}")


if __name__ == "__main__":
    main()
