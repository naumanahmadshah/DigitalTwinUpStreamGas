"""Phase 6 — build the AI/BI (Lakeview) dashboard over the GOLD marts (Design 3.2).

MV-only: every dataset is a plain SELECT * from a gold MV. Widget spec versions follow what works on
this workspace: counter=v2, line/bar=v3, table=v1. Idempotent by display name (create or patch), then
publish with embedded credentials.
"""
from __future__ import annotations
import argparse, json
from databricks.sdk import WorkspaceClient

NAME = "Gas Upstream Digital Twin"


def _ds(name, table, cat, sch):
    return {"name": name, "displayName": name, "queryLines": [f"SELECT * FROM `{cat}`.`{sch}`.{table}"]}


def _pos(x, y, w, h):
    return {"x": x, "y": y, "width": w, "height": h}


def _counter(wname, dataset, field, expr, title):
    return {"widget": {"name": wname,
                       "queries": [{"name": "main_query", "query": {"datasetName": dataset,
                                    "fields": [{"name": field, "expression": expr}], "disaggregated": False}}],
                       "spec": {"version": 2, "widgetType": "counter", "frame": {"title": title, "showTitle": True},
                                "encodings": {"value": {"fieldName": field, "displayName": title}}}}}


def _line(wname, dataset, x, y, title):
    fields = [{"name": x, "expression": f"`{x}`"}, {"name": y, "expression": f"`{y}`"}]
    return {"widget": {"name": wname,
                       "queries": [{"name": "main_query", "query": {"datasetName": dataset, "fields": fields, "disaggregated": True}}],
                       "spec": {"version": 3, "widgetType": "line", "frame": {"title": title, "showTitle": True},
                                "encodings": {"x": {"fieldName": x, "scale": {"type": "temporal"}},
                                              "y": {"fieldName": y, "scale": {"type": "quantitative"}}}}}}


def _bar(wname, dataset, xdim, ymeasure, yexpr, title):
    fields = [{"name": xdim, "expression": f"`{xdim}`"}, {"name": ymeasure, "expression": yexpr}]
    return {"widget": {"name": wname,
                       "queries": [{"name": "main_query", "query": {"datasetName": dataset, "fields": fields, "disaggregated": False}}],
                       "spec": {"version": 3, "widgetType": "bar", "frame": {"title": title, "showTitle": True},
                                "encodings": {"x": {"fieldName": xdim, "scale": {"type": "categorical"}},
                                              "y": {"fieldName": ymeasure, "scale": {"type": "quantitative"}}}}}}


def _table(wname, dataset, columns):
    fields = [{"name": c, "expression": f"`{c}`"} for c in columns]
    numeric = ("score", "risk", "dollars", "e3m3", "eff", "vibration", "rul", "health", "pct", "days", "kg")
    cols = [{"fieldName": c, "type": "integer" if any(k in c for k in numeric) else "string",
             "displayAs": "number" if any(k in c for k in numeric) else "string", "visible": True, "order": i,
             "title": c, "allowSearch": False, "alignContent": "right" if any(k in c for k in numeric) else "left",
             "allowHTML": False, "highlightLinks": False, "useMonospaceFont": False, "preserveWhitespace": False,
             "booleanValues": ["false", "true"]} for i, c in enumerate(columns)]
    return {"widget": {"name": wname,
                       "queries": [{"name": "main_query", "query": {"datasetName": dataset, "fields": fields, "disaggregated": True}}],
                       "spec": {"version": 2, "widgetType": "table", "encodings": {"columns": cols}}}}


def build(cat, sch):
    datasets = [
        _ds("field_production", "gld_field_production", cat, sch),
        _ds("well_scorecard", "gld_well_scorecard", cat, sch),
        _ds("compression_health", "gld_compression_health", cat, sch),
        _ds("deferment", "gld_deferment", cat, sch),
        _ds("emissions", "gld_emissions_ledger", cat, sch),
        _ds("constraint", "gld_constraint_ranking", cat, sch),
        _ds("custody", "gld_custody_allocation", cat, sch),
    ]
    pages = []
    pages.append({"name": "field", "displayName": "Field Command Centre", "layout": [
        {"position": _pos(0, 0, 2, 4), **_counter("c_act", "field_production", "act", "AVG(`actual_e3m3`)", "Avg actual (10^3 m^3/d)")},
        {"position": _pos(2, 0, 2, 4), **_counter("c_gap", "field_production", "gap", "AVG(`gap_to_nomination_e3m3`)", "Avg gap to nom")},
        {"position": _pos(4, 0, 2, 4), **_counter("c_par", "constraint", "par", "SUM(`production_at_risk_e3m3d`)", "Production at risk (10^3 m^3/d)")},
        {"position": _pos(0, 4, 6, 8), **_line("l_prod", "field_production", "gas_day", "actual_e3m3", "Actual production vs time (10^3 m^3/d)")},
        {"position": _pos(0, 12, 6, 8), **_table("t_con", "constraint", ["rank", "constraint_type", "asset_id", "production_at_risk_e3m3d"])},
    ]})
    pages.append({"name": "wells", "displayName": "Wells & Lift", "layout": [
        {"position": _pos(0, 0, 6, 8), **_bar("b_well", "well_scorecard", "well_id", "produced", "SUM(`produced_e3m3`)", "Produced by well (10^3 m^3)")},
        {"position": _pos(0, 8, 6, 8), **_table("t_well", "well_scorecard", ["well_id", "lift_type", "completion_type", "utilization_pct", "liquid_loading_watch", "sssv_status"])},
    ]})
    pages.append({"name": "compression", "displayName": "Compression Health", "layout": [
        {"position": _pos(0, 0, 3, 8), **_bar("b_h", "compression_health", "asset_id", "health", "AVG(`health_score`)", "Health score by unit")},
        {"position": _pos(3, 0, 3, 8), **_line("l_v", "compression_health", "gas_day", "vibration_overall", "Vibration trend (mm/s)")},
        {"position": _pos(0, 8, 6, 8), **_table("t_c", "compression_health", ["asset_id", "health_score", "rul_days_proxy", "antisurge_margin_pct", "production_at_risk_e3m3d"])},
    ]})
    pages.append({"name": "deferment", "displayName": "Deferment & Emissions", "layout": [
        {"position": _pos(0, 0, 3, 4), **_counter("c_d", "deferment", "d", "SUM(`deferred_dollars`)", "Total deferment ($)")},
        {"position": _pos(3, 0, 3, 4), **_counter("c_ci", "emissions", "ci", "AVG(`carbon_intensity_kg_per_e3m3`)", "Carbon intensity (kg/10^3 m^3)")},
        {"position": _pos(0, 4, 6, 8), **_bar("b_def", "deferment", "cause_code", "dv", "SUM(`deferred_e3m3`)", "Deferred gas by cause (10^3 m^3)")},
        {"position": _pos(0, 12, 6, 8), **_table("t_def", "deferment", ["gas_day", "asset_id", "cause_desc", "deferred_e3m3", "deferred_dollars"])},
    ]})
    pages.append({"name": "custody", "displayName": "Custody & UAF", "layout": [
        {"position": _pos(0, 0, 6, 8), **_line("l_uaf", "custody", "gas_day", "uaf_pct", "Unaccounted-for gas (%)")},
        {"position": _pos(0, 8, 6, 8), **_table("t_uaf", "custody", ["gas_day", "asset_id", "metered_e3m3", "uaf_e3m3", "uaf_pct"])},
    ]})
    return {"datasets": datasets, "pages": pages}


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
    serialized = json.dumps(build(args.catalog, args.schema))
    user = w.current_user.me().user_name
    parent = f"/Users/{user}/gas_twin_dashboards"
    try:
        w.workspace.mkdirs(parent)
    except Exception:
        pass
    existing = None
    resp = w.api_client.do("GET", "/api/2.0/lakeview/dashboards", query={"page_size": 200})
    for d in (resp.get("dashboards") or []):
        if d.get("display_name") == NAME and not d.get("trashed_at"):
            existing = d.get("dashboard_id")
            break
    body = {"display_name": NAME, "warehouse_id": wh, "serialized_dashboard": serialized}
    if existing:
        w.api_client.do("PATCH", f"/api/2.0/lakeview/dashboards/{existing}", body=body)
        did = existing
        print(f"Updated dashboard {did}")
    else:
        body["parent_path"] = parent
        did = w.api_client.do("POST", "/api/2.0/lakeview/dashboards", body=body).get("dashboard_id")
        print(f"Created dashboard {did}")
    try:
        w.api_client.do("POST", f"/api/2.0/lakeview/dashboards/{did}/published",
                        body={"warehouse_id": wh, "embed_credentials": True})
        print("Published.")
    except Exception as e:
        print(f"Publish note: {e}")


if __name__ == "__main__":
    main()
