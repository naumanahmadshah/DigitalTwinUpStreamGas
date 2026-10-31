"""Upstream Gas Digital Twin v2: App backend (Design Part 3).

Serves the single-pane twin from Lakebase, the App's only database: the curated GOLD marts arrive there as
Lakebase synced tables (Postgres schema TWIN_PG_SCHEMA, one table per mart, same name), and recommendation
decisions are written back to Lakebase. Genie and the AI/BI dashboard read the same gold marts in the
lakehouse. Organised around the physical value chain: Command Centre + maps + one tab per value-chain stop +
Analytics, with a link out to the Genie space and a human-in-the-loop Recommendation Inbox.
MV-only: the marts carry the logic; routes issue plain SELECTs over them.
Each user's preferences live in Lakebase (gas_twin.settings). The Commercial & Finance briefing (the top
three commercial actions) is read from its gold mart, with a summary Genie writes in the background
through the conversation API and Lakebase caches (gas_twin.ai_briefings), so it is ready before it is opened.
"""
from __future__ import annotations
import concurrent.futures as _cf
import datetime as _dt
import hashlib
import json
import os
import re
import threading
import time as _time
from decimal import Decimal

import psycopg
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from databricks.sdk import WorkspaceClient

SCHEMA = os.environ["TWIN_SCHEMA"]
PG_SCHEMA = os.environ.get("TWIN_PG_SCHEMA") or f"{SCHEMA}_app"   # where the synced gold marts land
PG_ENDPOINT = os.environ["TWIN_PG_ENDPOINT"]                       # projects/<id>/branches/<id>/endpoints/<id>
PGHOST = os.environ.get("PGHOST") or os.environ.get("TWIN_PGHOST")  # PGHOST/PGUSER/PGDATABASE come from the app's Lakebase resource
PGDATABASE = os.environ.get("PGDATABASE") or "databricks_postgres"
GENIE_SPACE = os.environ.get("TWIN_GENIE_SPACE")  # optional; set post-deploy for the header Genie link

app = FastAPI(title="Upstream Gas Digital Twin v2")


# Make browsers revalidate the UI files on every load (cheap 304 via ETag when unchanged), so a redeploy
# shows up on a normal refresh instead of serving a stale cached app.js / style.css.
@app.middleware("http")
async def _no_stale_ui(request: Request, call_next):
    resp = await call_next(request)
    if not request.url.path.startswith("/api/"):
        resp.headers["Cache-Control"] = "no-cache"
    return resp

# Lakebase connection POOL + concurrent query execution: a screen runs its queries side by side (qmany).
# Lakebase signs in with a short-lived OAuth token, so the token is refreshed every 30 minutes and pooled
# connections are recycled after 40. The compute scales to zero when idle: the first query after a pause
# wakes it, and a connection broken by the pause is dropped and retried once on a fresh one. Pooled
# connections idle for more than 90 seconds are closed, because an open connection keeps the compute awake.
_pool: list = []
_pool_lock = threading.Lock()
_MAXPOOL = 8
_CONN_MAX_AGE = 40 * 60
_CONN_MAX_IDLE = 90
_cache: dict = {}
_tok = {"v": None, "t": 0.0}
_tok_lock = threading.Lock()
_ws = None


def T(name):
    return f'"{PG_SCHEMA}".{name}'


def _coerce(v):
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, _dt.datetime) and v.tzinfo is not None:
        v = v.astimezone(_dt.timezone.utc)   # UTC, as the lakehouse returned it
    if isinstance(v, (_dt.date, _dt.datetime)):
        return v.isoformat()
    return v


def _client():
    global _ws
    if _ws is None:
        _ws = WorkspaceClient()
    return _ws


def _token():
    with _tok_lock:
        if _tok["v"] is None or _time.time() - _tok["t"] > 30 * 60:
            _tok["v"] = _client().api_client.do("POST", "/api/2.0/postgres/credentials", body={"endpoint": PG_ENDPOINT})["token"]
            _tok["t"] = _time.time()
        return _tok["v"]


def _pg_user():
    return os.environ.get("PGUSER") or os.environ.get("DATABRICKS_CLIENT_ID") or _client().current_user.me().user_name


def _new_conn():
    c = psycopg.connect(host=PGHOST, port=int(os.environ.get("PGPORT") or 5432), dbname=PGDATABASE,
                        user=_pg_user(), password=_token(), sslmode="require", autocommit=True,
                        connect_timeout=30, application_name="gas-upstream-twin")
    c._born = _time.time()
    return c


def _acquire():
    with _pool_lock:
        while _pool:
            c = _pool.pop()
            if not c.closed and _time.time() - getattr(c, "_born", 0) < _CONN_MAX_AGE:
                return c
            try:
                c.close()
            except Exception:
                pass
    return _new_conn()


def _release(c, ok=True):
    if c is None:
        return
    if ok and not c.closed:
        c._used = _time.time()
        with _pool_lock:
            if len(_pool) < _MAXPOOL:
                _pool.append(c)
                return
    try:
        c.close()
    except Exception:
        pass


def _reap_idle():
    """Close pooled connections nobody has used for _CONN_MAX_IDLE seconds, so Lakebase can scale to zero."""
    while True:
        _time.sleep(30)
        now = _time.time()
        with _pool_lock:
            stale = [c for c in _pool if now - getattr(c, "_used", 0) > _CONN_MAX_IDLE]
            _pool[:] = [c for c in _pool if c not in stale]
        for c in stale:
            try:
                c.close()
            except Exception:
                pass


threading.Thread(target=_reap_idle, daemon=True, name="pg-idle-reaper").start()


def q(statement, params=None):
    for attempt in (1, 2):
        c = None
        try:
            c = _acquire()
            with c.cursor() as cur:
                cur.execute(statement, params or None)
                if cur.description is None:
                    rows = []
                else:
                    cols = [d.name for d in cur.description]
                    rows = [{col: _coerce(v) for col, v in zip(cols, r)} for r in cur.fetchall()]
            _release(c, True)
            return rows
        except (psycopg.OperationalError, psycopg.InterfaceError):
            _release(c, ok=False)   # drop a broken connection (e.g. the compute scaled to zero); retry on a fresh one
            if attempt == 2:
                raise
        except Exception:
            _release(c, ok=False)
            raise


def qmany(items):
    """Run several SQL statements concurrently (each on its own pooled connection), preserving order.
    Each item is a SQL string or a (sql, params) tuple. Falls back to serial for a single item."""
    def run(it):
        return q(it[0], it[1]) if isinstance(it, (list, tuple)) else q(it)
    if len(items) <= 1:
        return [run(it) for it in items]
    with _cf.ThreadPoolExecutor(max_workers=min(_MAXPOOL, len(items))) as ex:
        return list(ex.map(run, items))


def _sv(res):
    """First column of the first row of a query result (the scalar), or None."""
    return (list(res[0].values())[0] if res and res[0] else None)


def _scalar(statement):
    return _sv(q(statement))


def _since(table, col="gas_day", days=61):
    """WHERE fragment: rows within `days` of the latest day in `table` (day columns may be DATE or ISO text)."""
    return f"{col}::date >= (SELECT MAX({col})::date FROM {table}) - {days}"


def _month(col="gas_day"):
    """The YYYY-MM month of a DATE or ISO-text day column."""
    return f"left({col}::text, 7)"


_MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]


def _fmt_day(s):
    try:
        y, m, d = str(s)[:10].split("-")
        return f"{int(d)} {_MONTHS[int(m) - 1]} {y}"
    except Exception:
        return str(s)


def _data_period():
    """The actual date span of the field data, so period-summed tiles can state their period (item 23)."""
    def f():
        r = q(f"SELECT MIN(gas_day) a, MAX(gas_day) b, MAX(gas_day)::date - MIN(gas_day)::date + 1 n FROM {T('gld_field_production')}")
        if r and r[0].get("a"):
            return f"{_fmt_day(r[0]['a'])} to {_fmt_day(r[0]['b'])} ({r[0]['n']} days)"
        return None
    return cached("data_period", 600, f)


def cached(key, ttl, fn):
    now = _time.time()
    hit = _cache.get(key)
    if hit and now - hit[0] < ttl:
        return hit[1]
    val = fn()
    _cache[key] = (now, val)
    return val


# ============================ Navigation / modules ===========================================
NAV = [
    {"key": "command_centre", "label": "Command Centre", "group": "Home", "level": "Live"},
    {"key": "reservoir_wells", "label": "Reservoirs & Wells", "group": "Value chain", "level": "Predictive"},
    {"key": "gathering", "label": "Gathering & Flow Assurance", "group": "Value chain", "level": "Predictive"},
    {"key": "processing", "label": "Processing Facility / CPF", "group": "Value chain", "level": "Predictive"},
    {"key": "compression", "label": "Compression Health", "group": "Value chain", "level": "Prescriptive (shadow)"},
    {"key": "custody", "label": "Custody Transfer & Export", "group": "Value chain", "level": "Informative"},
    {"key": "integrity", "label": "Integrity & RBI", "group": "Cross-cutting", "level": "Predictive"},
    {"key": "safety", "label": "Safety & Alarms", "group": "Cross-cutting", "level": "Informative"},
    {"key": "emissions", "label": "Emissions, Carbon & Safeguard", "group": "Cross-cutting", "level": "Informative"},
    {"key": "deferment", "label": "Deferment & Losses", "group": "Commercial", "level": "Informative"},
    {"key": "economics", "label": "Cost & Economics", "group": "Commercial", "level": "Informative"},
    {"key": "maintenance", "label": "Maintenance", "group": "Cross-cutting", "level": "Predictive"},
    {"key": "lab", "label": "Lab & Assay", "group": "Cross-cutting", "level": "Informative"},
    {"key": "commercial", "label": "Commercial & Prices", "group": "Commercial", "level": "Informative"},
    {"key": "weather", "label": "Weather", "group": "Cross-cutting", "level": "Informative"},
    {"key": "produced_water", "label": "Produced Water", "group": "Cross-cutting", "level": "Informative"},
    {"key": "energy", "label": "Fuel Gas & Energy", "group": "Cross-cutting", "level": "Informative"},
    {"key": "methane_ldar", "label": "Methane LDAR", "group": "Cross-cutting", "level": "Predictive"},
    {"key": "margin", "label": "Molecule-to-Market Margin", "group": "Commercial", "level": "Informative"},
    {"key": "spares_risk", "label": "Spares & Procurement", "group": "Cross-cutting", "level": "Predictive"},
    {"key": "capital_plan", "label": "Capital & Decline-Aware Investment", "group": "Commercial", "level": "Informative"},
    {"key": "recommendations", "label": "Recommendation Inbox", "group": "Inbox", "level": "Human-in-the-loop"},  # own menu item, after Analytics
    {"key": "asset_model", "label": "Asset Explorer", "group": "Others", "level": "Governed"},
]


# Human column labels + a one-line meaning for the info 'i'. Defined here (backend/model side) so surfaces
# never show raw snake_case or coded units. (key -> (label, description))
COL_META = {
    "well_id": ("Well ID", "Unique well identifier."),
    "asset_id": ("Asset ID", "Contextualised asset tag (AREA-SYSTEM-CLASS-NNNN), the universal join key."),
    "area_code": ("Area", "Field area / basin the asset sits in."),
    "pad_area": ("Pad area", "Well-pad grouping."),
    "completion_type": ("Completion", "Well completion type (CSG or conventional)."),
    "lift_type": ("Lift type", "Artificial-lift method (ESP, gas lift, plunger, natural)."),
    "utilization_pct": ("Utilisation %", "Production as a share of recent peak deliverability."),
    "risk_score": ("Risk score", "Composite liquid-loading risk score for the well."),
    "liquid_loading_watch": ("Loading watch", "Whether the well is on the liquid-loading watchlist."),
    "gas_upside_e3m3": ("Gas upside (10³ m³)", "Gas recoverable if the loading risk is relieved."),
    "line_pressure": ("Line pressure (kPa)", "Average gathering-line pressure."),
    "line_flow": ("Line flow (10³ m³/d)", "Average gathering-line gas flow."),
    "line_temp": ("Line temp (°C)", "Average gathering-line temperature."),
    "hydrate_margin_c": ("Hydrate margin (°C)", "Approach margin: how far the line temperature sits above the gas-hydrate formation temperature. Lower = closer to forming hydrates (watch < 8 °C, at risk < 3 °C)."),
    "hydrate_formation_temp_c": ("Hydrate point (°C)", "Gas-hydrate formation temperature at the line pressure; the line must stay above this."),
    "hydrate_status": ("Hydrate", "Hydrate approach status: at risk / watch / ok (line temperature vs the hydrate formation temperature)."),
    "erosion_risk": ("Erosion risk", "Modelled erosion risk from flow velocity."),
    "pig_due_days": ("Pig due (days)", "Days until the next pigging run is due."),
    "system_code": ("System", "Process-system code (SEP, CMP, DEH, AMN, NGL, MET, …)."),
    "water_dewpoint": ("Water dewpoint (°C)", "Treated-gas water dewpoint vs the ≤ -10 °C spec."),
    "h2s_outlet": ("H₂S outlet (ppm)", "Treated-gas H₂S vs the ≤ 4 ppm spec."),
    "co2_outlet": ("CO₂ outlet (Mol %)", "Treated-gas CO₂ vs the ≤ 3 mol % spec."),
    "ngl_recovery": ("NGL recovery (%)", "NGL recovery against target."),
    "health_score": ("Health score", "Composite 0–100 health from vibration, efficiency and condition."),
    "rul_days_proxy": ("RUL (days)", "Model estimate of remaining useful life."),
    "rul_days": ("RUL (days)", "Model estimate of remaining useful life."),
    "antisurge_margin_pct": ("Anti-surge margin (%)", "Margin before a compressor surge trip."),
    "production_at_risk_e3m3d": ("Production at risk (10³ m³/d)", "Production lost if the constraint is not relieved."),
    "metered_e3m3": ("Metered (10³ m³)", "Custody-metered volume."),
    "uaf_e3m3": ("UAF (10³ m³)", "Unaccounted-for gas (metered minus allocated)."),
    "uaf_pct": ("UAF %", "Unaccounted-for gas as a share of metered volume."),
    "equipment_type": ("Equipment", "Equipment type."),
    "inspection_due_days": ("Inspection due (days)", "Days until the risk-based inspection is due."),
    "remaining_life_years": ("Remaining life (yrs)", "Estimated years before an integrity limit is reached."),
    "rbi_status": ("RBI status", "Risk-based-inspection status."),
    "gas_day": ("Gas day", "Production gas day."),
    "event_time": ("Event time", "Timestamp of the safety event."),
    "event": ("Event", "Safety event type (ESD trip, F&G detection, critical alarm, emission exceedance)."),
    "severity": ("Severity", "Event severity (critical / warning / advisory)."),
    "alarm_count": ("Alarms", "Process alarms recorded that day."),
    "esd_trips": ("ESD trips", "Emergency-shutdown trips (status only; the twin never actuates)."),
    "fg_detections": ("F&G detections", "Fire & gas detector activations."),
    "critical_events": ("Critical events", "Events classed critical."),
    "ch4_kg": ("CH₄ (kg)", "Methane mass emitted."),
    "co2_kg": ("CO₂ (kg)", "CO₂ mass emitted."),
    "nox_kg": ("NOx (kg)", "NOx mass emitted."),
    "cause_desc": ("Cause", "Coded deferment cause (ISO 14224 style)."),
    "deferred_e3m3": ("Deferred (10³ m³)", "Lost-production volume."),
    "deferred_dollars": ("Deferred ($)", "Lost-production value at the gas price."),
    "revenue_dollars": ("Revenue ($)", "Produced gas valued at the gas price."),
    "revenue": ("Revenue ($)", "Produced gas valued at the gas price."),
    "deferred": ("Deferred ($)", "Lost-production value at the gas price."),
    "carbon_cost_dollars": ("Carbon cost ($)", "Cost of CO₂-e emissions at the carbon price."),
    "open_wos": ("Open WOs", "Open + in-progress work orders."),
    "completed_wos": ("Completed WOs", "Completed work orders."),
    "failures": ("Failures", "Recorded failure events."),
    "wo_cost": ("WO cost ($)", "Total work-order cost (labour + parts)."),
    "wo_id": ("WO ID", "Work-order identifier."),
    "wo_type": ("WO type", "Work-order type (PM, CM, inspection, calibration)."),
    "priority": ("Priority", "Work-order priority (P1–P4)."),
    "status": ("Status", "Current status."),
    "created_date": ("Created", "Creation date."),
    "cost": ("Cost ($)", "Work-order cost (labour + parts)."),
    "notif_id": ("Notification ID", "Notification identifier."),
    "notif_type": ("Type", "Notification type (anomaly, inspection-due, corrosion, operator flag)."),
    "raised_date": ("Raised", "Date the notification was raised."),
    "failure_id": ("Failure ID", "Failure-record identifier."),
    "failure_date": ("Failure date", "Date of the failure."),
    "iso14224_code": ("Failure mode (ISO 14224)", "Failure mode in the ISO 14224 reliability taxonomy."),
    "failure_mode": ("Failure mode", "Observed failure mode."),
    "downtime_hours": ("Downtime (h)", "Downtime caused by the failure."),
    "sample_id": ("Sample ID", "Lab sample identifier."),
    "sample_point": ("Sample point", "Where in the value chain the sample was taken (wellhead, inlet separator, treated gas, sales gas / custody)."),
    "sample_date": ("Sample date", "Date the sample was taken."),
    "c1_molpct": ("C₁ (Mol %)", "Methane mole fraction in the sampled gas."),
    "co2_molpct": ("CO₂ (Mol %)", "CO₂ mole fraction in the sampled gas."),
    "n2_molpct": ("N₂ (Mol %)", "Nitrogen mole fraction in the sampled gas."),
    "h2s_ppm": ("H₂S (ppm)", "Hydrogen sulphide concentration."),
    "water_ppm": ("Water (ppm)", "Water content."),
    "oil_in_water_ppm": ("Oil in water (ppm)", "Oil-in-water concentration."),
    "gas_price_gj": ("Sales gas price ($/GJ)", "The daily sales-gas price the operator SELLS at (the gas-sales-agreement reference price for the day), not a buying or spot-only price."),
    "carbon_price_t": ("Carbon price ($/t)", "The market / ACCU carbon price for the day (what it costs to offset a tonne of CO₂-e), not a price the operator receives."),
    "nominated_e3m3": ("Nominated (10³ m³)", "The buyer's contractual nomination for the day."),
    "actual_e3m3": ("Delivered (10³ m³)", "Gas actually delivered to the buyer."),
    "nomination_delivery_pct": ("Delivery vs nomination %", "Gas delivered as a share of the day's CONTRACTUAL NOMINATION (not of production). Can exceed 100% when the buyer nominates up and the field delivers above the base nomination."),
    "cost_of_generation_dollars": ("Cost of generation ($)", "Lifting cost to produce the day's gas (operating cost)."),
    "profit_dollars": ("Profit ($)", "Sale revenue minus cost of generation and carbon cost."),
    "obs_date": ("Date", "Observation date."),
    "ambient_c": ("Ambient (°C)", "Ambient air temperature."),
    "wind_kmh": ("Wind (km/h)", "Wind speed."),
    "lightning_risk": ("Lightning risk", "Modelled lightning-risk index (0–1)."),
    "flood_risk": ("Flood risk", "Modelled flood-risk index (0–1)."),
    "lat": ("Latitude", "Latitude."),
    "lon": ("Longitude", "Longitude."),
    "tubing_pressure": ("Tubing pressure (kPa)", "Flowing tubing-head pressure; its decline tracks reservoir depletion."),
    # cross-BU / sustainability fusions
    "water_m3": ("Produced water (m³/d)", "Produced-water rate = gas rate × the well's PVT water-gas ratio."),
    "reinjected_m3": ("Reinjected (m³/d)", "Produced water routed to reinjection."),
    "treated_m3": ("Treated (m³/d)", "Produced water routed to treatment / disposal."),
    "disposal_cost_dollars": ("Disposal cost ($)", "Cost of produced-water reinjection + treatment."),
    "fuel_gas_e3m3": ("Fuel gas (10³ m³/d)", "Own-use fuel gas burned for compression / power."),
    "flare_e3m3": ("Flare (10³ m³/d)", "Gas flared."),
    "field_e3m3": ("Field production (10³ m³/d)", "Total field gas production."),
    "own_use_pct": ("Own-use %", "Fuel gas as a share of production (lower is better)."),
    "flaring_pct": ("Flaring %", "Flared gas as a share of production (lower is better)."),
    "reporting_year": ("Year", "Safeguard reporting year."),
    "facility": ("Facility", "Designated Safeguard facility."),
    "scope1_t": ("Scope-1 (t)", "Scope-1 CO₂ emitted."),
    "baseline_t": ("Baseline (t)", "Best-practice Safeguard baseline for the facility."),
    "position_t": ("Position (t)", "Scope-1 minus baseline; positive = over baseline (shortfall)."),
    "accu_cost_dollars": ("ACCU cost ($)", "Cost to cover a shortfall with carbon credits at the carbon price."),
    "ch4_kg_d": ("CH₄ (kg/d)", "Methane emitted (measured + factor-based fugitive)."),
    "co2e_t_yr": ("CO₂-e (t/yr)", "Methane as CO₂-equivalent (GWP 28), annualised."),
    "detections": ("Detections", "Fire & gas / exceedance detector activations attributed to the asset."),
    "product_value_dollars_yr": ("Lost gas value ($/yr)", "Annual market value of the gas currently escaping as this leak: gas the operator keeps and sells (instead of losing it to atmosphere) once the leak is repaired."),
    "survey_status": ("Survey status", "LDAR triage: investigate / monitor / ok."),
    "netback_dollars": ("Netback ($)", "Revenue minus lifting and carbon cost."),
    "netback_per_gj": ("Netback ($/GJ)", "Unit netback margin."),
    "committed_e3m3": ("Committed (10³ m³)", "Contracted daily quantity across gas-sales agreements."),
    "take_or_pay_shortfall_e3m3": ("Take-or-pay shortfall (10³ m³)", "Volume below the take-or-pay floor (penalty exposure)."),
    "part_name": ("To procure", "The specific part, repair or service to procure when this asset's spare is the binding risk."),
    "procurement_type": ("Procurement", "What to procure: a part, a repair or a service."),
    "unit_cost": ("Unit cost ($)", "Cost of the part / repair / service to procure."),
    "lead_time_days": ("Lead time (days)", "Procurement lead time for the applicable spare."),
    "on_hand": ("On hand", "Units of the applicable spare in stock."),
    "exposure": ("Exposure", "Spares risk: at risk / watch / ok (lead time vs RUL)."),
    "gap": ("Lead − RUL (days)", "Procurement lead time minus remaining useful life (positive = exposed)."),
    "project_id": ("Project ID", "Development / capital project id."),
    "project_type": ("Project type", "Infill well, workover, debottleneck, compression upgrade."),
    "capex_dollars": ("Capex ($)", "Capital cost."),
    "expected_uplift_e3m3d": ("Expected uplift (10³ m³/d)", "Production uplift the project is expected to deliver."),
    "annual_value_dollars": ("Annual value ($)", "Uplift valued at the gas price over a year."),
    "payback_years": ("Payback (yrs)", "Capex divided by annual value."),
    "planned_date": ("Planned", "Planned execution date."),
    "assigned_to": ("Assigned to", "Crew the work order is assigned to (from the HR roster)."),
    "target_completion": ("Target completion", "Scheduled completion date/time (priority SLA)."),
    "overdue": ("Overdue", "Open work order past its target completion."),
    "customer_name": ("Customer", "The buyer (fictional)."),
    "contract_id": ("Contract", "Gas-sales agreement (GSA) or LNG sale-and-purchase agreement (SPA) id."),
    "contract_type": ("Contract type", "Firm GSA, LNG SPA or Spot."),
    "price_gj": ("Price ($/GJ)", "Contract price; for spot buyers the average price received over the last 30 days."),
    "mdq_tj": ("MDQ (TJ/d)", "Maximum daily quantity the buyer can call for."),
    "take_or_pay_pct": ("Take-or-pay (%)", "Share of MDQ the buyer pays for even if it does not take it (0 = none)."),
    "nominated_tj_d": ("Nominated (TJ/d)", "Average daily nomination over the last 30 gas days."),
    "delivered_tj_d": ("Delivered (TJ/d)", "Average energy delivered per gas day over the last 30 gas days."),
    "shortfall_days_30d": ("Short days (30 d)", "Gas days in the last 30 delivered below the buyer's nomination."),
    "contract_end": ("Contract end", "When the contract expires (the renewal and re-pricing point)."),
}

_UNIT_SUFFIX = {"e3m3d": "(10³ m³/d)", "e3m3": "(10³ m³)", "molpct": "(Mol %)", "pct": "(%)",
                "ppm": "(ppm)", "kg": "(kg)", "gj": "($/GJ)", "kpa": "(kPa)", "dollars": "($)",
                "days": "(days)", "hours": "(h)", "kmh": "(km/h)"}


def _humanise_col(name):
    """Fallback label for a column not in COL_META: drop snake_case, map a trailing unit token, title-case."""
    parts = name.split("_")
    unit = ""
    if parts[-1] in _UNIT_SUFFIX:
        unit = " " + _UNIT_SUFFIX[parts[-1]]
        parts = parts[:-1]
    label = " ".join(parts).strip().capitalize() or name
    for a, b in (("Co2", "CO₂"), ("Ch4", "CH₄"), ("H2s", "H₂S"), ("N2", "N₂"), ("Id", "ID")):
        label = label.replace(a, b)
    return label + unit


def _cols(names):
    """Turn a list of raw column names into [{key,label,desc}] for the frontend (human header + info 'i')."""
    out = []
    for n in names:
        lab, desc = COL_META.get(n, (_humanise_col(n), ""))
        out.append({"key": n, "label": lab, "desc": desc})
    return out


def _specs():
    m = T
    return {
        "reservoir_wells": {
            "title": "Reservoirs & Wells", "group": "Value chain", "level": "Predictive",
            "blurb": "Reservoir pressure and depletion vs plan, water encroachment, PVT and reserves, together with per-well nodal performance, liquid-loading watch with ranked intervention, artificial lift and SSSV status.",
            "kpis": [("Producing wells", f"SELECT COUNT(*) v FROM {m('gld_well_scorecard')} WHERE gas_day=(SELECT MAX(gas_day) FROM {m('gld_well_scorecard')})"),
                     ("CSG wells", f"SELECT COUNT(DISTINCT well_id) v FROM {m('gld_well_scorecard')} WHERE completion_type='csg'"),
                     ("Mean utilization (%)", f"SELECT ROUND(AVG(utilization_pct),1) v FROM {m('gld_well_scorecard')}"),
                     ("Wells on liquid-loading watch", f"SELECT COUNT(*) v FROM {m('gld_pred_liquid_loading')} WHERE liquid_loading_watch='watch'"),
                     ("Wells SSSV not passed", f"SELECT COUNT(*) v FROM (SELECT sssv_status, ROW_NUMBER() OVER (PARTITION BY well_id ORDER BY gas_day DESC NULLS LAST) rn FROM {m('gld_well_scorecard')}) WHERE rn=1 AND sssv_status='not_passed'")],
            # wellhead pressure by basin as % of its starting level (a CSG well runs at ~1 MPa, an offshore well at
            # ~15 MPa, so absolute kPa would flatten the onshore basins); spikes are shut-ins building up
            # (plant turnarounds), the drift is depletion
            "charts": [("Wellhead pressure by basin (% of initial)", "line", "gas_day", "Carnarvon",
                        f"""WITH b AS (SELECT w.gas_day, substring(st.name from '([A-Za-z-]+) Basin\\)$') basin, AVG(w.tubing_pressure) p
                                       FROM {m('gld_well_scorecard')} w JOIN {m('gld_sites')} st ON st.area_code = w.pad_area GROUP BY 1, 2),
                                 i AS (SELECT basin, AVG(p) p0 FROM b WHERE gas_day < (SELECT MIN(gas_day) FROM b) + 30 GROUP BY basin),
                                 r AS (SELECT b.gas_day, b.basin, 100.0 * b.p / NULLIF(i.p0, 0) v FROM b JOIN i ON i.basin = b.basin)
                            SELECT gas_day,
                                   ROUND(MAX(CASE WHEN basin = 'Carnarvon' THEN v END), 1) "Carnarvon",
                                   ROUND(MAX(CASE WHEN basin = 'Browse' THEN v END), 1) "Browse",
                                   ROUND(MAX(CASE WHEN basin = 'Gippsland' THEN v END), 1) "Gippsland",
                                   ROUND(MAX(CASE WHEN basin = 'Surat' THEN v END), 1) "Surat",
                                   ROUND(MAX(CASE WHEN basin = 'Bowen' THEN v END), 1) "Bowen",
                                   ROUND(MAX(CASE WHEN basin = 'Cooper-Eromanga' THEN v END), 1) "Cooper"
                            FROM r GROUP BY gas_day ORDER BY gas_day""",
                        {"series": [{"key": "Carnarvon", "label": "Carnarvon", "color": "#0c5a66"}, {"key": "Browse", "label": "Browse", "color": "#3a8a96"},
                                    {"key": "Gippsland", "label": "Gippsland", "color": "#6a5cf0"}, {"key": "Surat", "label": "Surat", "color": "#2f7dc4"},
                                    {"key": "Bowen", "label": "Bowen", "color": "#c98a1e"}, {"key": "Cooper", "label": "Cooper-Eromanga", "color": "#dd4b1a"}]}),
                       # the average well in each basin: big offshore wells make 20 to 30 times a CSG well
                       ("Average rate per well by basin, latest 30 days (10³ m³/d)", "bar", "basin", "rate",
                        f"""SELECT substring(st.name from '([A-Za-z-]+) Basin\\)$') basin, ROUND(AVG(w.produced_e3m3), 1) rate
                            FROM {m('gld_well_scorecard')} w JOIN {m('gld_sites')} st ON st.area_code = w.pad_area
                            WHERE w.gas_day >= (SELECT MAX(gas_day) FROM {m('gld_well_scorecard')}) - 30
                            GROUP BY 1 ORDER BY 2 DESC""")],
            "tables": [(f"SELECT well_id, completion_type, lift_type, ROUND(AVG(utilization_pct),1) utilization_pct FROM {m('gld_well_scorecard')} GROUP BY well_id, completion_type, lift_type ORDER BY utilization_pct LIMIT 500",
                        ["well_id", "completion_type", "lift_type", "utilization_pct"], "Wells by utilization (lowest first)"),
                       (f"SELECT well_id, pad_area, lift_type, ROUND(risk_score,2) risk_score, liquid_loading_watch, ROUND(gas_upside_e3m3,1) gas_upside_e3m3 FROM {m('gld_pred_liquid_loading')} ORDER BY risk_score DESC NULLS LAST, gas_upside_e3m3 DESC NULLS LAST, well_id LIMIT 500",
                        ["well_id", "pad_area", "lift_type", "risk_score", "liquid_loading_watch", "gas_upside_e3m3"], "Liquid-loading watchlist (ranked by risk)")],
        },
        "gathering": {
            "title": "Gathering & Flow Assurance", "group": "Value chain", "level": "Predictive",
            "blurb": "Hydrate/erosion/slugging risk in gathering lines, backpressure and bottlenecks, and pig scheduling.",
            "kpis": [("Segments on hydrate watch", f"SELECT COUNT(DISTINCT asset_id) v FROM {m('gld_flow_assurance')} WHERE hydrate_status IN ('watch','at_risk')"),
                     ("Min hydrate margin (°C)", f"SELECT ROUND(MIN(hydrate_margin_c),1) v FROM {m('gld_flow_assurance')}"),
                     ("Pig runs due < 14 days", f"SELECT COUNT(DISTINCT asset_id) v FROM {m('gld_flow_assurance')} WHERE pig_due_days<14"),
                     ("Avg line pressure (kPa)", f"SELECT ROUND(AVG(line_pressure),0) v FROM {m('gld_flow_assurance')}")],
            # worst-case (coldest) approach margin per segment, so the chart, table and KPIs tell one story
            "charts": [("Worst-case hydrate approach margin by segment (°C)", "bar", "asset_id", "margin",
                        f"SELECT asset_id, ROUND(MIN(hydrate_margin_c),1) margin FROM {m('gld_flow_assurance')} GROUP BY asset_id ORDER BY asset_id", {"color": "lowbad"})],
            "table": (f"SELECT asset_id, area_code, ROUND(AVG(line_pressure),0) line_pressure, ROUND(AVG(line_flow),1) line_flow, ROUND(MIN(line_temp),1) line_temp, ROUND(AVG(hydrate_formation_temp_c),1) hydrate_formation_temp_c, ROUND(MIN(hydrate_margin_c),1) hydrate_margin_c, CASE WHEN MIN(hydrate_margin_c)<3 THEN 'at_risk' WHEN MIN(hydrate_margin_c)<8 THEN 'watch' ELSE 'ok' END hydrate_status, MIN(pig_due_days) pig_due_days FROM {m('gld_flow_assurance')} GROUP BY asset_id, area_code ORDER BY asset_id LIMIT 60",
                      ["asset_id", "area_code", "line_pressure", "line_flow", "line_temp", "hydrate_formation_temp_c", "hydrate_margin_c", "hydrate_status", "pig_due_days"], "Gathering segments (worst-case approach)"),
        },
        "processing": {
            "title": "Processing Facility / CPF", "group": "Value chain", "level": "Predictive",
            "blurb": "Dehydration and amine spec compliance (dewpoint, H2S/CO2), NGL recovery, throughput. Streams flagged marginal are within 20% of a contractual limit.",
            "kpis": [("Streams off dewpoint spec", f"SELECT COUNT(*) v FROM {m('gld_plant_kpi')} WHERE gas_day=(SELECT MAX(gas_day) FROM {m('gld_plant_kpi')}) AND dewpoint_on_spec=false"),
                     ("Streams marginal", f"SELECT COUNT(*) v FROM {m('gld_plant_kpi')} WHERE gas_day=(SELECT MAX(gas_day) FROM {m('gld_plant_kpi')}) AND marginal=true"),
                     ("Min NGL recovery (%)", f"SELECT ROUND(MIN(ngl_recovery),1) v FROM {m('gld_plant_kpi')}")],
            "charts": [("Water dewpoint trend (C)", "line", "gas_day", "dp",
                        f"SELECT gas_day, ROUND(AVG(water_dewpoint),2) dp FROM {m('gld_plant_kpi')} WHERE water_dewpoint IS NOT NULL GROUP BY gas_day ORDER BY gas_day"),
                       ("H2S outlet trend (ppm)", "line", "gas_day", "h2s",
                        f"SELECT gas_day, ROUND(AVG(h2s_outlet),2) h2s FROM {m('gld_plant_kpi')} WHERE h2s_outlet IS NOT NULL GROUP BY gas_day ORDER BY gas_day")],
            "table": (f"SELECT asset_id, system_code, ROUND(AVG(water_dewpoint),2) water_dewpoint, ROUND(AVG(h2s_outlet),2) h2s_outlet, ROUND(AVG(co2_outlet),2) co2_outlet, ROUND(AVG(ngl_recovery),1) ngl_recovery FROM {m('gld_plant_kpi')} GROUP BY asset_id, system_code ORDER BY asset_id",
                      ["asset_id", "system_code", "water_dewpoint", "h2s_outlet", "co2_outlet", "ngl_recovery"], "Processing streams vs spec"),
        },
        "compression": {
            "title": "Compression Health", "group": "Value chain", "level": "Prescriptive (shadow)",
            "blurb": "Per-compressor efficiency, vibration, anti-surge margin, RUL proxy and production-at-risk.",
            "kpis": [("Mean health score", f"SELECT ROUND(AVG(health_score),1) v FROM {m('gld_compression_health')} WHERE gas_day=(SELECT MAX(gas_day) FROM {m('gld_compression_health')})"),
                     ("Min RUL proxy (days)", f"SELECT ROUND(MIN(rul_days),0) v FROM {m('gld_pred_rul')}"),
                     ("Production at risk (10^3 m^3/d)", f"SELECT ROUND(SUM(par),0) v FROM (SELECT asset_id, MAX(production_at_risk_e3m3d) par FROM {m('gld_compression_health')} GROUP BY asset_id)")],
            # health as of the latest gas day (a two-year average hides a unit that is failing now)
            "charts": [("Health score by unit, latest gas day", "bar", "asset_id", "health",
                        f"SELECT asset_id, ROUND(health_score,1) health FROM {m('gld_compression_health')} WHERE gas_day=(SELECT MAX(gas_day) FROM {m('gld_compression_health')}) ORDER BY asset_id", {"color": "lowbad"})],
            "table": (f"SELECT asset_id, ROUND(health_score,1) health_score, ROUND(rul_days_proxy,0) rul_days_proxy, ROUND(antisurge_margin_pct,1) antisurge_margin_pct, ROUND(production_at_risk_e3m3d,0) production_at_risk_e3m3d FROM {m('gld_compression_health')} WHERE gas_day=(SELECT MAX(gas_day) FROM {m('gld_compression_health')}) ORDER BY health_score",
                      ["asset_id", "health_score", "rul_days_proxy", "antisurge_margin_pct", "production_at_risk_e3m3d"], "Compressor fleet health"),
        },
        "custody": {
            "title": "Custody Transfer & Export", "group": "Value chain", "level": "Informative",
            "blurb": "Metering vs allocation reconciliation, unaccounted-for gas (UAF), energy content vs spec. The fiscal calc of record stays in the flow computer; the twin reconciles and flags.",
            "kpis": [("Max UAF (%)", f"SELECT ROUND(MAX(uaf_pct),2) v FROM {m('gld_custody_allocation')} WHERE gas_day=(SELECT MAX(gas_day) FROM {m('gld_custody_allocation')})"),
                     ("Metering stations", f"SELECT COUNT(DISTINCT asset_id) v FROM {m('gld_custody_allocation')}")],
            "charts": [("UAF trend (%)", "line", "gas_day", "uaf",
                        f"SELECT gas_day, ROUND(AVG(uaf_pct),2) uaf FROM {m('gld_custody_allocation')} GROUP BY gas_day ORDER BY gas_day")],
            "table": (f"SELECT asset_id, gas_day, ROUND(metered_e3m3,1) metered_e3m3, ROUND(uaf_e3m3,1) uaf_e3m3, ROUND(uaf_pct,2) uaf_pct FROM {m('gld_custody_allocation')} ORDER BY gas_day DESC NULLS LAST LIMIT 30",
                      ["asset_id", "gas_day", "metered_e3m3", "uaf_e3m3", "uaf_pct"], "Custody reconciliation"),
        },
        "integrity": {
            "title": "Integrity & RBI", "group": "Cross-cutting", "level": "Predictive",
            "blurb": "Risk-based inspection: corrosion rate, remaining life, inspection-due and the production it puts at risk if it slips.",
            "kpis": [("Equipment due < 30 days", f"SELECT COUNT(*) v FROM {m('gld_integrity_rbi')} WHERE inspection_due_days<30"),
                     ("High-risk equipment", f"SELECT COUNT(*) v FROM {m('gld_integrity_rbi')} WHERE rbi_status='high'"),
                     ("Min remaining life (yrs)", f"SELECT ROUND(MIN(remaining_life_years),1) v FROM {m('gld_integrity_rbi')}")],
            "charts": [("Remaining life by area (yrs)", "bar", "area_code", "rl",
                        f"SELECT area_code, ROUND(MIN(remaining_life_years),1) rl FROM {m('gld_integrity_rbi')} GROUP BY area_code ORDER BY area_code", {"color": "lowbad"})],
            "table": (f"SELECT asset_id, equipment_type, inspection_due_days, remaining_life_years, ROUND(production_at_risk_e3m3d,0) production_at_risk_e3m3d, rbi_status FROM {m('gld_integrity_rbi')} WHERE inspection_due_days<45 ORDER BY inspection_due_days LIMIT 500",
                      ["asset_id", "equipment_type", "inspection_due_days", "remaining_life_years", "production_at_risk_e3m3d", "rbi_status"], "Inspection due (next 45 days)"),
        },
        "safety": {
            "title": "Safety & Alarms", "group": "Cross-cutting", "level": "Informative",
            "blurb": "SIS/ESD and F&G status, alarm rationalization and flood analytics, trip and sequence-of-events.",
            "kpis": [("ESD trips (all time)", f"SELECT SUM(esd_trips) v FROM {m('gld_safety_alarms')}"),
                     ("Alarms (all time)", f"SELECT SUM(alarm_count) v FROM {m('gld_safety_alarms')}"),
                     ("Critical events", f"SELECT SUM(critical_events) v FROM {m('gld_safety_alarms')}")],
            "charts": [("ESD trips by area", "bar", "area_code", "trips",
                        f"SELECT area_code, SUM(esd_trips) trips FROM {m('gld_safety_alarms')} GROUP BY area_code HAVING SUM(esd_trips)>0 ORDER BY area_code")],
            "table": (f"SELECT event_time, event, asset_id, area_code, equipment_type, severity FROM {m('gld_safety_events')} ORDER BY event_time DESC NULLS LAST LIMIT 200",
                      ["event_time", "event", "asset_id", "area_code", "equipment_type", "severity"], "Recent safety events (click a row for the asset)"),
        },
        "emissions": {
            "title": "Emissions, Carbon & Safeguard", "group": "Cross-cutting", "level": "Informative",
            "blurb": "CH4/CO2/NOx by source, flaring, carbon intensity per 10^3 m^3 and its drivers. Scope 1/2 reporting ledger, and the Safeguard Mechanism position: Scope-1 CO₂ by facility and year against a best-practice baseline, with the ACCU cost to cover any shortfall.",
            "kpis": [("Avg carbon intensity (kg/10^3 m^3)", f"SELECT ROUND(AVG(ci),2) v FROM (SELECT gas_day, SUM(co2_kg)/NULLIF(MAX(field_e3m3),0) ci FROM {m('gld_emissions_ledger')} GROUP BY gas_day)"),
                     ("Total CH4 (kg)", f"SELECT ROUND(SUM(ch4_kg),0) v FROM {m('gld_emissions_ledger')}"),
                     ("Total CO2 (t)", f"SELECT ROUND(SUM(co2_kg)/1000,0) v FROM {m('gld_emissions_ledger')}"),
                     ("Scope-1 this year (t)", f"SELECT ROUND(SUM(scope1_t),0) v FROM {m('gld_carbon_compliance')} WHERE reporting_year=(SELECT MAX(reporting_year) FROM {m('gld_carbon_compliance')})"),
                     ("Position vs baseline (t)", f"SELECT ROUND(SUM(position_t),0) v FROM {m('gld_carbon_compliance')} WHERE reporting_year=(SELECT MAX(reporting_year) FROM {m('gld_carbon_compliance')})"),
                     ("ACCU cost ($)", f"SELECT ROUND(SUM(accu_cost_dollars),0) v FROM {m('gld_carbon_compliance')} WHERE reporting_year=(SELECT MAX(reporting_year) FROM {m('gld_carbon_compliance')})")],
            # field-level daily intensity (total CO2 / field production), stable, no divide-by-source spikes
            "charts": [("Carbon intensity trend (kg/10^3 m^3)", "line", "gas_day", "ci",
                        f"SELECT gas_day, ROUND(SUM(co2_kg)/NULLIF(MAX(field_e3m3),0),2) ci FROM {m('gld_emissions_ledger')} GROUP BY gas_day ORDER BY gas_day"),
                       ("CH4 by source (kg)", "bar", "source", "ch4",
                        f"SELECT CASE WHEN equipment_type='compressor' THEN 'Compression' WHEN equipment_type='teg_dehydration' THEN 'Dehydration' WHEN equipment_type='utilities' THEN 'Utilities' WHEN equipment_type='flare' THEN 'Flare' ELSE 'Other' END source, ROUND(SUM(ch4_kg),0) ch4 FROM {m('gld_emissions_ledger')} WHERE ch4_kg>0 GROUP BY 1 ORDER BY 1"),
                       ("Scope-1 by facility (t)", "bar", "facility", "scope1_t",
                        f"SELECT facility, ROUND(SUM(scope1_t),0) scope1_t FROM {m('gld_carbon_compliance')} WHERE reporting_year=(SELECT MAX(reporting_year) FROM {m('gld_carbon_compliance')}) GROUP BY facility ORDER BY facility")],
            "tables": [(f"SELECT gas_day, asset_id, ROUND(ch4_kg,1) ch4_kg, ROUND(co2_kg,0) co2_kg, ROUND(nox_kg,1) nox_kg FROM {m('gld_emissions_ledger')} ORDER BY gas_day DESC NULLS LAST, co2_kg DESC NULLS LAST LIMIT 30",
                      ["gas_day", "asset_id", "ch4_kg", "co2_kg", "nox_kg"], "Emissions ledger"),
                       (f"SELECT facility, reporting_year, scope1_t, baseline_t, position_t, accu_cost_dollars FROM {m('gld_carbon_compliance')} ORDER BY facility, reporting_year DESC NULLS LAST LIMIT 500",
                      ["facility", "reporting_year", "scope1_t", "baseline_t", "position_t", "accu_cost_dollars"], "Safeguard position by facility & year")],
        },
        "deferment": {
            "title": "Deferment & Losses", "group": "Cross-cutting", "level": "Informative",
            "blurb": "Lost production accounting with ISO-14224-style coded cause and dollar impact.",
            "kpis": [("Total deferment ($)", f"SELECT ROUND(SUM(deferred_dollars),0) v FROM {m('gld_deferment')}"),
                     ("Total deferred gas (10^3 m^3)", f"SELECT ROUND(SUM(deferred_e3m3),0) v FROM {m('gld_deferment')}"),
                     ("Distinct causes", f"SELECT COUNT(DISTINCT cause_code) v FROM {m('gld_deferment')}")],
            "charts": [("Deferred gas by cause (10³ m³)", "bar", "cause_desc", "dv",
                        f"SELECT cause_desc, ROUND(SUM(deferred_e3m3),0) dv FROM {m('gld_deferment')} GROUP BY cause_desc ORDER BY cause_desc"),
                       ("Deferment dollars by day", "line", "gas_day", "dollars",
                        f"SELECT gas_day, ROUND(SUM(deferred_dollars),0) dollars FROM {m('gld_deferment')} GROUP BY gas_day ORDER BY gas_day")],
            "table": (f"SELECT cause_desc, asset_id, ROUND(SUM(deferred_e3m3),1) deferred_e3m3, ROUND(SUM(deferred_dollars),0) deferred_dollars FROM {m('gld_deferment')} GROUP BY cause_desc, asset_id ORDER BY deferred_dollars DESC NULLS LAST LIMIT 30",
                      ["cause_desc", "asset_id", "deferred_e3m3", "deferred_dollars"], "Deferment by cause and asset"),
        },
        "economics": {
            "title": "Cost & Economics", "group": "Cross-cutting", "level": "Informative",
            "blurb": "Revenue, lifting cost, cost-to-produce per GJ, deferment economics and cost of carbon.",
            "kpis": [("Total revenue ($)", f"SELECT ROUND(SUM(revenue_dollars),0) v FROM {m('gld_economics')}"),
                     ("Deferment ($)", f"SELECT ROUND(SUM(deferred_dollars),0) v FROM {m('gld_economics')}"),
                     ("Carbon cost ($)", f"SELECT ROUND(SUM(carbon_cost_dollars),0) v FROM {m('gld_economics')}")],
            "charts": [("Revenue by day ($)", "line", "gas_day", "rev",
                        f"SELECT gas_day, ROUND(SUM(revenue_dollars),0) rev FROM {m('gld_economics')} GROUP BY gas_day ORDER BY gas_day")],
            "table": (f"SELECT asset_id, area_code, ROUND(SUM(revenue_dollars),0) revenue_dollars, ROUND(SUM(deferred_dollars),0) deferred_dollars, ROUND(SUM(carbon_cost_dollars),0) carbon_cost_dollars FROM {m('gld_economics')} GROUP BY asset_id, area_code ORDER BY revenue_dollars DESC NULLS LAST LIMIT 500",
                      ["asset_id", "area_code", "revenue_dollars", "deferred_dollars", "carbon_cost_dollars"], "Economics by asset"),
        },
        "maintenance": {
            "title": "Planning & Maintenance", "group": "Cross-cutting", "level": "Predictive",
            "blurb": "Planning, maintenance and reliability on one page: work orders (EAM), PM compliance, reliability backlog and failure history, with notifications and failures on the tiles above. Click a work order for its detail (with a link to the asset); click an asset id to open the asset.",
            "kpis": [("Open work orders", f"SELECT COUNT(*) v FROM {m('gld_workorders')} WHERE status IN ('open','in_progress')"),
                     ("P1 open", f"SELECT COUNT(*) v FROM {m('gld_workorders')} WHERE status IN ('open','in_progress') AND priority='P1'"),
                     ("Assets with failures", f"SELECT COUNT(*) v FROM {m('gld_maintenance_reliability')} WHERE failures>0"),
                     ("Total WO cost ($)", f"SELECT ROUND(SUM(wo_cost),0) v FROM {m('gld_maintenance_reliability')}"),
                     ("Open notifications", f"SELECT COUNT(*) v FROM {m('gld_notifications')} WHERE status='open'", "notifications"),
                     ("Recorded failures", f"SELECT COUNT(*) v FROM {m('gld_failure_history')}", "failures")],
            "charts": [("Open WOs by area", "bar", "area_code", "wos",
                        f"SELECT area_code, SUM(open_wos) wos FROM {m('gld_maintenance_reliability')} GROUP BY area_code ORDER BY area_code"),
                       ("Work orders by state", "bar", "state", "n",
                        f"SELECT CASE WHEN status IN ('open','in_progress') THEN 'Open' WHEN status='completed' THEN 'Completed' WHEN status='rejected' THEN 'Rejected' ELSE initcap(status) END state, COUNT(*) n FROM {m('gld_workorders')} GROUP BY 1 ORDER BY 1"),
                       ("Failures by failure mode", "bar", "iso14224_code", "n",
                        f"SELECT iso14224_code, COUNT(*) n FROM {m('gld_failure_history')} GROUP BY iso14224_code ORDER BY iso14224_code")],
            "tables": [(f"SELECT wo_id, asset_id, wo_type, assigned_to, target_completion, status, priority, created_date, ROUND(cost,0) cost FROM {m('gld_workorders')} ORDER BY created_date DESC NULLS LAST LIMIT 1000",
                        ["wo_id", "asset_id", "wo_type", "assigned_to", "target_completion", "status", "priority", "created_date", "cost"], "Work orders (EAM)"),
                       (f"SELECT asset_id, equipment_type, open_wos, completed_wos, failures, ROUND(wo_cost,0) wo_cost FROM {m('gld_maintenance_reliability')} WHERE open_wos>0 ORDER BY open_wos DESC NULLS LAST LIMIT 500",
                        ["asset_id", "equipment_type", "open_wos", "completed_wos", "failures", "wo_cost"], "Maintenance backlog")],
        },
        "workorders": {
            "title": "Work Orders & Reliability", "group": "Asset 360", "level": "Informative",
            "blurb": "EAM maintenance context (Maximo / SAP PM). Click a work order for its detail (with a link to the asset); click an asset id to open the asset. Notifications and failure history open from the tiles above or inside an asset.",
            "kpis": [("Open work orders", f"SELECT COUNT(*) v FROM {m('gld_workorders')} WHERE status IN ('open','in_progress')"),
                     ("P1 open", f"SELECT COUNT(*) v FROM {m('gld_workorders')} WHERE status IN ('open','in_progress') AND priority='P1'"),
                     ("Open notifications", f"SELECT COUNT(*) v FROM {m('gld_notifications')} WHERE status='open'", "notifications"),
                     ("Recorded failures", f"SELECT COUNT(*) v FROM {m('gld_failure_history')}", "failures")],
            "charts": [("Work orders by state", "bar", "state", "n",
                        f"SELECT CASE WHEN status IN ('open','in_progress') THEN 'Open' WHEN status='completed' THEN 'Completed' WHEN status='rejected' THEN 'Rejected' ELSE initcap(status) END state, COUNT(*) n FROM {m('gld_workorders')} GROUP BY 1 ORDER BY 1"),
                       ("Failures by failure mode", "bar", "iso14224_code", "n",
                        f"SELECT iso14224_code, COUNT(*) n FROM {m('gld_failure_history')} GROUP BY iso14224_code ORDER BY iso14224_code")],
            "table": (f"SELECT wo_id, asset_id, wo_type, assigned_to, target_completion, status, priority, created_date, ROUND(cost,0) cost FROM {m('gld_workorders')} ORDER BY created_date DESC NULLS LAST LIMIT 1000",
                      ["wo_id", "asset_id", "wo_type", "assigned_to", "target_completion", "status", "priority", "created_date", "cost"], "Work orders (EAM)"),
        },
        "lab": {
            "title": "Lab & Assay", "group": "Cross-cutting", "level": "Informative",
            "blurb": "Laboratory gas-composition assays (LIMS) across the value chain: wellhead gas sampled per well (quarterly), plus inlet-separator, treated-gas and sales-gas (custody) composition, so the full chain from well to custody is covered, not only wells. C₁/CO₂/N₂, H₂S, water and oil-in-water.",
            "kpis": [("Samples", f"SELECT COUNT(*) v FROM {m('gld_lab_assay')}"),
                     ("Value-chain points sampled", f"SELECT COUNT(DISTINCT sample_point) v FROM {m('gld_lab_assay')}"),
                     ("Avg C1 (mol%)", f"SELECT ROUND(AVG(c1_molpct),1) v FROM {m('gld_lab_assay')}"),
                     ("Max H2S (ppm)", f"SELECT ROUND(MAX(h2s_ppm),0) v FROM {m('gld_lab_assay')}")],
            "charts": [("CO₂ by sample point (Mol %)", "bar", "sample_point", "co2",
                        f"SELECT sample_point, ROUND(AVG(co2_molpct),2) co2 FROM {m('gld_lab_assay')} GROUP BY sample_point ORDER BY sample_point")],
            "table": (f"SELECT asset_id, sample_id, sample_point, well_id, sample_date, c1_molpct, co2_molpct, n2_molpct, h2s_ppm, water_ppm, oil_in_water_ppm FROM {m('gld_lab_assay')} ORDER BY sample_date DESC NULLS LAST LIMIT 500",
                      ["asset_id", "sample_id", "sample_point", "well_id", "sample_date", "c1_molpct", "co2_molpct", "n2_molpct", "h2s_ppm", "water_ppm", "oil_in_water_ppm"], "Recent lab assays"),
        },
        "commercial": {
            "title": "Commercial & Prices", "group": "Cross-cutting", "level": "Informative",
            "blurb": "Commercial context: the sales-gas price the operator sells at and the market carbon price; contractual nominations vs delivery; and the money: cost of generation, sale revenue and profit.",
            "kpis": [("Sale revenue (last month $)", f"SELECT ROUND(SUM(revenue_dollars),0) v FROM {m('gld_economics')} WHERE {_month()}=(SELECT MAX({_month()}) FROM {m('gld_economics')})"),
                     ("Cost of generation (last month $)", f"SELECT ROUND(SUM(lifting_cost_dollars),0) v FROM {m('gld_economics')} WHERE {_month()}=(SELECT MAX({_month()}) FROM {m('gld_economics')})"),
                     ("Profit (last month $)", f"SELECT ROUND(SUM(revenue_dollars-lifting_cost_dollars-carbon_cost_dollars),0) v FROM {m('gld_economics')} WHERE {_month()}=(SELECT MAX({_month()}) FROM {m('gld_economics')})"),
                     ("Carbon cost (last month $)", f"SELECT ROUND(SUM(carbon_cost_dollars),0) v FROM {m('gld_economics')} WHERE {_month()}=(SELECT MAX({_month()}) FROM {m('gld_economics')})"),
                     ("Water disposal (last month $)", f"SELECT ROUND(SUM(disposal_cost_dollars),0) v FROM {m('gld_produced_water')} WHERE {_month()}=(SELECT MAX({_month()}) FROM {m('gld_produced_water')})"),
                     ("Latest sales gas price ($/GJ)", f"SELECT ROUND(gas_price_gj,2) v FROM {m('gld_commercial')} ORDER BY gas_day DESC NULLS LAST LIMIT 1"),
                     ("Avg delivery vs nomination (%)", f"SELECT ROUND(AVG(nomination_delivery_pct),1) v FROM {m('gld_commercial')} WHERE {_since(m('gld_commercial'), 'gas_day', 61)}")],
            "charts": [("Monthly revenue, cost of generation & profit ($M)", "line", "month", "revenue",
                        f"SELECT left(gas_day::text, 7) || '-01' AS month, ROUND(SUM(revenue_dollars)/1e6,2) revenue, ROUND(SUM(lifting_cost_dollars)/1e6,2) cost, ROUND(SUM(revenue_dollars-lifting_cost_dollars-carbon_cost_dollars)/1e6,2) profit FROM {m('gld_economics')} GROUP BY 1 ORDER BY 1",
                        {"series": [{"key": "revenue", "label": "Sale revenue ($M)"}, {"key": "cost", "label": "Cost of generation ($M)"}, {"key": "profit", "label": "Profit ($M)"}]}),
                       ("Sales gas price trend ($/GJ)", "line", "gas_day", "gas_price_gj",
                        f"SELECT gas_day, ROUND(gas_price_gj,2) gas_price_gj FROM {m('gld_commercial')} ORDER BY gas_day")],
            "tables": [(f"SELECT c.gas_day, c.gas_price_gj, c.carbon_price_t, c.nominated_e3m3, c.actual_e3m3, c.nomination_delivery_pct, e.revenue_dollars, e.cost_of_generation_dollars, e.profit_dollars FROM {m('gld_commercial')} c LEFT JOIN (SELECT gas_day, ROUND(SUM(revenue_dollars),0) revenue_dollars, ROUND(SUM(lifting_cost_dollars),0) cost_of_generation_dollars, ROUND(SUM(revenue_dollars-lifting_cost_dollars-carbon_cost_dollars),0) profit_dollars FROM {m('gld_economics')} GROUP BY gas_day) e ON c.gas_day=e.gas_day::date ORDER BY c.gas_day DESC NULLS LAST LIMIT 500",
                        ["gas_day", "gas_price_gj", "carbon_price_t", "nominated_e3m3", "actual_e3m3", "nomination_delivery_pct", "revenue_dollars", "cost_of_generation_dollars", "profit_dollars"], "Commercial ledger (daily)"),
                       # the contract book: price, floor, nominations vs delivery and expiry, per buyer (the briefing's contract actions point here)
                       (f"SELECT customer_name, contract_id, contract_type, price_gj, mdq_tj, take_or_pay_pct, nominated_tj_d, delivered_tj_d, shortfall_days_30d, contract_end FROM {m('gld_customer_supply')} ORDER BY contract_end, customer_name",
                        ["customer_name", "contract_id", "contract_type", "price_gj", "mdq_tj", "take_or_pay_pct", "nominated_tj_d", "delivered_tj_d", "shortfall_days_30d", "contract_end"], "Contracts (last 30 gas days)")],
        },
        "weather": {
            "title": "Weather & Environment", "group": "Cross-cutting", "level": "Informative",
            "blurb": "Weather, environment and geospatial context: ambient temperature, wind, lightning and flood risk. Drives compression energy, dewpoint margin, flaring and HSE exposure.",
            "kpis": [("Latest ambient (C)", f"SELECT ROUND(ambient_c,1) v FROM {m('gld_weather')} ORDER BY obs_date DESC NULLS LAST LIMIT 1"),
                     ("Max wind (km/h)", f"SELECT ROUND(MAX(wind_kmh),0) v FROM {m('gld_weather')} WHERE {_since(m('gld_weather'), 'obs_date', 61)}"),
                     ("High flood-risk days", f"SELECT COUNT(*) v FROM {m('gld_weather')} WHERE flood_risk>0.3")],
            "charts": [("Ambient temperature trend (C)", "line", "obs_date", "ambient_c",
                        f"SELECT obs_date, ROUND(ambient_c,1) ambient_c FROM {m('gld_weather')} ORDER BY obs_date"),
                       ("Wind trend (km/h)", "line", "obs_date", "wind_kmh",
                        f"SELECT obs_date, ROUND(wind_kmh,1) wind_kmh FROM {m('gld_weather')} ORDER BY obs_date")],
            "table": (f"SELECT obs_date, ambient_c, wind_kmh, lightning_risk, flood_risk, lat, lon FROM {m('gld_weather')} ORDER BY obs_date DESC NULLS LAST LIMIT 40",
                      ["obs_date", "ambient_c", "wind_kmh", "lightning_risk", "flood_risk", "lat", "lon"], "Weather observations (daily)"),
        },
        "produced_water": {
            "title": "Produced Water Management", "group": "Cross-cutting", "level": "Informative",
            "blurb": "Produced-water volumes derived from each well's gas rate and PVT water-gas ratio, with reinjection split and disposal cost. CSG water is a leading cost and compliance driver.",
            "kpis": [("Produced water (m³/d)", f"SELECT ROUND(SUM(water_m3),0) v FROM {m('gld_produced_water')} WHERE gas_day=(SELECT MAX(gas_day) FROM {m('gld_produced_water')})"),
                     ("Reinjected (%)", f"SELECT ROUND(100*SUM(reinjected_m3)/NULLIF(SUM(water_m3),0),1) v FROM {m('gld_produced_water')} WHERE {_since(m('gld_produced_water'), 'gas_day', 61)}"),
                     ("Disposal cost 62d ($)", f"SELECT ROUND(SUM(disposal_cost_dollars),0) v FROM {m('gld_produced_water')} WHERE {_since(m('gld_produced_water'), 'gas_day', 61)}")],
            "charts": [("Produced water trend (m³/d)", "line", "gas_day", "water_m3",
                        f"SELECT gas_day, ROUND(SUM(water_m3),0) water_m3 FROM {m('gld_produced_water')} GROUP BY gas_day ORDER BY gas_day")],
            "table": (f"SELECT well_id, pad_area, completion_type, ROUND(AVG(water_m3),1) water_m3, ROUND(AVG(reinjected_m3),1) reinjected_m3, ROUND(SUM(disposal_cost_dollars),0) disposal_cost_dollars FROM {m('gld_produced_water')} GROUP BY well_id, pad_area, completion_type ORDER BY well_id LIMIT 500",
                      ["well_id", "pad_area", "completion_type", "water_m3", "reinjected_m3", "disposal_cost_dollars"], "Produced water by well"),
        },
        "energy": {
            "title": "Fuel-Gas & Energy Efficiency", "group": "Cross-cutting", "level": "Informative",
            "blurb": "Own-use fuel gas and flaring vs production. Cutting fuel-gas burn and flaring frees salable gas and lowers Scope 1.",
            "kpis": [("Own-use (%)", f"SELECT ROUND(AVG(own_use_pct),2) v FROM {m('gld_energy_efficiency')} WHERE {_since(m('gld_energy_efficiency'), 'gas_day', 61)}"),
                     ("Flaring (%)", f"SELECT ROUND(AVG(flaring_pct),3) v FROM {m('gld_energy_efficiency')} WHERE {_since(m('gld_energy_efficiency'), 'gas_day', 61)}"),
                     ("Fuel gas (10³ m³/d)", f"SELECT ROUND(AVG(fuel_gas_e3m3),0) v FROM {m('gld_energy_efficiency')} WHERE {_since(m('gld_energy_efficiency'), 'gas_day', 61)}")],
            "charts": [("Own-use fuel gas trend (%)", "line", "gas_day", "own_use_pct",
                        f"SELECT gas_day, own_use_pct FROM {m('gld_energy_efficiency')} ORDER BY gas_day"),
                       ("Flaring trend (%)", "line", "gas_day", "flaring_pct",
                        f"SELECT gas_day, flaring_pct FROM {m('gld_energy_efficiency')} ORDER BY gas_day")],
            "table": (f"SELECT gas_day, fuel_gas_e3m3, flare_e3m3, own_use_pct, flaring_pct FROM {m('gld_energy_efficiency')} ORDER BY gas_day DESC NULLS LAST LIMIT 40",
                      ["gas_day", "fuel_gas_e3m3", "flare_e3m3", "own_use_pct", "flaring_pct"], "Energy & fuel-gas ledger (daily)"),
        },
        "methane_ldar": {
            "title": "Methane LDAR", "group": "Cross-cutting", "level": "Predictive",
            "blurb": "Leak detection and repair: measured and factor-based fugitive methane by component, its CO₂-e, detector activations, and the annual value of the escaping gas the operator keeps and sells once the leak is repaired.",
            "kpis": [("CH₄ (kg/d)", f"SELECT ROUND(SUM(ch4_kg_d),0) v FROM {m('gld_methane_ldar')}"),
                     ("CO₂-e (t/yr)", f"SELECT ROUND(SUM(co2e_t_yr),0) v FROM {m('gld_methane_ldar')}"),
                     ("Components to investigate", f"SELECT COUNT(*) v FROM {m('gld_methane_ldar')} WHERE survey_status='investigate'")],
            "charts": [("CH₄ by area (kg/d)", "bar", "area_code", "ch4",
                        f"SELECT area_code, ROUND(SUM(ch4_kg_d),1) ch4 FROM {m('gld_methane_ldar')} GROUP BY area_code ORDER BY area_code")],
            "table": (f"SELECT asset_id, area_code, equipment_type, ch4_kg_d, co2e_t_yr, detections, product_value_dollars_yr, survey_status FROM {m('gld_methane_ldar')} WHERE ch4_kg_d>0 ORDER BY asset_id LIMIT 500",
                      ["asset_id", "area_code", "equipment_type", "ch4_kg_d", "co2e_t_yr", "detections", "product_value_dollars_yr", "survey_status"], "Methane by component (LDAR)"),
        },
        "margin": {
            "title": "Molecule-to-Market Margin", "group": "Cross-cutting", "level": "Informative",
            "blurb": "Netback margin by gas day (revenue less lifting and carbon cost) against the contracted take-or-pay commitment, so an OT-driven production dip is priced straight to the trading book.",
            "kpis": [("Netback 62d ($)", f"SELECT ROUND(SUM(netback_dollars),0) v FROM {m('gld_margin')} WHERE {_since(m('gld_margin'), 'gas_day', 61)}"),
                     ("Netback ($/GJ)", f"SELECT ROUND(AVG(netback_per_gj),2) v FROM {m('gld_margin')} WHERE {_since(m('gld_margin'), 'gas_day', 61)}"),
                     ("Take-or-pay shortfall days 62d", f"SELECT COUNT(*) v FROM {m('gld_margin')} WHERE take_or_pay_shortfall_e3m3>0 AND {_since(m('gld_margin'), 'gas_day', 61)}")],
            "charts": [("Netback trend ($/day)", "line", "gas_day", "netback_dollars",
                        f"SELECT gas_day, netback_dollars FROM {m('gld_margin')} ORDER BY gas_day"),
                       ("Netback per GJ trend ($)", "line", "gas_day", "netback_per_gj",
                        f"SELECT gas_day, netback_per_gj FROM {m('gld_margin')} ORDER BY gas_day")],
            "table": (f"SELECT gas_day, revenue, netback_dollars, netback_per_gj, deferred, committed_e3m3, take_or_pay_shortfall_e3m3 FROM {m('gld_margin')} ORDER BY gas_day DESC NULLS LAST LIMIT 40",
                      ["gas_day", "revenue", "netback_dollars", "netback_per_gj", "deferred", "committed_e3m3", "take_or_pay_shortfall_e3m3"], "Margin & commitment (daily)"),
        },
        "spares_risk": {
            "title": "Spares & Procurement Risk", "group": "Cross-cutting", "level": "Predictive",
            "blurb": "Predictive remaining life against the procurement lead time of the applicable spare, flagging every asset whose part will arrive after it fails.",
            "kpis": [("Assets at risk", f"SELECT COUNT(*) v FROM {m('gld_spares_risk')} WHERE exposure='at_risk'"),
                     ("On watch", f"SELECT COUNT(*) v FROM {m('gld_spares_risk')} WHERE exposure='watch'"),
                     ("Min RUL (days)", f"SELECT ROUND(MIN(rul_days),0) v FROM {m('gld_spares_risk')}")],
            "charts": [("Remaining life by asset (days)", "bar", "asset_id", "rul",
                        f"SELECT asset_id, ROUND(rul_days,0) rul FROM {m('gld_spares_risk')} ORDER BY asset_id", {"color": "lowbad"})],
            "table": (f"SELECT asset_id, area_code, equipment_type, part_name, procurement_type, rul_days, lead_time_days, on_hand, unit_cost, exposure FROM {m('gld_spares_risk')} WHERE exposure<>'ok' ORDER BY asset_id LIMIT 500",
                      ["asset_id", "area_code", "equipment_type", "part_name", "procurement_type", "rul_days", "lead_time_days", "on_hand", "unit_cost", "exposure"], "Spares exposure, what to procure (lead time beats RUL)"),
        },
        "capital_plan": {
            "title": "Capital & Decline-Aware Investment", "group": "Cross-cutting", "level": "Informative",
            "blurb": "Development and workover projects by area with capex, expected uplift, annual value and payback, timed against the field's production decline.",
            "kpis": [("Projects", f"SELECT COUNT(*) v FROM {m('gld_capital_plan')}"),
                     ("Capex ($)", f"SELECT ROUND(SUM(capex_dollars),0) v FROM {m('gld_capital_plan')}"),
                     ("Expected uplift (10³ m³/d)", f"SELECT ROUND(SUM(expected_uplift_e3m3d),0) v FROM {m('gld_capital_plan')}")],
            "charts": [("Capex by area ($)", "bar", "area_code", "capex",
                        f"SELECT area_code, ROUND(SUM(capex_dollars),0) capex FROM {m('gld_capital_plan')} GROUP BY area_code ORDER BY area_code")],
            "table": (f"SELECT project_id, area_code, project_type, capex_dollars, expected_uplift_e3m3d, annual_value_dollars, payback_years, planned_date, status FROM {m('gld_capital_plan')} ORDER BY area_code, planned_date LIMIT 40",
                      ["project_id", "area_code", "project_type", "capex_dollars", "expected_uplift_e3m3d", "annual_value_dollars", "payback_years", "planned_date", "status"], "Development & capital schedule"),
        },
        "asset_model": {
            "title": "Asset Explorer", "group": "Others", "level": "Governed",
            "blurb": "The semantic backbone: tag-to-asset resolution, the functional-location hierarchy, and cross-system id mapping. Every tag ID is the universal join key.",
            "kpis": [],
            "charts": [],  # item 55: "Tags by system" bar removed (no analytical value)
        },
    }


# ============================ Routes =========================================================
@app.get("/api/modules")
def modules():
    return JSONResponse(NAV)


@app.get("/api/genie-link")
def genie_link():
    """Deep link to the Genie space in Databricks One (the business-user interface), not the practitioner UI."""
    if not GENIE_SPACE:
        return JSONResponse({"url": None})
    def load():
        host, wsid = "", os.environ.get("DATABRICKS_WORKSPACE_ID", "")
        try:
            from databricks.sdk import WorkspaceClient
            w = WorkspaceClient()
            host = (w.config.host or "").rstrip("/")
            if not wsid:
                wsid = str(w.get_workspace_id())
        except Exception:
            pass
        if host and not host.startswith("http"):
            host = "https://" + host
        q_ = f"?o={wsid}" if wsid else ""
        return {"url": f"{host}/one/agents/{GENIE_SPACE}{q_}" if host else None, "space_id": GENIE_SPACE}
    return JSONResponse(cached("genie_link", 3600, load))


@app.get("/api/tag-lookup")
def tag_lookup(term: str = ""):
    """Resolve the contextualized tag end to end: tag -> asset -> functional location -> latest live
    value -> cross-system ids (historian / EAM / OSDU / P&ID). Demonstrates the one universal join key."""
    ql = (term or "").strip().lower()
    if not ql:
        return JSONResponse({"rows": []})
    sql = (
        "SELECT tag_id, asset_id, area_code, system_code, class_code, signal, unit, "
        "description, measured_or_calculated, cadence, "
        "ROUND(eng_range_min,2) eng_range_min, ROUND(eng_range_max,2) eng_range_max, "
        "equipment_type, criticality, manufacturer, install_year, fl_name, fl_level, "
        "ROUND(latest_value,3) latest_value, latest_quality, latest_ts, "
        "historian_tag, eam_fl, osdu_ref, doc_entity, is_stale "
        f"FROM {T('gld_tag_context')} t "
        "WHERE lower(t.tag_id) LIKE %(p)s OR lower(t.asset_id) LIKE %(p)s "
        "OR lower(t.signal) LIKE %(p)s OR lower(t.description) LIKE %(p)s "
        "ORDER BY t.tag_id"  # item 52: no 30-tag cap: show every match
    )
    try:
        rows = q(sql, {"p": f"%{ql}%"})
    except Exception as e:
        return JSONResponse({"rows": [], "error": str(e)[:200]})
    return JSONResponse({"rows": rows})


# ---- Physical-location tree (item 54): area -> asset -> tag, lazy-loaded per level -----------
@app.get("/api/tree")
def tree_areas():
    """Top of the physical-location tree: every area (well pad / CPF train / gathering hub /
    custody station) with its proper name and asset / tag counts. Lazy, children load on expand."""
    def load():
        return q(f"SELECT area_code, n_assets, n_tags, name FROM {T('gld_sites')} ORDER BY area_code")
    return JSONResponse({"areas": cached("tree_areas", 300, load)})


@app.get("/api/tree/{area}")
def tree_assets(area: str):
    """Assets at one physical location, with equipment type, criticality, health colour and tag count."""
    sql = (
        "SELECT a.asset_id, a.equipment_type, a.criticality, a.system_code, a.n_tags, s.pin_colour "
        f"FROM {T('gld_asset_master')} a "
        f"LEFT JOIN {T('gld_asset_state')} s ON s.asset_id = a.asset_id "
        "WHERE a.area_code = %(a)s "
        "ORDER BY a.system_code, a.asset_id"
    )
    try:
        rows = q(sql, {"a": area})
    except Exception as e:
        return JSONResponse({"assets": [], "error": str(e)[:200]})
    return JSONResponse({"assets": rows})


@app.get("/api/tree-asset/{asset_id}")
def tree_tags(asset_id: str):
    """Tags on one asset: the leaf of the physical-location tree. Each resolves via /api/tag-lookup."""
    sql = (
        "SELECT tag_id, signal, unit, ROUND(latest_value,2) latest_value, latest_quality "
        f"FROM {T('gld_tag_context')} "
        "WHERE asset_id = %(a)s ORDER BY tag_id"
    )
    try:
        rows = q(sql, {"a": asset_id})
    except Exception as e:
        return JSONResponse({"tags": [], "error": str(e)[:200]})
    return JSONResponse({"tags": rows})


@app.get("/api/overview")
def overview():
    def load():
        res = qmany([
            f"SELECT actual_e3m3, nominated_e3m3, gap_to_nomination_e3m3 FROM {T('gld_field_production')} ORDER BY gas_day DESC NULLS LAST LIMIT 1",          # 0
            f"SELECT ROUND(SUM(production_at_risk_e3m3d),0) v FROM {T('gld_constraint_ranking')}",                                                   # 1
            f"SELECT ROUND(SUM(deferred_dollars),0) v FROM {T('gld_deferment')} WHERE {_since(T('gld_field_production'), 'gas_day', 61)}",  # 2
            f"SELECT ROUND(SUM(revenue_dollars),0) v FROM {T('gld_economics')}",                                                                     # 3
            f"SELECT ROUND(AVG(ci),2) v FROM (SELECT gas_day, SUM(co2_kg)/NULLIF(MAX(field_e3m3),0) ci FROM {T('gld_emissions_ledger')} GROUP BY gas_day)",  # 4 field-level carbon intensity (consistent with Emissions tab)
            f"SELECT SUM(is_red) v FROM {T('gld_asset_state')}",                                                                                     # 5
            f"SELECT SUM(esd_trips) v FROM {T('gld_safety_alarms')} WHERE {_since(T('gld_field_production'), 'gas_day', 61)}",  # 6
            f"SELECT SUM(open_wos) v FROM {T('gld_maintenance_reliability')}",                                                                       # 7
            f"SELECT gas_day, actual_e3m3, nominated_e3m3 FROM {T('gld_field_production')} ORDER BY gas_day",                                        # 8
        ])
        row = res[0][0] if res[0] else {}
        kpis = [
            {"label": "Production (10^3 m^3/d)", "value": round(row.get("actual_e3m3") or 0, 1), "link": "reservoir_wells"},
            {"label": "Gap to nomination", "value": round(row.get("gap_to_nomination_e3m3") or 0, 1), "link": "reservoir_wells"},
            {"label": "Production at risk (10^3 m^3/d)", "value": _sv(res[1]), "link": "compression"},
            {"label": "Deferment ($)", "value": _sv(res[2]), "link": "deferment"},
            {"label": "Revenue ($)", "value": _sv(res[3]), "link": "economics"},
            {"label": "Carbon intensity (kg/10^3 m^3)", "value": _sv(res[4]), "link": "emissions"},
            {"label": "Red assets", "value": _sv(res[5]), "link": "navigator"},
            {"label": "ESD trips", "value": _sv(res[6]), "link": "safety"},
            {"label": "Open work orders", "value": _sv(res[7]), "link": "maintenance"},
        ]
        return {"kpis": kpis, "series": res[8]}
    return JSONResponse(cached("overview", 120, load))


@app.get("/api/constraints")
def constraints():
    return JSONResponse(cached("constraints", 120, lambda: q(
        # each constraint with the well it is (for well constraints) and its site's name, so a screen opened from
        # the Command Centre can point at exactly the rows, bars and basins behind a red or amber stop
        f"""SELECT c.rank, c.constraint_type, c.asset_id, COALESCE(c.area_code, am.area_code) area_code, ROUND(c.health_score,1) health_score,
                   ROUND(c.production_at_risk_e3m3d,0) production_at_risk_e3m3d, l.well_id, st.name site_name
            FROM {T('gld_constraint_ranking')} c
            LEFT JOIN {T('gld_pred_liquid_loading')} l ON l.asset_id = c.asset_id
            LEFT JOIN {T('gld_asset_master')} am ON am.asset_id = c.asset_id
            LEFT JOIN {T('gld_sites')} st ON st.area_code = COALESCE(c.area_code, am.area_code)
            ORDER BY c.rank LIMIT 25""")))


@app.get("/api/module/{key}")
def module(key: str):
    spec = _specs().get(key)
    if not spec:
        return JSONResponse({"error": "unknown module"}, status_code=404)

    def load():
        out = {"key": key, "title": spec["title"], "group": spec["group"], "level": spec["level"], "blurb": spec["blurb"]}
        if spec["group"] in ("Cross-cutting", "Commercial"):
            out["period"] = _data_period()   # these tabs sum over the full dataset; state the period (item 23)
        kpi_specs = spec.get("kpis", [])
        chart_specs = spec.get("charts", [])
        tbls = spec.get("tables") if spec.get("tables") else ([spec["table"]] if spec.get("table") else [])
        # run every query this screen needs concurrently (KPIs + charts + tables), then slot by index
        res = qmany([s[1] for s in kpi_specs] + [c[4] for c in chart_specs] + [t[0] for t in tbls])
        i = 0
        kout = []
        for j, s in enumerate(kpi_specs):
            k = {"label": s[0], "value": _sv(res[i + j])}
            if len(s) > 2 and s[2]:        # a KPI tile that opens a full list drawer (e.g. notifications, failures)
                k["list"] = s[2]
            kout.append(k)
        out["kpis"] = kout
        i += len(kpi_specs)
        charts = []
        for j, c in enumerate(chart_specs):
            row = {"title": c[0], "type": c[1], "x": c[2], "y": c[3], "rows": res[i + j]}
            if len(c) > 5 and c[5]:
                row["opts"] = c[5]
            charts.append(row)
        out["charts"] = charts
        i += len(chart_specs)
        built = [{"rows": res[i + j], "columns": _cols(t[1]), "title": t[2] if len(t) > 2 else "Detail"} for j, t in enumerate(tbls)]
        if spec.get("tables"):
            out["tables"] = built
        elif spec.get("table"):
            out["table"] = built[0]
        out["map"] = bool(spec.get("map"))
        out["rounds"] = bool(spec.get("rounds"))
        return out
    return JSONResponse(cached(f"module:{key}", 150, load))


@app.get("/api/scenes")
def scenes():
    return JSONResponse(cached("scenes", 200, lambda: q(
        # each site with its proper name (e.g. "Well pad BAR029 (Surat Basin)"), sorted along the gas path:
        # well pads, offshore platforms, gathering hubs, offshore manifolds, gas plants, then custody; by site code
        f"""SELECT a.scene_id, COUNT(*) assets,
                   SUM(a.is_red) red, SUM(a.is_amber) amber, SUM(CASE WHEN a.pin_colour='green' THEN 1 ELSE 0 END) green,
                   CASE WHEN COUNT(DISTINCT a.area_code) > 1 THEN 'Custody metering stations' ELSE MAX(st.name) END AS name,
                   CASE WHEN a.scene_id LIKE 'scene_BAR%' THEN 'Well pads' WHEN a.scene_id LIKE 'scene_PLT%' THEN 'Offshore platforms'
                        WHEN a.scene_id LIKE 'scene_GTH%' THEN 'Gathering hubs' WHEN a.scene_id LIKE 'scene_MNF%' THEN 'Offshore manifolds'
                        WHEN a.scene_id LIKE 'scene_CPF%' THEN 'Gas plants' ELSE 'Custody' END AS kind,
                   CASE WHEN a.scene_id LIKE 'scene_BAR%' THEN 1 WHEN a.scene_id LIKE 'scene_PLT%' THEN 2
                        WHEN a.scene_id LIKE 'scene_GTH%' THEN 3 WHEN a.scene_id LIKE 'scene_MNF%' THEN 4
                        WHEN a.scene_id LIKE 'scene_CPF%' THEN 5 ELSE 6 END AS kind_order
            FROM {T('gld_asset_state')} a LEFT JOIN {T('gld_sites')} st ON st.area_code = a.area_code
            WHERE a.scene_id IS NOT NULL
            GROUP BY a.scene_id ORDER BY kind_order, a.scene_id""")))


@app.get("/api/scene/{scene_id}")
def scene(scene_id: str):
    return JSONResponse(q(
        f"""SELECT asset_id, equipment_type, area_code, pin_colour, criticality,
                   ROUND(health_score,1) health_score, ROUND(production_at_risk_e3m3d,0) production_at_risk_e3m3d,
                   ROUND(latest_vibration,2) latest_vibration, hotspot_x, hotspot_y
            FROM {T('gld_asset_state')} WHERE scene_id = %(s)s ORDER BY is_red DESC NULLS LAST, is_amber DESC NULLS LAST, asset_id""",
        {"s": scene_id}))


def _resolve_asset(aid: str) -> str:
    """Row clicks can arrive as an asset_id (facilities) or a well_id (wells tables). Resolve a well_id
    to its wellhead asset so the detail drawer and sparklines work from either. One cached query."""
    def _lookup():
        try:
            r = q(f"""SELECT asset_id FROM (
                        SELECT asset_id, 1 pr FROM {T('gld_asset_state')} WHERE asset_id = %(a)s
                        UNION ALL
                        SELECT asset_id, 2 pr FROM {T('gld_well_scorecard')} WHERE well_id = %(a)s
                      ) ORDER BY pr LIMIT 1""", {"a": aid})
            return r[0]["asset_id"] if r else aid
        except Exception:
            return aid
    return cached(f"resolve:{aid}", 900, _lookup)


@app.get("/api/asset/{asset_id}")
def asset(asset_id: str):
    asset_id = _resolve_asset(asset_id)

    def load():
        state, health, reco, defer, tags, wos = qmany([
            (f"SELECT * FROM {T('gld_asset_state')} WHERE asset_id = %(a)s", {"a": asset_id}),
            (f"SELECT gas_day, ROUND(vibration_overall,2) vibration_overall, ROUND(polytropic_eff,1) polytropic_eff, ROUND(health_score,1) health_score, ROUND(rul_days_proxy,0) rul_days_proxy FROM {T('gld_compression_health')} WHERE asset_id = %(a)s ORDER BY gas_day", {"a": asset_id}),
            (f"SELECT recommendation, maturity_level, evidence, production_at_risk_e3m3d FROM {T('gld_recommendations')} WHERE asset_id = %(a)s", {"a": asset_id}),
            (f"SELECT gas_day, cause_desc, ROUND(deferred_e3m3,1) deferred_e3m3, ROUND(deferred_dollars,0) deferred_dollars FROM {T('gld_deferment')} WHERE asset_id = %(a)s ORDER BY gas_day DESC NULLS LAST LIMIT 20", {"a": asset_id}),
            (f"""SELECT tag_id, signal, unit, description, ROUND(latest_value,2) latest_value, latest_quality
                 FROM {T('gld_tag_context')} WHERE asset_id = %(a)s ORDER BY tag_id""", {"a": asset_id}),
            (f"SELECT wo_id, wo_type, priority, status, ROUND(cost,0) cost, created_date FROM {T('gld_workorders')} WHERE asset_id = %(a)s ORDER BY created_date DESC NULLS LAST LIMIT 15", {"a": asset_id}),
        ])
        return {"state": state[0] if state else {}, "health": health, "recommendations": reco, "deferment": defer, "tags": tags, "workorders": wos}
    return JSONResponse(cached(f"asset:{asset_id}", 180, load))


@app.get("/api/asset_context/{asset_id}")
def asset_context(asset_id: str):
    """Context that lives as sublinks inside the asset detail: lab assays, notifications, failures (item 29/32)."""
    asset_id = _resolve_asset(asset_id)

    def load():
        lab, notifs, fails, spares = qmany([
            (f"SELECT sample_id, sample_point, sample_date, c1_molpct, co2_molpct, h2s_ppm FROM {T('gld_lab_assay')} WHERE asset_id = %(a)s ORDER BY sample_date DESC NULLS LAST LIMIT 10", {"a": asset_id}),
            (f"SELECT notif_id, notif_type, priority, status, raised_date FROM {T('gld_notifications')} WHERE asset_id = %(a)s ORDER BY raised_date DESC NULLS LAST LIMIT 10", {"a": asset_id}),
            (f"SELECT failure_id, failure_date, iso14224_code, failure_mode, downtime_hours FROM {T('gld_failure_history')} WHERE asset_id = %(a)s ORDER BY failure_date DESC NULLS LAST LIMIT 10", {"a": asset_id}),
            (f"SELECT part_name, procurement_type, lead_time_days, on_hand, unit_cost, exposure FROM {T('gld_spares_risk')} WHERE asset_id = %(a)s", {"a": asset_id}),
        ])
        return {"lab": lab, "notifications": notifs, "failures": fails, "spares": spares}
    return JSONResponse(cached(f"asset_ctx:{asset_id}", 180, load))


@app.get("/api/workorder/{wo_id}")
def workorder(wo_id: str):
    """Work-order detail (item 29): the WO itself, plus any notifications on its asset, with a link to the asset."""
    def load():
        wo = q(f"SELECT * FROM {T('gld_workorders')} WHERE wo_id = %(w)s", {"w": wo_id})
        row = wo[0] if wo else {}
        aid = row.get("asset_id")
        notifs = q(f"SELECT notif_id, notif_type, priority, status, raised_date FROM {T('gld_notifications')} WHERE asset_id = %(a)s ORDER BY raised_date DESC NULLS LAST LIMIT 10", {"a": aid}) if aid else []
        return {"wo": row, "notifications": notifs}
    return JSONResponse(cached(f"wo:{wo_id}", 150, load))


@app.get("/api/list/{kind}")
def entity_list(kind: str):
    """Full notification / failure lists, opened from the KPI tiles (item 29) rather than shown by default."""
    if kind == "notifications":
        rows = q(f"SELECT notif_id, asset_id, area_code, notif_type, priority, status, raised_date FROM {T('gld_notifications')} ORDER BY raised_date DESC NULLS LAST LIMIT 1000")
        return JSONResponse({"title": "Notifications", "columns": _cols(["notif_id", "asset_id", "area_code", "notif_type", "priority", "status", "raised_date"]), "rows": rows})
    if kind == "failures":
        rows = q(f"SELECT failure_id, asset_id, area_code, equipment_type, failure_date, iso14224_code, failure_mode, downtime_hours FROM {T('gld_failure_history')} ORDER BY failure_date DESC NULLS LAST LIMIT 1000")
        return JSONResponse({"title": "Failure history (ISO 14224)", "columns": _cols(["failure_id", "asset_id", "area_code", "equipment_type", "failure_date", "iso14224_code", "failure_mode", "downtime_hours"]), "rows": rows})
    return JSONResponse({"error": "unknown list"}, status_code=404)


@app.get("/api/asset_series/{asset_id}")
def asset_series(asset_id: str):
    """Per-asset telemetry over time, for the asset detail drawer's sparklines. Works for any asset type:
    generic process signals come from the per-asset signal series mart (5-gas-day averages); compressor
    vibration/health are unioned from the compression-health mart (not carried on the process stream)."""
    asset_id = _resolve_asset(asset_id)

    def load():
        rows, vib = qmany([
            (f"""SELECT signal, unit, gas_day, value FROM {T('gld_asset_signal_series')}
                 WHERE asset_id = %(a)s ORDER BY signal, gas_day""", {"a": asset_id}),
            (f"""SELECT gas_day, ROUND(vibration_overall,2) vibration_overall, ROUND(health_score,1) health_score
                 FROM {T('gld_compression_health')} WHERE asset_id = %(a)s ORDER BY gas_day""", {"a": asset_id}),
        ])
        series: dict = {}
        for r in rows:
            s = series.setdefault(r["signal"], {"signal": r["signal"], "unit": r.get("unit") or "", "points": []})
            s["points"].append({"gas_day": r["gas_day"], "value": r["value"]})
        if vib:
            series["vibration_overall"] = {"signal": "vibration_overall", "unit": "mm/s",
                                           "points": [{"gas_day": r["gas_day"], "value": r["vibration_overall"]} for r in vib if r["vibration_overall"] is not None]}
            series["health_score"] = {"signal": "health_score", "unit": "",
                                      "points": [{"gas_day": r["gas_day"], "value": r["health_score"]} for r in vib if r["health_score"] is not None]}
        # vibration first (the hero signal), then the rest alphabetically; cap to keep the drawer tidy
        order = sorted(series.keys(), key=lambda k: (k != "vibration_overall", k))
        # downsample each series to ~160 points: a 240px sparkline needs no more, and it cuts the
        # drawer payload ~4x (730 daily points -> 160), so row-clicks open faster.
        def _ds(pts, n=160):
            if len(pts) <= n:
                return pts
            step = len(pts) / float(n)
            out = [pts[int(i * step)] for i in range(n)]
            if out[-1] is not pts[-1]:
                out[-1] = pts[-1]   # always keep the latest point (the acute end of the story)
            return out
        result = []
        for k in order[:9]:
            if series[k]["points"]:
                series[k]["points"] = _ds(series[k]["points"])
                result.append(series[k])
        return {"asset_id": asset_id, "series": result}
    return JSONResponse(cached(f"asset_series:{asset_id}", 180, load))


@app.get("/api/weather_map")
def weather_map():
    """Per-location weather (item 39): one point per area with its temperature range, wind and risks."""
    def load():
        rows, names = qmany([
            f"""SELECT area_code, ROUND(AVG(lat),5) lat, ROUND(AVG(lon),5) lon,
                       ROUND(MIN(ambient_c),1) tmin, ROUND(MAX(ambient_c),1) tmax, ROUND(AVG(ambient_c),1) tavg,
                       ROUND(MAX(wind_kmh),0) wind_max, ROUND(MAX(lightning_risk),2) ltn_max, ROUND(MAX(flood_risk),2) fld_max
                FROM {T('gld_weather')} WHERE lat IS NOT NULL GROUP BY area_code ORDER BY area_code""",
            f"SELECT area_code, name FROM {T('gld_sites')}",
        ])
        nm = {r["area_code"]: r["name"] for r in names}
        for r in rows:
            r["name"] = nm.get(r["area_code"]) or r["area_code"]   # site name, e.g. "Well pad BAR045 (Bowen Basin)"
        return rows
    return JSONResponse(cached("weather_map", 300, load))


@app.get("/api/weather_site/{area}")
def weather_site(area: str):
    """One location's weather history (item 39): temperature, wind, lightning and flood over time."""
    return JSONResponse(q(f"""SELECT obs_date, ambient_c, wind_kmh, lightning_risk, flood_risk
                              FROM {T('gld_weather')} WHERE area_code = %(a)s ORDER BY obs_date""", {"a": area}))


@app.get("/api/map")
def field_map():
    def load():
        facs, names = qmany([
            f"""SELECT area_code, ROUND(AVG(gps_lat),5) lat, ROUND(AVG(gps_lon),5) lon,
                       SUM(is_red) red, SUM(is_amber) amber, COUNT(*) assets,
                       ROUND(SUM(production_at_risk_e3m3d),0) risk
                FROM {T('gld_asset_state')} WHERE gps_lat IS NOT NULL GROUP BY area_code""",
            f"SELECT area_code, name FROM {T('gld_sites')}",
        ])
        nm = {r["area_code"]: r["name"] for r in names}
        for f in facs:
            f["name"] = nm.get(f["area_code"]) or f["area_code"]
            f["pin_colour"] = "red" if (f.get("red") or 0) > 0 else ("amber" if (f.get("amber") or 0) > 0 else "green")
            f["risk_dollars"] = f.get("risk") or 0
        return {"facilities": facs}
    return JSONResponse(cached("map", 200, load))


@app.get("/api/recommendations")
def recommendations():
    return JSONResponse(_with_open_wos(_recommendations()))


def _recommendations():
    try:
        return q("""SELECT rec_id, asset_id, constraint_type, production_at_risk_e3m3d, recommendation,
                           maturity_level, evidence, status, note, decided_by FROM gas_twin.recommendation_decisions
                    ORDER BY production_at_risk_e3m3d DESC NULLS LAST""")
    except Exception:
        pass
    # fallback to the synced gold mart (read-only) if the write-back table is not seeded yet
    return q(f"SELECT rec_id, asset_id, constraint_type, ROUND(production_at_risk_e3m3d,0) production_at_risk_e3m3d, recommendation, maturity_level, evidence, 'proposed' status FROM {T('gld_recommendations')} ORDER BY production_at_risk_e3m3d DESC NULLS LAST")


def _with_open_wos(recs):
    """Attach each recommended asset's open work orders (P1 first), so the evidence links to real WOs."""
    ids = sorted({r["asset_id"] for r in recs if r.get("asset_id")})
    if not ids:
        return recs
    try:
        lst = ", ".join("'" + i.replace("'", "") + "'" for i in ids)
        rows = q(f"SELECT asset_id, wo_id, priority FROM {T('gld_workorders')} WHERE status IN ('open','in_progress') AND asset_id IN ({lst}) ORDER BY asset_id, priority, created_date DESC NULLS LAST")
    except Exception:
        return recs
    by = {}
    for w in rows:
        by.setdefault(w["asset_id"], []).append({"wo_id": w["wo_id"], "priority": w["priority"]})
    for r in recs:
        r["open_wos"] = by.get(r.get("asset_id"), [])
    return recs


@app.post("/api/recommendation/{rec_id}")
async def decide(rec_id: str, request: Request):
    body = await request.json()
    status = body.get("status", "proposed")
    note = body.get("note", "")
    who = request.headers.get("X-Forwarded-Email") or request.headers.get("X-Forwarded-User") or "app-user"
    try:
        q("""UPDATE gas_twin.recommendation_decisions SET status=%s, note=%s, decided_by=%s, decided_at=now() WHERE rec_id=%s""",
          (status, note, who, rec_id))
        return JSONResponse({"ok": True, "decided_by": who})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)[:200]}, status_code=500)


# ============================ User settings (Lakebase gas_twin.settings) ========================
# One row per user per preference. The Databricks Apps proxy forwards the signed-in user's e-mail.
SETTING_KEYS = {"commercial_briefing", "last_tabs", "site_list_folded"}


def _who(request: Request) -> str:
    return request.headers.get("X-Forwarded-Email") or request.headers.get("X-Forwarded-User") or "app-user"


@app.get("/api/settings")
def get_settings(request: Request):
    who = _who(request)
    try:
        rows = q("SELECT setting_key, setting_value FROM gas_twin.settings WHERE user_email=%s", (who,))
    except Exception as e:   # the table is missing (an older deploy): the app works, preferences just are not kept
        return JSONResponse({"user": who, "settings": {}, "stored": False, "error": str(e)[:160]})
    return JSONResponse({"user": who, "settings": {r["setting_key"]: r["setting_value"] for r in rows}, "stored": True})


@app.put("/api/settings/{key}")
async def put_setting(key: str, request: Request):
    if key not in SETTING_KEYS:
        return JSONResponse({"ok": False, "error": "unknown setting"}, status_code=400)
    who, val = _who(request), (await request.json()).get("value")
    try:
        if val is None:   # clearing a preference removes its row
            q("DELETE FROM gas_twin.settings WHERE user_email=%s AND setting_key=%s", (who, key))
        else:
            q("""INSERT INTO gas_twin.settings (user_email, setting_key, setting_value, updated_at) VALUES (%s, %s, %s::jsonb, now())
                 ON CONFLICT (user_email, setting_key) DO UPDATE SET setting_value=EXCLUDED.setting_value, updated_at=now()""",
              (who, key, json.dumps(val)))
        return JSONResponse({"ok": True})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)[:200]}, status_code=500)


# ============================ Commercial briefing (Genie, prepared ahead) =======================
# The top three commercial actions come from gld_commercial_actions (ranked across the Commercial & Finance
# reports by AUD at stake per year). Genie writes the summary through the conversation API in a background
# thread, at start-up and again whenever the ranked actions change; Lakebase caches it (gas_twin.ai_briefings)
# so the briefing is ready before anyone opens Commercial & Finance. Until Genie's summary is in, a summary
# worked out from the same actions is shown.
BRIEF_KEY = "commercial_top_actions"
BRIEF_Q = ("What are the top 3 commercial actions we should take now, and why? Use gld_commercial_actions, ranks 1 to 3. "
           "Answer for an executive in plain prose of no more than three sentences, with no list, no table and no mention "
           "of tables, columns or ranks, and money rounded the way an executive says it (for example $202M a year): say what "
           "the three actions are worth together, which one matters most, and why.")
BRIEF_RECHECK_S = 600          # look for changed actions at most every 10 minutes (only when the app is in use)
BRIEF_RETRY_S = 1800           # after a Genie error, wait 30 minutes before asking again
_brief = {"checked": 0.0, "running": False}
_brief_lock = threading.Lock()


def _money(v):
    v = float(v or 0); a = abs(v)
    if a >= 1e9: return f"${v / 1e9:.1f}B"
    if a >= 1e8: return f"${v / 1e6:.0f}M"
    if a >= 1e6: return f"${v / 1e6:.1f}M"
    if a >= 1e3: return f"${v / 1e3:.0f}k"
    return f"${v:.0f}"


def _actions():
    return q(f"SELECT rank, lever, action, detail, value_dollars_yr, value_basis, module_key, module_label, focus_ids, focus_kpis, data_as_of "
             f"FROM {T('gld_commercial_actions')} ORDER BY rank")


def _actions_hash(acts):
    top = [[a.get("lever"), a.get("action"), round(float(a.get("value_dollars_yr") or 0), -3)] for a in acts[:3]]
    return hashlib.md5(json.dumps([BRIEF_Q, top]).encode()).hexdigest()   # a new question also earns a fresh answer


def _brief_row():
    r = q("SELECT briefing_key, data_as_of, content_hash, summary, source, generated_at, last_attempt_at, last_error FROM gas_twin.ai_briefings WHERE briefing_key=%s", (BRIEF_KEY,))
    return r[0] if r else None


def _ask_genie(question, timeout=300):
    """One Genie conversation: start it, poll until it completes, return (conversation id, the text answer)."""
    api, base = _client().api_client.do, f"/api/2.0/genie/spaces/{GENIE_SPACE}"
    r = api("POST", f"{base}/start-conversation", body={"content": question})
    cid, mid, t0 = r["conversation_id"], r["message_id"], _time.time()
    while _time.time() - t0 < timeout:
        _time.sleep(4)
        m = api("GET", f"{base}/conversations/{cid}/messages/{mid}")
        st = m.get("status")
        if st == "COMPLETED":
            texts = [a["text"]["content"] for a in (m.get("attachments") or []) if (a.get("text") or {}).get("content")]
            if not texts:
                raise RuntimeError("Genie answered without a text summary")
            return cid, "\n".join(texts)
        if st in ("FAILED", "CANCELLED", "QUERY_RESULT_EXPIRED"):
            raise RuntimeError(f"Genie {st}: {str(m.get('error') or '')[:200]}")
    raise TimeoutError("Genie did not answer within 5 minutes")


def _plain(text):
    """Genie writes light markdown; the briefing shows plain prose."""
    s = re.sub(r"\*\*|__|`", "", text or "")
    s = re.sub(r"^\s*(?:[-*\u2022]|\d+[.)])\s+", "", s, flags=re.M)
    s = re.sub(r"\s+", " ", s).strip().replace("10^3 m^3", "10³ m³").replace("m^3", "m³")
    s = re.sub(r"(\d) (?:x )?10³", r"\1 ×10³", s)   # "734 10³ m³/d" -> "734 ×10³ m³/d"
    return s[:900]


def _save_brief(**cols):
    keys = ["briefing_key"] + list(cols)
    q(f"""INSERT INTO gas_twin.ai_briefings ({", ".join(keys)}) VALUES ({", ".join(["%s"] * len(keys))})
          ON CONFLICT (briefing_key) DO UPDATE SET {", ".join(f"{k}=EXCLUDED.{k}" for k in cols)}""",
      [BRIEF_KEY] + list(cols.values()))


def _refresh_brief():
    """Ask Genie for a fresh summary if the ranked actions changed since the cached one (runs in a thread)."""
    with _brief_lock:
        if _brief["running"]:
            return
        _brief["running"], _brief["checked"] = True, _time.time()
    try:
        acts = _actions()
        if not GENIE_SPACE or not acts:
            return
        h, row = _actions_hash(acts), _brief_row()
        if row and row.get("content_hash") == h and row.get("summary"):
            return                                   # Genie's summary still describes these actions
        if row and row.get("last_error") and row.get("last_attempt_at"):
            last = _dt.datetime.fromisoformat(str(row["last_attempt_at"]).replace("Z", "+00:00"))
            if (_dt.datetime.now(_dt.timezone.utc) - last).total_seconds() < BRIEF_RETRY_S:
                return                               # back off after a failure
        now = _dt.datetime.now(_dt.timezone.utc)
        try:
            cid, text = _ask_genie(BRIEF_Q)
            _save_brief(data_as_of=acts[0].get("data_as_of"), content_hash=h, summary=_plain(text), source="genie",
                        genie_conversation_id=cid, generated_at=now, last_attempt_at=now, last_error=None)
        except Exception as e:
            _save_brief(last_attempt_at=now, last_error=str(e)[:300])
    except Exception:
        pass                                         # no table yet / Lakebase waking up: try again on the next check
    finally:
        _brief["running"] = False


def _kick_brief(force=False):
    if force or _time.time() - _brief["checked"] > BRIEF_RECHECK_S:
        threading.Thread(target=_refresh_brief, daemon=True, name="genie-brief").start()


@app.on_event("startup")
def _brief_on_startup():
    _kick_brief(force=True)                          # prepare the briefing as soon as the app starts


@app.get("/api/briefing/commercial")
def commercial_briefing():
    _kick_brief()
    try:
        acts = _actions()
    except Exception as e:
        return JSONResponse({"actions": [], "error": str(e)[:160]})
    top = acts[:3]
    for a in top:
        a["value_label"] = _money(a.get("value_dollars_yr")) + " a year"
    try:
        row = _brief_row()
    except Exception:
        row = None
    fresh = bool(row and row.get("summary") and row.get("content_hash") == _actions_hash(acts))
    if fresh:
        summary, source = _plain(row["summary"]), "genie"
    else:   # Genie's summary for these actions is not in yet: the same story, worked out from the actions
        tot = sum(float(a.get("value_dollars_yr") or 0) for a in top)
        lead = top[0]["action"] if top else ""
        summary = (f"Together these three actions are worth about {_money(tot)} a year. The largest is to "
                   f"{lead[:1].lower() + lead[1:]} ({top[0]['value_label']}).") if top else ""
        source = "rules"
    return JSONResponse({"actions": top, "summary": summary, "summary_source": source,
                         "pending": bool(GENIE_SPACE) and not fresh and _brief["running"],
                         "data_as_of": top[0].get("data_as_of") if top else None,
                         "generated_at": row.get("generated_at") if fresh else None, "genie": bool(GENIE_SPACE)})


@app.get("/health")
def health():
    try:
        n = _scalar(f"SELECT COUNT(*) v FROM {T('gld_asset_state')}")
        return JSONResponse({"ok": True, "assets": n, "database": "lakebase", "schema": PG_SCHEMA})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)[:200]})


# ============================ Impact analysis (supply network) ===============================
# The physical gas path reservoir -> well pad -> gathering hub -> processing train -> compressor ->
# custody meter -> customer, from gld_supply_nodes / gld_supply_network / gld_customer_supply.
# Selecting any stop traces everything upstream that feeds it and everything downstream it feeds, and
# propagates its throughput along the flow splits to say how much gas each downstream customer would
# lose if that stop stopped. One engine serves the Sankey highlight, the customer bubble and the PDF.
STOP_LABEL = {"reservoir": "Reservoir", "wells": "Well pad", "gathering": "Gathering hub", "processing": "Processing train",
              "compression": "Compressor", "custody": "Custody meter", "customer": "Customer"}
TJ_PER_E3M3 = 0.038


def _impact_graph():
    def load():
        nodes, links, custs = qmany([
            f"SELECT node_id, node_name, stop, stop_order, basin, throughput_e3m3d, throughput_tj_d, status FROM {T('gld_supply_nodes')} ORDER BY stop_order, node_id",
            f"SELECT from_id, to_id, flow_e3m3d, flow_tj_d FROM {T('gld_supply_network')} WHERE flow_e3m3d > 0",
            f"SELECT * FROM {T('gld_customer_supply')} ORDER BY delivery_point, priority, customer_name",
        ])
        return {"nodes": nodes, "links": links, "customers": custs, "as_of": (custs[0].get("as_of_day") if custs else None)}
    return cached("impact_graph", 300, load)


def _impact(node_id):
    g = _impact_graph()
    nodes = {n["node_id"]: n for n in g["nodes"]}
    if node_id not in nodes:
        return None
    kids, pars = {}, {}
    for l in g["links"]:
        kids.setdefault(l["from_id"], []).append(l)
        pars.setdefault(l["to_id"], []).append(l)
    tp = {k: float(n["throughput_e3m3d"] or 0) for k, n in nodes.items()}

    def walk(start, edges, key):
        seen, stack = set(), [start]
        while stack:
            for l in edges.get(stack.pop(), []):
                nxt = l[key]
                if nxt not in seen:
                    seen.add(nxt); stack.append(nxt)
        return seen
    down, up = walk(node_id, kids, "to_id"), walk(node_id, pars, "from_id")
    order = lambda k: nodes[k]["stop_order"] if k in nodes else 9
    # downstream: push the selected stop's throughput along each link in proportion to the link's share of the node
    amt = {node_id: tp[node_id]}
    for n in sorted(down | {node_id}, key=order):
        a = amt.get(n, 0.0)
        if not a or not tp.get(n):
            continue
        for l in kids.get(n, []):
            amt[l["to_id"]] = amt.get(l["to_id"], 0.0) + a * float(l["flow_e3m3d"]) / tp[n]
    # upstream: how much of the selected stop's gas originates at each upstream stop
    src = {node_id: tp[node_id]}
    for n in sorted(up | {node_id}, key=order, reverse=True):
        a = src.get(n, 0.0)
        ins = pars.get(n, [])
        tin = sum(float(l["flow_e3m3d"]) for l in ins)
        if not a or not tin:
            continue
        for l in ins:
            src[l["from_id"]] = src.get(l["from_id"], 0.0) + a * float(l["flow_e3m3d"]) / tin
    on = {node_id} | up | down
    links = [f"{l['from_id']}>{l['to_id']}" for l in g["links"]
             if (l["to_id"] in up | {node_id} and l["from_id"] in up) or (l["from_id"] in down | {node_id} and l["to_id"] in down)]
    sel = nodes[node_id]

    def row(k, v, base):
        n = nodes[k]
        return {"node_id": k, "node_name": n["node_name"], "stop": n["stop"], "stop_label": STOP_LABEL.get(n["stop"], n["stop"]),
                "basin": n["basin"], "status": n["status"], "throughput_e3m3d": round(tp[k], 1),
                "flow_e3m3d": round(v, 1), "share_pct": round(100.0 * v / base, 1) if base else None}
    upstream = sorted([row(k, src.get(k, 0.0), tp[node_id]) for k in up if k in nodes], key=lambda r: (order(r["node_id"]), -r["flow_e3m3d"]))
    downstream = sorted([row(k, amt.get(k, 0.0), tp[k]) for k in down if k in nodes and nodes[k]["stop"] != "customer"],
                        key=lambda r: (order(r["node_id"]), -r["flow_e3m3d"]))
    cust_ids = {k for k in down if nodes.get(k, {}).get("stop") == "customer"}
    if sel["stop"] == "customer":
        cust_ids = {node_id}
        amt[node_id] = tp[node_id]
    customers = []
    for c in g["customers"]:
        if c["customer_id"] not in cust_ids:
            continue
        risk_e3 = amt.get(c["customer_id"], 0.0)
        risk_tj = risk_e3 * TJ_PER_E3M3
        deliv = float(c["delivered_tj_d"] or 0)
        px = float(c["price_gj"] or 0)
        customers.append({**c, "at_risk_tj_d": round(risk_tj, 1), "at_risk_e3m3d": round(risk_e3, 0),
                          "pct_of_supply": round(min(100.0, 100.0 * risk_tj / deliv), 1) if deliv else None,   # rounding can't exceed all its gas
                          "revenue_at_risk_per_day": round(risk_tj * 1000.0 * px, 0)})
    customers.sort(key=lambda c: -c["at_risk_tj_d"])
    return {"node": {**sel, "stop_label": STOP_LABEL.get(sel["stop"], sel["stop"])}, "as_of": g["as_of"],
            "highlight_nodes": sorted(on), "highlight_links": links,
            "upstream": upstream, "downstream": downstream, "customers": customers,
            "totals": {"upstream_stops": len(upstream), "downstream_stops": len(downstream), "customers": len(customers),
                       "at_risk_tj_d": round(sum(c["at_risk_tj_d"] for c in customers), 1),
                       "revenue_at_risk_per_day": round(sum(c["revenue_at_risk_per_day"] for c in customers), 0)}}


@app.get("/api/impact/graph")
def impact_graph():
    try:
        return JSONResponse(_impact_graph())
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/impact/select/{node_id}")
def impact_select(node_id: str):
    r = _impact(node_id)
    return JSONResponse(r) if r else JSONResponse({"error": "unknown stop"}, status_code=404)


@app.get("/api/impact/pdf/{node_id}")
def impact_pdf(node_id: str):
    from fastapi.responses import Response
    r = _impact(node_id)
    if not r:
        return JSONResponse({"error": "unknown stop"}, status_code=404)
    pdf = _impact_pdf(r)
    fn = "impact_" + "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in node_id) + ".pdf"
    return Response(content=pdf, media_type="application/pdf", headers={"Content-Disposition": f'attachment; filename="{fn}"'})


def _impact_pdf(r):
    """A landscape A4 impact report: summary, upstream stops, downstream stops, affected customers."""
    from fpdf import FPDF
    from fpdf.fonts import FontFace

    def t(x):   # core PDF fonts are Latin-1: map the few non-Latin-1 characters we use
        s = "" if x is None else str(x)
        return s.replace("\u2013", "-").replace("\u2014", "-").replace("’", "'").encode("latin-1", "replace").decode("latin-1")

    def num(v, d=1):
        return "" if v is None else f"{float(v):,.{d}f}"
    n, tot = r["node"], r["totals"]
    pdf = FPDF(orientation="L", unit="mm", format="A4")
    pdf.set_auto_page_break(True, margin=14)
    pdf.set_margins(12, 12, 12)
    pdf.alias_nb_pages()

    def footer():
        pdf.set_y(-10); pdf.set_font("Helvetica", "", 7.5); pdf.set_text_color(120, 130, 140)
        pdf.cell(0, 4, t(f"Commercial in confidence. Synthetic demonstration data (fictional customers).   Page {pdf.page_no()} of {{nb}}"), align="R")
    pdf.footer = footer
    pdf.add_page()
    pdf.set_font("Helvetica", "B", 17); pdf.set_text_color(18, 32, 41)
    pdf.cell(0, 9, t("Impact analysis: " + n["node_name"]), new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "", 9.5); pdf.set_text_color(80, 95, 110)
    pdf.cell(0, 5, t(f"{n['stop_label']} {n['node_id']}  |  {n.get('basin') or ''} Basin  |  flows are the average gas day over the 30 gas days to {r['as_of']}  |  generated {_dt.datetime.now().strftime('%d %b %Y %H:%M')}"),
             new_x="LMARGIN", new_y="NEXT")
    pdf.ln(3)
    # summary tiles
    tiles = [("Throughput", f"{num(n['throughput_e3m3d'], 0)} 10³ m³/d"),
             ("Upstream stops", str(tot["upstream_stops"])), ("Downstream stops", str(tot["downstream_stops"])),
             ("Customers affected", str(tot["customers"])), ("Gas at risk", f"{num(tot['at_risk_tj_d'])} TJ/d"),
             ("Revenue at risk", f"${num(tot['revenue_at_risk_per_day'], 0)}/day")]
    w = (pdf.w - 24 - 5 * 3) / 6
    y0 = pdf.get_y()
    for i, (k, v) in enumerate(tiles):
        x = 12 + i * (w + 3)
        pdf.set_fill_color(238, 243, 251); pdf.set_draw_color(201, 215, 236); pdf.rect(x, y0, w, 15, style="DF")
        pdf.set_xy(x + 3, y0 + 2); pdf.set_font("Helvetica", "", 7.5); pdf.set_text_color(95, 113, 133); pdf.cell(w - 6, 4, t(k))
        pdf.set_xy(x + 3, y0 + 7); pdf.set_font("Helvetica", "B", 11); pdf.set_text_color(18, 32, 41); pdf.cell(w - 6, 6, t(v))
    pdf.set_y(y0 + 20)
    pdf.set_font("Helvetica", "", 8.5); pdf.set_text_color(60, 75, 90)
    pdf.multi_cell(0, 4.2, t("Upstream stops are everything whose gas reaches the selected stop, with how much of its throughput each one supplies. "
                             "Downstream stops and customers are everything the selected stop feeds; gas at risk is how much of the selected stop's "
                             "throughput reaches each one along the network's flow splits, i.e. what each would lose if the selected stop stopped."))
    pdf.ln(2)
    head = FontFace(emphasis="BOLD", color=(255, 255, 255), fill_color=(47, 125, 196))

    def table(title, cols, rows, widths):
        pdf.set_font("Helvetica", "B", 11.5); pdf.set_text_color(18, 32, 41)
        pdf.cell(0, 7, t(title), new_x="LMARGIN", new_y="NEXT")
        if not rows:
            pdf.set_font("Helvetica", "", 9); pdf.set_text_color(95, 113, 133)
            pdf.cell(0, 6, "None.", new_x="LMARGIN", new_y="NEXT"); pdf.ln(2); return
        pdf.set_font("Helvetica", "", 8); pdf.set_text_color(28, 43, 58)
        with pdf.table(col_widths=widths, headings_style=head, line_height=5, cell_fill_color=(246, 249, 253),
                       cell_fill_mode="ROWS", borders_layout="HORIZONTAL_LINES", text_align="LEFT") as tb:
            hr = tb.row()
            for c in cols:
                hr.cell(t(c))
            for rr in rows:
                rw = tb.row()
                for v in rr:
                    rw.cell(t(v))
        pdf.ln(4)
    stop_cols = ["Stop", "Id", "Name", "Basin", "Health", "Throughput (10³ m³/d)"]
    table(f"Upstream stops feeding {n['node_id']} ({len(r['upstream'])})",
          stop_cols + ["Gas supplied to it (10³ m³/d)", "Share of its gas"],
          [[u["stop_label"], u["node_id"], u["node_name"], u["basin"], u["status"], num(u["throughput_e3m3d"], 0), num(u["flow_e3m3d"], 0), f"{num(u['share_pct'])}%"] for u in r["upstream"]],
          (24, 34, 72, 26, 16, 30, 40, 26))
    table(f"Downstream stops fed by {n['node_id']} ({len(r['downstream'])})",
          stop_cols + ["Gas from it (10³ m³/d)", "Share of the stop"],
          [[d["stop_label"], d["node_id"], d["node_name"], d["basin"], d["status"], num(d["throughput_e3m3d"], 0), num(d["flow_e3m3d"], 0), f"{num(d['share_pct'])}%"] for d in r["downstream"]],
          (24, 34, 72, 26, 16, 30, 40, 26))
    table(f"Customers affected ({len(r['customers'])})",
          ["Customer", "Segment", "Location", "Meter", "Contract", "Priority", "MDQ (TJ/d)", "Delivered (TJ/d)", "Gas at risk (TJ/d)", "% of supply", "Price ($/GJ)", "Revenue at risk ($/d)", "Contract end"],
          [[c["customer_name"], c["segment"], c["location"], c["delivery_point"], c["contract_type"], str(c["priority"]), num(c["mdq_tj"]), num(c["delivered_tj_d"]),
            num(c["at_risk_tj_d"]), f"{num(c['pct_of_supply'])}%", num(c["price_gj"], 2), num(c["revenue_at_risk_per_day"], 0), c["contract_end"]] for c in r["customers"]],
          (42, 24, 30, 14, 22, 13, 17, 19, 20, 16, 16, 22, 18))
    return bytes(pdf.output())


app.mount("/", StaticFiles(directory="static", html=True), name="static")
