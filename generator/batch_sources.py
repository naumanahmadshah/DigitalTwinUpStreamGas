"""Batch + CDC + file sources (Data-gen Part 5.2/5.3/5.4, cadence classes B and E).

Produces the reference/transactional data that the asset/semantic model and the marts read: EAM
(work orders, notifications, failure history), production accounting (allocation, deferment,
nominations), workforce/permits/MOC/spares (the connected-asset cross-domain play), plus files on the
landing Volume (lab, custody proving, OSDU well/PVT, prices, weather, documents, geometry, imagery).

Everything derives from the SAME backbone + physics + scenarios as the streaming emitters, so the
marts reconcile: a reservoir/liquid-loading/compressor scenario shows up in allocation AND deferment
AND the work-order/spares story at once.
"""
from __future__ import annotations
import argparse, datetime as dt, json, os, random, sys
from pyspark.sql import SparkSession, types as T

GJ_PER_E3M3 = 38.0  # ~heating value: 1000 m^3 sales gas ~ 38 GJ


def _fq(catalog, schema, table):
    return f"`{catalog}`.`{schema}`.`{table}`"


def _vol(catalog, schema, *parts):
    return os.path.join("/Volumes", catalog, schema, "landing", *parts)


def _write_delta(spark, catalog, schema, table, schema_struct, rows, table_comment, col_comments):
    name = _fq(catalog, schema, table)
    names = [f.name for f in schema_struct.fields]
    data = [tuple(r.get(n) for n in names) for r in rows]
    df = spark.createDataFrame(data, schema=schema_struct)
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(name)
    spark.sql(f"COMMENT ON TABLE {name} IS '{table_comment.replace(chr(39), chr(39)*2)}'")
    for c, cm in col_comments.items():
        spark.sql(f"ALTER TABLE {name} ALTER COLUMN `{c}` COMMENT '{cm.replace(chr(39), chr(39)*2)}'")
    print(f"  src {name}: {len(rows)} rows")


def _S(*pairs):
    tm = {"string": T.StringType(), "double": T.DoubleType(), "int": T.IntegerType(),
          "boolean": T.BooleanType(), "timestamp": T.TimestampType()}
    return T.StructType([T.StructField(n, tm[t], True) for (n, t) in pairs])


def generate(spark, cfg, catalog, schema):
    root = None  # imports resolved by run_generator/sys.path
    import asset_model, core, simclock
    seed = int(cfg["seed"])
    rng = random.Random(f"{seed}:batch")
    bb = asset_model.build_semantic_backbone(cfg)
    idx = core.index_backbone(bb)
    assets = bb["asset_master"]
    wells = bb["well_master"]
    start, end = simclock.timeline(cfg)
    scen = core.Scenarios(cfg, start, end)
    import physics

    os.makedirs(_vol(catalog, schema), exist_ok=True)

    import math
    # ---- prices (global daily) + weather (PER LOCATION daily) ----------------------------
    # Australia is a continent: a Beetaloo (tropical north) site and an Otway (cool south) site have
    # very different weather on the same day, so weather is generated per area (every map location),
    # driven by its latitude, not a single field-wide average.
    price_rows, weather_rows = [], []
    price_by_day = {}
    area_geo = {}
    for a in assets:
        la, lo = a.get("gps_lat"), a.get("gps_lon")
        if la is None or lo is None:
            continue
        area_geo.setdefault(a["area_code"], []).append((la, lo))
    area_rep = {ac: (sum(p[0] for p in v) / len(v), sum(p[1] for p in v) / len(v)) for ac, v in area_geo.items()}
    d = start
    while d < end:
        gas = round(9.0 + 2.5 * physics._h(seed, "gas", d.date()) + 1.5 * (d.month in (6, 7, 8)), 3)  # $/GJ, winter bump
        carbon = round(30 + 8 * physics._h(seed, "carbon", d.date()), 2)                              # $/t CO2e
        price_by_day[d.date()] = gas
        price_rows.append({"price_date": d.strftime("%Y-%m-%d"), "gas_price_gj": gas, "carbon_price_t": carbon,
                           "currency": "AUD"})
        doy = d.timetuple().tm_yday
        for ac, (la, lo) in area_rep.items():
            north = max(0.0, min(1.0, (la + 43.0) / 32.0))                 # 1 = tropical north, 0 = cool south
            base = 15.0 + north * 16.0                                      # mean temp ~15C (south) .. ~31C (north)
            seasonal = 7.0 * math.cos(2 * math.pi * (doy - 15) / 365.0) * (1.6 - north)   # bigger swing in the south
            amb = round(base + seasonal + 5.0 * (physics._h(seed, "wx" + ac, d.date()) - 0.5), 1)
            wind = round(10 + 25 * physics._h(seed, "wind" + ac, d.date()) + 12 * (1 - north), 1)       # windier south
            lightning = round(min(1.0, (0.15 + 0.6 * north) * physics._h(seed, "ltn" + ac, d.date())), 2)  # tropical north
            flood = round(min(1.0, (0.10 + 0.5 * north) * physics._h(seed, "fld" + ac, d.date())), 2)
            weather_rows.append({"obs_date": d.strftime("%Y-%m-%d"), "area_code": ac, "ambient_c": amb,
                                 "wind_kmh": wind, "lightning_risk": lightning, "flood_risk": flood,
                                 "lat": round(la, 5), "lon": round(lo, 5)})
        d += dt.timedelta(days=1)
    _csv(_vol(catalog, schema, "prices", "prices.csv"), price_rows)
    _csv(_vol(catalog, schema, "weather", "weather.csv"), weather_rows)

    # ---- allocation + deferment + nominations (per well per gas day) ---------------------
    alloc_rows, defer_rows, nom_rows = [], [], []
    defer_id = 0
    wells_by_wh = idx["wells_by_wh"]
    comp_assets = [a for a in assets if a["equipment_type"] == "compressor"]
    d = start
    while d < end:
        gas_day = d.strftime("%Y-%m-%d")
        df = simclock.day_frac(d, start, end)
        price = price_by_day[d.date()]
        field_actual = 0.0
        field_potential = 0.0
        for w in wells:
            wh = w["wellhead_asset_id"]
            asset = idx["assets_by_id"].get(wh, {"asset_id": wh, "area_code": w["pad_area"], "system_code": "WB",
                                                  "equipment_type": "wellhead"})
            res = physics.reservoir_frac(w["pad_area"], df)
            mods = scen.mods_for(asset, w, d)
            actual = physics.well_rate_e3m3d(w, res, mods)
            potential = physics.well_rate_e3m3d(w, res, {})   # no-scenario potential
            field_actual += actual
            field_potential += potential
            # allocation (with small back-allocation noise)
            allocated = round(actual * (0.97 + 0.06 * physics._h(w["well_id"], "alloc", d.date())), 2)
            alloc_rows.append({"gas_day": gas_day, "well_id": w["well_id"], "asset_id": wh,
                               "area_code": w["pad_area"], "produced_e3m3": actual,
                               "allocated_e3m3": allocated})
            loss = potential - actual
            if loss > max(2.0, 0.03 * potential):
                cause, desc = _defer_cause(scen, asset, w, d)
                defer_id += 1
                defer_rows.append({"deferment_id": f"DEF-{defer_id:07d}", "gas_day": gas_day,
                                   "asset_id": wh, "well_id": w["well_id"], "area_code": w["pad_area"],
                                   "cause_code": cause, "cause_desc": desc,
                                   "deferred_e3m3": round(loss, 2),
                                   "deferred_dollars": round(loss * GJ_PER_E3M3 * price, 0),
                                   "updated_at": d})
        # compressor-constraint deferment (throughput binding on the degrading unit)
        for a in comp_assets:
            mods = scen.mods_for(a, None, d)
            deg = mods.get("degradation", 0.0)
            if deg > 0.55:
                lost = round(200 * (deg - 0.55), 2)
                if lost > 2:
                    defer_id += 1
                    defer_rows.append({"deferment_id": f"DEF-{defer_id:07d}", "gas_day": gas_day,
                                       "asset_id": a["asset_id"], "well_id": None, "area_code": a["area_code"],
                                       "cause_code": "COMP-CONSTRAINT",
                                       "cause_desc": "Compression capacity constraint (rising vibration, efficiency loss)",
                                       "deferred_e3m3": lost, "deferred_dollars": round(lost * GJ_PER_E3M3 * price, 0),
                                       "updated_at": d})
        nominated = round(field_potential * 0.95, 1)  # commitment close to deliverability; field dips below it during the acute constraint
        nom_rows.append({"gas_day": gas_day, "nominated_e3m3": nominated,
                         "actual_e3m3": round(field_actual, 1),
                         "field_potential_e3m3": round(field_potential, 1)})
        d += dt.timedelta(days=1)

    _write_delta(spark, catalog, schema, "src_pa_allocation",
                 _S(("gas_day", "string"), ("well_id", "string"), ("asset_id", "string"),
                    ("area_code", "string"), ("produced_e3m3", "double"), ("allocated_e3m3", "double")),
                 alloc_rows, "PA back-allocation: per-well produced vs allocated by gas day (Energy Components-style). Feeds gld_custody_allocation, gld_field_production, gld_well_scorecard.",
                 {"gas_day": "Gas day (YYYY-MM-DD).", "well_id": "Well id.", "asset_id": "Wellhead asset id.",
                  "area_code": "Pad area.", "produced_e3m3": "Actual produced volume (10^3 m^3).",
                  "allocated_e3m3": "Back-allocated volume (10^3 m^3); differs from produced by allocation factor."})
    _write_delta(spark, catalog, schema, "src_pa_deferment",
                 _S(("deferment_id", "string"), ("gas_day", "string"), ("asset_id", "string"),
                    ("well_id", "string"), ("area_code", "string"), ("cause_code", "string"),
                    ("cause_desc", "string"), ("deferred_e3m3", "double"), ("deferred_dollars", "double"),
                    ("updated_at", "timestamp")),
                 defer_rows, "PA deferment/lost-production ledger with ISO-14224-style coded cause and dollar impact. Feeds gld_deferment and gld_economics; answers the Genie deferment question.",
                 {"deferment_id": "Deferment event id.", "gas_day": "Gas day.", "asset_id": "Asset that deferred.",
                  "well_id": "Well id where applicable.", "area_code": "Area.", "cause_code": "Coded cause (RES-DECL, WELL-LL, UNPLANNED-SD, COMP-CONSTRAINT).",
                  "cause_desc": "Human-readable cause.", "deferred_e3m3": "Deferred volume (10^3 m^3).",
                  "deferred_dollars": "Dollar impact (deferred x heating value x gas price).", "updated_at": "CDC commit time."})
    _write_delta(spark, catalog, schema, "src_pa_nominations",
                 _S(("gas_day", "string"), ("nominated_e3m3", "double"), ("actual_e3m3", "double"),
                    ("field_potential_e3m3", "double")),
                 nom_rows, "Daily field nomination vs actual vs potential. Feeds gld_field_production (gap-to-nomination).",
                 {"gas_day": "Gas day.", "nominated_e3m3": "Contracted nomination (10^3 m^3).",
                  "actual_e3m3": "Actual field production (10^3 m^3).", "field_potential_e3m3": "Unconstrained potential (10^3 m^3)."})

    # ---- EAM: asset register, work orders, notifications, failure history ----------------
    wo_rows, notif_rows, fail_rows = [], [], []
    wo_id = notif_id = fail_id = 0
    for a in assets:
        crit_w = {"A": 3, "B": 2, "C": 1}.get(a["criticality"], 1)
        # a couple of past failures for older/critical rotating assets
        if a["equipment_type"] in ("compressor", "esp", "wellhead") and physics._h(a["asset_id"], "fail") < 0.4 * crit_w / 3:
            fail_id += 1
            fdate = start + dt.timedelta(days=int(physics._h(a["asset_id"], "fdate") * ((end - start).days)))
            fail_rows.append({"failure_id": f"FL-{fail_id:06d}", "asset_id": a["asset_id"],
                              "failure_date": fdate.strftime("%Y-%m-%d"),
                              "iso14224_code": rng.choice(["1.1 Vibration", "3.2 Leakage", "4.1 Overheating",
                                                            "2.3 Seal failure", "5.1 Instrument fault"]),
                              "failure_mode": rng.choice(["bearing wear", "seal leak", "valve fault", "sensor drift"]),
                              "downtime_hours": round(rng.uniform(2, 72), 1), "updated_at": fdate})
        # open/closed work orders; more for degraded/critical
        nwo = rng.randint(0, crit_w)
        for _ in range(nwo):
            wo_id += 1
            created = start + dt.timedelta(days=int(physics._h(a["asset_id"], "wo", wo_id) * ((end - start).days)))
            wo_type = rng.choice(["PM", "CM", "inspection", "calibration"])
            if wo_type == "PM":
                # planned maintenance is kept compliant: ~90% completed on schedule
                status = rng.choices(["completed", "in_progress", "open"], weights=[90, 5, 5])[0]
            else:
                status = rng.choices(["completed", "in_progress", "open", "rejected"], weights=[6, 2, 3, 1])[0]
            priority = rng.choice(["P1", "P2", "P3", "P4"])
            # EAM labour assignment: the WO carries the crew_id it's assigned to (the real join key to HR),
            # and a scheduled finish derived from priority SLA.
            target = created + dt.timedelta(days={"P1": 3, "P2": 7, "P3": 21, "P4": 45}[priority])
            wo_rows.append({"wo_id": f"WO-{wo_id:07d}", "asset_id": a["asset_id"],
                            "wo_type": wo_type,
                            "priority": priority, "status": status,
                            "assigned_crew_id": f"CR-{rng.randint(0, 29):03d}",
                            "target_completion": target.strftime("%Y-%m-%d %H:%M"),
                            "description": f"{a['equipment_type']} maintenance on {a['asset_id']}",
                            "created_date": created.strftime("%Y-%m-%d"),
                            "cost": round(rng.uniform(500, 45000), 0), "updated_at": created})
        if physics._h(a["asset_id"], "notif") < 0.25:
            notif_id += 1
            notif_rows.append({"notif_id": f"NT-{notif_id:06d}", "asset_id": a["asset_id"],
                               "notif_type": rng.choice(["anomaly", "inspection_due", "corrosion", "operator_flag"]),
                               "priority": rng.choice(["high", "medium", "low"]),
                               "description": f"Notification on {a['asset_id']}",
                               "raised_date": (end - dt.timedelta(days=rng.randint(1, 120))).strftime("%Y-%m-%d"),
                               "status": rng.choice(["open", "closed"]), "updated_at": end})
    # ensure the compressor-scenario asset carries an OPEN high-priority WO + a fresh notification (the story).
    # Resolve to a REAL compressor asset in CPF002 (deterministic: first by asset_id) so the connected-asset
    # cross-domain play (WO + spares + MOC + roster) lands on an asset that actually exists.
    _comp2 = sorted(a["asset_id"] for a in assets if a["area_code"] == "CPF002" and a["equipment_type"] == "compressor")
    vib_target = _comp2[0] if _comp2 else None
    if vib_target:
        wo_id += 1
        wo_rows.append({"wo_id": f"WO-{wo_id:07d}", "asset_id": vib_target, "wo_type": "CM", "priority": "P1",
                        "status": "open", "assigned_crew_id": "CR-000",
                        "target_completion": (end - dt.timedelta(days=3)).strftime("%Y-%m-%d %H:%M"),
                        "description": f"Investigate rising vibration on {vib_target}; bearing suspected",
                        "created_date": (end - dt.timedelta(days=6)).strftime("%Y-%m-%d"), "cost": 38000.0,
                        "updated_at": end})

    _write_delta(spark, catalog, schema, "src_eam_workorders",
                 _S(("wo_id", "string"), ("asset_id", "string"), ("wo_type", "string"), ("priority", "string"),
                    ("status", "string"), ("assigned_crew_id", "string"), ("target_completion", "string"),
                    ("description", "string"), ("created_date", "string"),
                    ("cost", "double"), ("updated_at", "timestamp")),
                 wo_rows, "EAM (Maximo/SAP PM) work orders. Feeds gld_maintenance_reliability, the connected-asset play, and the Recommendation Inbox context.",
                 {"wo_id": "Work-order id.", "asset_id": "Asset the WO is against.", "wo_type": "PM/CM/inspection/calibration.",
                  "priority": "P1-P4.", "status": "open/in_progress/completed/rejected.",
                  "assigned_crew_id": "Crew the WO is assigned to (join key to the HR/workforce roster).",
                  "target_completion": "Scheduled completion date/time (priority SLA).", "description": "WO text.",
                  "created_date": "Creation date.", "cost": "WO cost (AUD).", "updated_at": "CDC commit time."})
    _write_delta(spark, catalog, schema, "src_eam_notifications",
                 _S(("notif_id", "string"), ("asset_id", "string"), ("notif_type", "string"), ("priority", "string"),
                    ("description", "string"), ("raised_date", "string"), ("status", "string"), ("updated_at", "timestamp")),
                 notif_rows, "EAM notifications (anomaly/inspection/corrosion/operator flags). Feeds gld_maintenance_reliability and integrity.",
                 {"notif_id": "Notification id.", "asset_id": "Asset.", "notif_type": "Type.", "priority": "high/medium/low.",
                  "description": "Text.", "raised_date": "Raised date.", "status": "open/closed.", "updated_at": "CDC commit time."})
    _write_delta(spark, catalog, schema, "src_eam_failure_history",
                 _S(("failure_id", "string"), ("asset_id", "string"), ("failure_date", "string"),
                    ("iso14224_code", "string"), ("failure_mode", "string"), ("downtime_hours", "double"),
                    ("updated_at", "timestamp")),
                 fail_rows, "ISO 14224 failure history. Feeds gld_maintenance_reliability and RUL/reliability context.",
                 {"failure_id": "Failure id.", "asset_id": "Asset.", "failure_date": "Date.",
                  "iso14224_code": "ISO 14224 failure-mode code.", "failure_mode": "Mode.", "downtime_hours": "Downtime (h).",
                  "updated_at": "CDC commit time."})

    # ---- workforce / permits / MOC / spares (the connected-asset cross-domain play) ------
    _connected_asset_sources(spark, catalog, schema, cfg, assets, wells, start, end, rng, vib_target)

    # ---- lab assay + custody proving (files) --------------------------------------------
    _lab_and_proving(spark, catalog, schema, cfg, bb, start, end, rng)

    # ---- OSDU well/PVT, documents, geometry, imagery (files) -----------------------------
    _osdu_docs_geometry(spark, catalog, schema, bb)

    print("Batch/CDC/file sources complete.")


def _defer_cause(scen, asset, well, d):
    for (sid, target, f) in scen.active_targets(d):
        if sid == "reservoir_depletion" and (asset["area_code"] == target or well["pad_area"] == target):
            return "RES-DECL", "Reservoir pressure decline vs plan"
        if sid == "liquid_loading_onset" and (asset["area_code"] == target or well["pad_area"] == target):
            return "WELL-LL", "Liquid loading (below critical rate)"
        if sid == "deferment_event" and (asset["area_code"] == target or well["pad_area"] == target):
            return "UNPLANNED-SD", "Unplanned shutdown / trip"
    return "MINOR-VAR", "Minor production variance"


def _connected_asset_sources(spark, catalog, schema, cfg, assets, wells, start, end, rng, vib_target):
    # crew / workforce
    crew = []
    certs = ["rotating_equipment", "electrical", "instrumentation", "confined_space", "hot_work", "gas_test"]
    for i in range(30):
        crew.append({"crew_id": f"CR-{i:03d}", "name": f"Technician {i:02d}",
                     "certifications": ",".join(rng.sample(certs, k=rng.randint(1, 3))),
                     "available": rng.random() > 0.25,
                     "on_leave_until": ((end + dt.timedelta(days=rng.randint(1, 6))).strftime("%Y-%m-%d")
                                        if rng.random() < 0.3 else None),
                     "home_area": rng.choice([a["area_code"] for a in assets])})
    # make the only rotating-equipment cert holder near the compressor scenario on leave (the story)
    crew[0].update({"certifications": "rotating_equipment,confined_space", "available": False,
                    "on_leave_until": (end + dt.timedelta(days=3)).strftime("%Y-%m-%d")})
    _write_delta(spark, catalog, schema, "src_hr_workforce",
                 _S(("crew_id", "string"), ("name", "string"), ("certifications", "string"),
                    ("available", "boolean"), ("on_leave_until", "string"), ("home_area", "string")),
                 crew, "HR/workforce roster: crew competency and availability. The workforce arm of the connected-asset play (who is certified and available).",
                 {"crew_id": "Crew id.", "name": "Name (synthetic).", "certifications": "Comma-separated certs.",
                  "available": "Available now.", "on_leave_until": "Leave end date if on leave.", "home_area": "Home area."})

    # permits-to-work
    permits = []
    for i in range(12):
        a = rng.choice(assets)
        permits.append({"permit_id": f"PTW-{i:04d}", "asset_id": a["asset_id"], "area_code": a["area_code"],
                        "permit_type": rng.choice(["hot_work", "confined_space", "electrical_isolation", "general"]),
                        "status": rng.choice(["open", "closed"]),
                        "opened_at": (end - dt.timedelta(days=rng.randint(0, 20))).strftime("%Y-%m-%d"),
                        "isolation": rng.choice([True, False])})
    _write_delta(spark, catalog, schema, "src_hse_permits",
                 _S(("permit_id", "string"), ("asset_id", "string"), ("area_code", "string"),
                    ("permit_type", "string"), ("status", "string"), ("opened_at", "string"), ("isolation", "boolean")),
                 permits, "HSE permit-to-work: open permits, isolations, safe-to-start context. Contextualized to asset for the safety and connected-asset surfaces.",
                 {"permit_id": "Permit id.", "asset_id": "Asset.", "area_code": "Area.", "permit_type": "Type.",
                  "status": "open/closed.", "opened_at": "Opened date.", "isolation": "Isolation in place."})

    # MOC / OT-security change log (incl. a firmware change just before the compressor anomaly)
    moc = []
    for i in range(15):
        a = rng.choice(assets)
        moc.append({"moc_id": f"MOC-{i:04d}", "asset_id": a["asset_id"], "change_type": rng.choice(["firmware", "config", "setpoint", "hardware"]),
                    "description": f"Change on {a['asset_id']}", "change_date": (end - dt.timedelta(days=rng.randint(1, 200))).strftime("%Y-%m-%d"),
                    "approved_by": f"eng-{rng.randint(1,9)}"})
    if vib_target:
        moc.append({"moc_id": "MOC-9001", "asset_id": vib_target, "change_type": "firmware",
                    "description": f"Controller firmware update landed on {vib_target} two days before the vibration anomaly began",
                    "change_date": (end - dt.timedelta(days=8)).strftime("%Y-%m-%d"), "approved_by": "eng-2"})
    _write_delta(spark, catalog, schema, "src_ot_moc_log",
                 _S(("moc_id", "string"), ("asset_id", "string"), ("change_type", "string"),
                    ("description", "string"), ("change_date", "string"), ("approved_by", "string")),
                 moc, "OT-security / management-of-change log. The change-history arm of the connected-asset play (a firmware update landed before the anomaly).",
                 {"moc_id": "MOC id.", "asset_id": "Asset.", "change_type": "firmware/config/setpoint/hardware.",
                  "description": "Change text.", "change_date": "Date.", "approved_by": "Approver."})

    # spares inventory (incl. a bearing 6 weeks out for the compressor story). Each part carries the
    # equipment type it applies to (the maintenance-BOM key that joins a spare to an asset's RUL).
    spares = []
    # each procurable item carries WHAT it is (part / repair / service) so the asset detail can say
    # exactly what has to be ordered when its lead time beats the remaining life.
    part_equip = {"bearing": "compressor", "seal": "compressor", "impeller": "compressor",
                  "valve trim": "gathering_line", "filter": "separator", "gasket": "separator", "sensor": "wellhead",
                  "compressor overhaul": "compressor", "seal-face repair": "compressor",
                  "valve re-trim": "gathering_line", "separator internals inspection": "separator"}
    part_proc = {"bearing": "part", "seal": "part", "impeller": "part", "valve trim": "part", "filter": "part",
                 "gasket": "part", "sensor": "part", "compressor overhaul": "service", "seal-face repair": "repair",
                 "valve re-trim": "repair", "separator internals inspection": "service"}
    parts = list(part_equip.keys())
    for i in range(60):
        pn = rng.choice(parts)
        spares.append({"part_id": f"SP-{i:04d}", "part_name": pn, "procurement_type": part_proc[pn],
                       "applies_equipment_type": part_equip[pn],
                       "for_area": rng.choice([a["area_code"] for a in assets]),
                       "on_hand": rng.randint(0, 8), "lead_time_days": rng.choice([0, 3, 7, 14, 42]),
                       "unit_cost": round(rng.uniform(200, 20000), 0)})
    if vib_target:
        spares.append({"part_id": "SP-BRG-01", "part_name": "bearing", "procurement_type": "part",
                       "applies_equipment_type": "compressor", "for_area": vib_target.split("-")[0],
                       "on_hand": 0, "lead_time_days": 42, "unit_cost": 12500.0})
    _write_delta(spark, catalog, schema, "src_spares_inventory",
                 _S(("part_id", "string"), ("part_name", "string"), ("procurement_type", "string"), ("applies_equipment_type", "string"), ("for_area", "string"),
                    ("on_hand", "int"), ("lead_time_days", "int"), ("unit_cost", "double")),
                 spares, "Spare-parts / repair / service inventory with on-hand, procurement lead time and the equipment type each item applies to (maintenance BOM). Joins to predictive RUL to flag items whose lead time beats the asset's remaining life.",
                 {"part_id": "Procurement id.", "part_name": "Part / repair / service.", "procurement_type": "What to procure: part, repair or service.", "applies_equipment_type": "Equipment type the item applies to (BOM join key).",
                  "for_area": "Area it serves.", "on_hand": "Units on hand.",
                  "lead_time_days": "Procurement lead time (days).", "unit_cost": "Unit cost (AUD)."})

    # ---- commercial gas-sales contracts (#1 margin) : keyed to the custody delivery point --------
    counterparties = ["Southern Cross Energy", "Coral Coast Gas Retail", "Outback Power Co", "Meridian LNG Trading", "Tasman Industrial Gas"]  # fictional
    custody_areas = [st["area"] for st in cfg["custody"]["stations"]]
    contracts = []
    for i, area in enumerate(custody_areas):
        for j in range(2):
            k = i * 2 + j
            contracts.append({"contract_id": f"GSA-{k:03d}", "counterparty": counterparties[k % len(counterparties)],
                              "delivery_point": area, "daily_qty_e3m3": round(rng.uniform(1500, 4200), 0),
                              "price_basis_gj": round(rng.uniform(9.0, 13.0), 2), "take_or_pay_pct": rng.choice([80, 85, 90]),
                              "start_date": start.strftime("%Y-%m-%d"), "end_date": (end + dt.timedelta(days=365)).strftime("%Y-%m-%d")})
    _write_delta(spark, catalog, schema, "src_commercial_contracts",
                 _S(("contract_id", "string"), ("counterparty", "string"), ("delivery_point", "string"),
                    ("daily_qty_e3m3", "double"), ("price_basis_gj", "double"), ("take_or_pay_pct", "int"),
                    ("start_date", "string"), ("end_date", "string")),
                 contracts, "Gas-sales agreements (GSAs): each contract names a custody delivery point and a daily contracted quantity (the real join key from the trading book to metered production). Feeds the molecule-to-market margin.",
                 {"contract_id": "Contract id.", "counterparty": "Buyer.", "delivery_point": "Custody station the gas is delivered at (join key to metering).",
                  "daily_qty_e3m3": "Contracted daily quantity (10^3 m^3).", "price_basis_gj": "Contract price ($/GJ).",
                  "take_or_pay_pct": "Take-or-pay floor (%).", "start_date": "Start.", "end_date": "End."})

    # ---- development / capex schedule (#6 capital & decline) : keyed to area + planned date ------
    dev = []
    ptypes = [("infill_well", 1.2e7, 350), ("workover", 1.5e6, 120), ("compression_upgrade", 3.0e7, 900), ("dehydration_debottleneck", 8.0e6, 180)]
    dev_areas = sorted({a["area_code"] for a in assets})
    for i in range(12):
        area = dev_areas[i % len(dev_areas)]
        pt, capex, uplift = ptypes[i % len(ptypes)]
        dev.append({"project_id": f"CAP-{i:03d}", "area_code": area, "project_type": pt,
                    "capex_dollars": round(capex * rng.uniform(0.7, 1.4), 0),
                    "expected_uplift_e3m3d": round(uplift * rng.uniform(0.6, 1.3), 0),
                    "planned_date": (end + dt.timedelta(days=rng.randint(20, 400))).strftime("%Y-%m-%d"),
                    "status": rng.choice(["proposed", "approved", "fel-2"])})
    _write_delta(spark, catalog, schema, "src_dev_schedule",
                 _S(("project_id", "string"), ("area_code", "string"), ("project_type", "string"),
                    ("capex_dollars", "double"), ("expected_uplift_e3m3d", "double"), ("planned_date", "string"), ("status", "string")),
                 dev, "Development / capital schedule: infill wells, workovers and debottlenecks by area with capex and expected uplift (the join key from finance/development planning to the subsurface decline forecast).",
                 {"project_id": "Project id.", "area_code": "Area the project targets (join key to decline forecast).", "project_type": "Project type.",
                  "capex_dollars": "Capital cost (AUD).", "expected_uplift_e3m3d": "Expected production uplift (10^3 m^3/d).",
                  "planned_date": "Planned execution date.", "status": "Maturity (proposed/approved/fel-2)."})


def _lab_and_proving(spark, catalog, schema, cfg, bb, start, end, rng):
    # LIMS gas-composition sampling across the value chain (not just a handful): every well's wellhead
    # gas ~quarterly, plus inlet / treated / sales-gas composition at the processing and custody points
    # ~monthly. Each sample carries its asset_id and sample point so it attaches to the asset detail.
    lab_rows = []
    k = [0]

    def _comp(h2s_hi):
        return {"c1_molpct": round(rng.uniform(85, 96), 2), "co2_molpct": round(rng.uniform(0.5, 7), 2),
                "n2_molpct": round(rng.uniform(0.3, 3), 2), "h2s_ppm": rng.randint(0, h2s_hi),
                "water_ppm": rng.randint(20, 400), "oil_in_water_ppm": round(rng.uniform(2, 40), 1)}

    def _row(asset_id, well_id, point, d, h2s_hi):
        k[0] += 1
        r = {"sample_id": f"LAB-{k[0]:05d}", "asset_id": asset_id, "well_id": well_id or "",
             "sample_point": point, "sample_date": d.strftime("%Y-%m-%d")}
        r.update(_comp(h2s_hi))
        lab_rows.append(r)

    for w in bb["well_master"]:                                   # wellhead gas, ~quarterly per well
        d = start + dt.timedelta(days=rng.randint(0, 90))
        while d < end:
            _row(w["wellhead_asset_id"], w["well_id"], "wellhead", d, 350)
            d += dt.timedelta(days=rng.randint(80, 110))
    pts = {"SEP": ("inlet separator", 350), "DEH": ("treated gas", 10), "MET": ("sales gas (custody)", 8)}
    for a in bb["asset_master"]:                                  # value-chain points, ~monthly (sweet downstream)
        spec = pts.get(a.get("system_code"))
        if not spec:
            continue
        point, hi = spec
        d = start + dt.timedelta(days=rng.randint(0, 30))
        while d < end:
            _row(a["asset_id"], "", point, d, hi)
            d += dt.timedelta(days=rng.randint(26, 34))
    _csv(_vol(catalog, schema, "lab", "lab_assay.csv"), lab_rows)

    prov_rows = []
    meters = [a for a in bb["asset_master"] if a["system_code"] == "MET"]
    for m in meters:
        for k in range(6):
            pd = end - dt.timedelta(days=k * 60 + rng.randint(0, 20))
            prov_rows.append({"meter_id": m["asset_id"], "prove_ts": pd.strftime("%Y-%m-%d"),
                              "meter_factor": round(rng.uniform(0.997, 1.003), 5),
                              "k_factor": round(rng.uniform(0.98, 1.02), 5),
                              "uncertainty_pct": round(rng.uniform(0.2, 0.9), 3),
                              "proving_due": (k == 0 and rng.random() < 0.5)})
    _csv(_vol(catalog, schema, "proving", "custody_proving.csv"), prov_rows)


def _osdu_docs_geometry(spark, catalog, schema, bb):
    # OSDU well + PVT JSON
    _json(_vol(catalog, schema, "well_master", "well_master.json"), bb["well_master"])
    _json(_vol(catalog, schema, "pvt", "pvt.json"), bb["pvt"])
    # geometry manifest: scene -> asset hotspot (the 3D navigator join)
    geo = [{"scene_id": a["scene_id"], "asset_id": a["asset_id"], "hotspot_x": a["hotspot_x"],
            "hotspot_y": a["hotspot_y"], "lod": "med", "equipment_type": a["equipment_type"]}
           for a in bb["asset_master"]]
    _json(_vol(catalog, schema, "geometry", "geometry_manifest.json"), geo)
    # a few P&ID document stand-ins (Auto Loader picks up as binary; parsing deferred to Silver)
    docdir = _vol(catalog, schema, "documents")
    os.makedirs(docdir, exist_ok=True)
    areas = sorted({a["area_code"] for a in bb["asset_master"]})
    for ar in areas[:8]:
        tags = [t["tag_id"] for t in bb["tag_registry"] if t["area_code"] == ar][:12]
        with open(os.path.join(docdir, f"PID-{ar}.txt"), "w") as f:
            f.write(f"P&ID {ar}\nTags referenced: {', '.join(tags)}\nProcedure: isolate, purge, inspect.\n")
    # imagery manifest (OGI/drone)
    img = [{"image_id": f"IMG-{i:04d}", "type": "OGI", "asset_id": bb['asset_master'][i]["asset_id"],
            "captured": "2026-09-01", "finding": "none"} for i in range(min(10, len(bb['asset_master'])))]
    _json(_vol(catalog, schema, "imagery", "imagery_manifest.json"), img)


def _csv(path, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if not rows:
        return
    cols = list(rows[0].keys())
    import csv
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow(r)
    print(f"  file {path}: {len(rows)} rows")


def _json(path, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        for r in rows:
            f.write(json.dumps(r, default=str) + "\n")
    print(f"  file {path}: {len(rows)} rows")
