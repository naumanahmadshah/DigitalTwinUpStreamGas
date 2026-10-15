"""Phase 1 — the semantic backbone (the CONTRACT).

Builds the reference world deterministically from the seed config (Data-gen Part 9 Layer 1 /
Design Part 6.1): the ISO-14224 functional-location hierarchy, the asset master, the tag registry
under AREA-SYSTEM-CLASS-NNNN, the well master, PVT, and the cross-system id map. Every telemetry
point the generator emits and every Silver join binds to the `tag_id` this produces.

Pure stdlib (runs on serverless spark_python_task with no extra deps). Returns plain dict rows;
write_backbone.py writes them to Delta with column comments.
"""
from __future__ import annotations
import random
from typing import Any

# ---------------------------------------------------------------------------------------------
# Signal catalogue per equipment type: (signal, class_code, unit, range_min, range_max,
# measured|calculated, cadence). This is what makes the tag registry realistic. Class codes and
# units follow config/twin_config.yaml. measured tags stream; calculated tags are derived.
# ---------------------------------------------------------------------------------------------
SIGNAL_SPECS: dict[str, list[tuple]] = {
    "wellhead": [
        ("tubing_pressure",   "PT", "kPa",   500, 20000, "measured",   "RT"),
        ("casing_pressure_a", "PT", "kPa",   200, 18000, "measured",   "RT"),
        ("casing_pressure_b", "PT", "kPa",   100, 15000, "measured",   "NRT"),
        ("wellhead_temp",     "TT", "degC",   10,   120, "measured",   "RT"),
        ("choke_position",    "ZT", "pct",     0,   100, "measured",   "RT"),
        ("production_flow",   "FT", "e3m3/d",  0,   400, "measured",   "RT"),
        ("sssv_status",       "ZT", "pct",     0,   100, "measured",   "NRT"),
    ],
    "esp": [
        ("esp_frequency",     "SP", "Hz",     30,    65, "measured",   "RT"),
        ("esp_intake_press",  "PT", "kPa",   500, 12000, "measured",   "RT"),
        ("esp_motor_temp",    "TT", "degC",   40,   160, "measured",   "RT"),
        ("esp_current",       "JT", "A",       5,   180, "measured",   "RT"),
        ("esp_vibration",     "VT", "mm/s",  0.5,    20, "measured",   "RT"),
    ],
    "gas_lift": [
        ("gl_inj_rate",       "FT", "e3m3/d",  0,    60, "measured",   "NRT"),
        ("gl_inj_pressure",   "PT", "kPa",  2000, 18000, "measured",   "NRT"),
    ],
    "gathering_line": [
        ("line_pressure",     "PT", "kPa",   500, 12000, "measured",   "RT"),
        ("line_temp",         "TT", "degC",    5,    80, "measured",   "NRT"),
        ("line_flow",         "FT", "e3m3/d",  0,  2500, "measured",   "RT"),
        ("esd_status",        "ZT", "pct",     0,   100, "measured",   "NRT"),
    ],
    "separator": [
        ("sep_pressure",      "PT", "kPa",  1000,  9000, "measured",   "RT"),
        ("sep_temp",          "TT", "degC",    5,    90, "measured",   "RT"),
        ("sep_level",         "LT", "pct",     0,   100, "measured",   "RT"),
        ("sep_interface",     "LT", "pct",     0,   100, "measured",   "NRT"),
        ("sep_dp",            "PT", "kPa",     0,   500, "measured",   "NRT"),
    ],
    "compressor": [
        ("suction_pressure",  "PT", "kPa",   800,  6000, "measured",   "RT"),
        ("discharge_pressure","PT", "kPa",  3000, 16000, "measured",   "RT"),
        ("suction_temp",      "TT", "degC",    5,    60, "measured",   "RT"),
        ("discharge_temp",    "TT", "degC",   40,   180, "measured",   "RT"),
        ("speed",             "SP", "rpm",  3000, 14000, "measured",   "RT"),
        ("throughput",        "FT", "e3m3/d",  0,  1800, "measured",   "RT"),
        ("power",             "JT", "kW",    500,  9000, "measured",   "RT"),
        ("vibration_overall", "VT", "mm/s",  0.5,    25, "measured",   "RT"),
        ("bearing_temp",      "TT", "degC",   40,   140, "measured",   "RT"),
        ("seal_temp",         "TT", "degC",   40,   150, "measured",   "NRT"),
        ("antisurge_valve",   "ZT", "pct",     0,   100, "measured",   "RT"),
        ("polytropic_eff",    "AT", "pct",    40,    90, "calculated", "NRT"),
        ("ch4_massrate",      "AT", "kg/h",    0,   120, "calculated", "NRT"),   # rod-packing / seal vent methane
        ("co2_massrate",      "AT", "kg/h",    0, 11000, "calculated", "NRT"),   # fuel-gas combustion CO2
    ],
    "teg_dehydration": [
        ("contactor_pressure","PT", "kPa",  4000, 12000, "measured",   "RT"),
        ("contactor_temp",    "TT", "degC",   20,    60, "measured",   "RT"),
        ("glycol_flow",       "FT", "m3/h",    1,    40, "measured",   "NRT"),
        ("reboiler_temp",     "TT", "degC",  150,   210, "measured",   "RT"),
        ("water_dewpoint",    "AT", "degC",  -20,    10, "calculated", "NRT"),
        ("ch4_massrate",      "AT", "kg/h",    0,    90, "calculated", "NRT"),   # still-column / regenerator vent methane
    ],
    "amine": [
        ("h2s_outlet",        "AT", "ppm",     0,    30, "calculated", "NRT"),
        ("co2_outlet",        "AT", "molpct",  0,     4, "calculated", "NRT"),
        ("amine_circ",        "FT", "m3/h",   20,   400, "measured",   "NRT"),
        ("reboiler_duty",     "JT", "kW",    500,  8000, "measured",   "NRT"),
        ("foaming_index",     "AT", "idx",     0,   100, "calculated", "NRT"),
    ],
    "ngl_stab": [
        ("column_temp",       "TT", "degC",   40,   220, "measured",   "RT"),
        ("jt_pressure",       "PT", "kPa",  2000, 10000, "measured",   "RT"),
        ("expander_speed",    "SP", "rpm",  8000, 30000, "measured",   "RT"),
        ("product_rvp",       "AT", "kPa",   40,   120, "calculated", "NRT"),
        ("ngl_recovery",      "AT", "pct",   50,    98, "calculated", "NRT"),
    ],
    "utilities": [
        ("turbine_load",      "JT", "MW",      1,    30, "measured",   "RT"),
        ("fuel_gas_flow",     "FT", "e3m3/d",  5,   300, "measured",   "NRT"),
        ("instrument_air",    "PT", "kPa",   500,   900, "measured",   "NRT"),
        ("co2_massrate",      "AT", "kg/h",    0, 16000, "calculated", "NRT"),   # gas-turbine / genset combustion CO2
    ],
    "flare": [
        ("flare_flow",        "FT", "e3m3/d",  0,   500, "measured",   "RT"),
        ("combustion_temp",   "TT", "degC",  400,  1100, "measured",   "NRT"),
        ("ch4_massrate",      "AT", "kg/h",    0,   800, "calculated", "NRT"),
        ("co2_massrate",      "AT", "kg/h",    0, 60000, "calculated", "NRT"),
        ("nox_massrate",      "AT", "kg/h",    0,   200, "calculated", "NRT"),
    ],
    "metering": [
        ("meter_flow",        "FE", "e3m3/d",  0,  6000, "measured",   "NRT"),
        ("meter_pressure",    "PT", "kPa",  4000, 12000, "measured",   "NRT"),
        ("meter_temp",        "TT", "degC",    5,    50, "measured",   "NRT"),
        ("ghv",               "AT", "MJ/m3",  36,    42, "calculated", "NRT"),
        ("wobbe",             "AT", "MJ/m3",  46,    52, "calculated", "NRT"),
        ("gc_c1",             "AT", "molpct", 80,    98, "measured",   "NRT"),
        ("gc_co2",            "AT", "molpct",  0,     8, "measured",   "NRT"),
        ("gc_n2",             "AT", "molpct",  0,     5, "measured",   "NRT"),
    ],
    "fire_gas": [
        ("gas_detection",     "GD", "pctLEL",  0,   100, "measured",   "NRT"),
    ],
}

# tags whose signal is really an event carrier (safety/fg) — handled by the event emitter too.
CRITICALITY_TIERS = ["A", "B", "C"]


def _rng(seed: int, salt: str) -> random.Random:
    """Deterministic sub-stream keyed by a salt, so structures are independent but reproducible."""
    return random.Random(f"{seed}:{salt}")


def _scatter(rng: random.Random, lat: float, lon: float, deg: float) -> tuple[float, float]:
    return round(lat + rng.uniform(-deg, deg), 6), round(lon + rng.uniform(-deg, deg), 6)


# Real Australian gas basins (onshore / nearshore producing hubs). The operator's portfolio distributes
# across these, so the twin shows a complete value chain where gas is actually produced in Australia.
AU_BASINS = [
    ("Surat", -26.60, 150.10),          # QLD CSG (Wallumbilla hub)
    ("Bowen", -22.50, 148.50),          # QLD CSG/conventional
    ("Cooper-Eromanga", -27.70, 140.20),  # SA/QLD (Moomba hub)
    ("Carnarvon", -20.70, 116.80),      # WA North West Shelf
    ("Perth", -29.30, 115.05),          # WA (Waitsia/Dongara)
    ("Gippsland", -38.30, 147.60),      # VIC (Longford)
    ("Otway", -38.40, 142.40),          # VIC/SA
    ("Beetaloo", -16.80, 134.50),       # NT shale
    ("Amadeus", -24.00, 132.00),        # NT (Mereenie)
]


class _TagNum:
    """Per-area running 4-digit tag number, plausible-looking (starts near 1000)."""
    def __init__(self):
        self.c: dict[str, int] = {}

    def next(self, area: str) -> int:
        self.c[area] = self.c.get(area, 999) + 1
        return self.c[area]


def build_semantic_backbone(cfg: dict[str, Any]) -> dict[str, list[dict]]:
    """Return the six backbone tables as lists of dict rows, deterministic in cfg['seed']."""
    seed = int(cfg["seed"])
    field = cfg["field"]
    clat, clon, scatter = float(field["center_lat"]), float(field["center_lon"]), float(field["scatter_deg"])
    scheme = cfg["tag_scheme"]

    rg = _rng(seed, "geo")
    rw = _rng(seed, "well")
    ra = _rng(seed, "asset")
    rc = _rng(seed, "cross")
    tn = _TagNum()

    fl: list[dict] = []
    assets: list[dict] = []
    tags: list[dict] = []
    wells: list[dict] = []
    pvt: list[dict] = []
    crossmap: list[dict] = []

    field_code = field["name"][:3].upper()
    today = "2024-01-01"  # SCD2 effective-from anchor; the timeline shift handles "as of today"

    # ---- Site root -------------------------------------------------------------------------
    site_id = f"FL-{field_code}-SITE"
    fl.append(_fl(site_id, None, "Site", field_code, f"{field['name']} Gas Field", None, None, None, None, today))

    def add_asset(area, system, etype, cls, seq_hint, scene_id, lat, lon, crit=None):
        """Create one asset + its tags from SIGNAL_SPECS, wired to the functional-location tree."""
        n = tn.next(area)
        asset_id = f"{area}-{system}-{cls}-{n:04d}"
        crit = crit or ra.choice(CRITICALITY_TIERS)
        hx, hy = round(ra.uniform(0.08, 0.92), 4), round(ra.uniform(0.08, 0.92), 4)
        # functional-location node for the asset
        asset_fl = f"FL-{asset_id}"
        fl.append(_fl(asset_fl, f"FL-{area}-{system}", "Asset", cls, f"{etype.replace('_',' ')} {asset_id}",
                      area, system, cls, crit, today))
        assets.append({
            "asset_id": asset_id, "fl_id": asset_fl, "area_code": area, "system_code": system,
            "class_code": cls, "equipment_type": etype, "criticality": crit,
            "scene_id": scene_id, "hotspot_x": hx, "hotspot_y": hy,
            "gps_lat": lat, "gps_lon": lon,
            "manufacturer": ra.choice(["Solar", "Siemens", "GE", "Ariel", "Sulzer", "Flowserve", "Emerson", "Baker Hughes"]),
            "install_year": ra.randint(2008, 2022),
            "effective_from": today, "is_current": True,
        })
        for (signal, scls, unit, rmin, rmax, moc, cadence) in SIGNAL_SPECS.get(etype, []):
            tnum = tn.next(area)
            tag_id = scheme.format(area=area, system=system, **{"class": scls}, nnnn=tnum)
            tags.append({
                "tag_id": tag_id, "asset_id": asset_id, "fl_id": asset_fl,
                "area_code": area, "system_code": system, "class_code": scls,
                "signal": signal, "unit": unit,
                "eng_range_min": float(rmin), "eng_range_max": float(rmax),
                "description": f"{etype.replace('_',' ')} {signal.replace('_',' ')} ({area} {system})",
                "measured_or_calculated": moc, "cadence": cadence,
                "scene_id": scene_id, "hotspot_x": round(ra.uniform(0.05, 0.95), 4),
                "hotspot_y": round(ra.uniform(0.05, 0.95), 4),
                "gps_lat": lat, "gps_lon": lon,
            })
            # cross-system id map (historian tag <-> EAM fl <-> OSDU/doc), some deliberately stale
            crossmap.append({
                "entity": tag_id, "historian_tag": f"PI.{area}.{signal.upper().replace('_', '.')}.{tnum}",
                "eam_fl": asset_fl, "osdu_ref": None,
                "doc_entity": f"PID-{area}-{system}",
                "is_stale": rc.random() < 0.05,
            })
        return asset_id

    # ---- Area nodes + a helper to register a System node under an area ----------------------
    def add_area(area, name, lat, lon):
        fl.append(_fl(f"FL-{area}", site_id, "Area", area, name, area, None, None, None, today))

    def add_system(area, system):
        fl.append(_fl(f"FL-{area}-{system}", f"FL-{area}", "System", system,
                      cfg["systems"].get(system, {}).get("name", system), area, system, None, None, today))

    # =========================================================================================
    # Well pads + wells + wellheads + artificial lift
    # =========================================================================================
    wp = cfg["wellpads"]
    npads, prefix, wells_total = int(wp["count"]), wp["prefix"], int(wp["wells_total"])
    csg_frac, gl_frac = float(wp["csg_fraction"]), float(wp["gas_lift_fraction"])
    # distribute wells across pads roughly evenly
    per_pad = [wells_total // npads] * npads
    for i in range(wells_total - sum(per_pad)):
        per_pad[i % npads] += 1

    well_seq = 0
    for p in range(1, npads + 1):
        area = f"{prefix}{p:03d}"
        basin = AU_BASINS[(p - 1) % len(AU_BASINS)]        # distribute pads across real AU gas basins
        plat, plon = _scatter(rg, basin[1], basin[2], 0.30)
        add_area(area, f"Well pad {area} ({basin[0]} Basin)", plat, plon)
        add_system(area, "WB")
        add_system(area, "AL")
        scene_wh = f"scene_{area}_wh"
        for _ in range(per_pad[p - 1]):
            well_seq += 1
            well_id = f"{area}-W{well_seq:03d}"
            is_csg = rw.random() < csg_frac
            completion = "csg" if is_csg else "conventional"
            if is_csg:
                lift = "esp"
            elif rw.random() < gl_frac:
                lift = "gas_lift"
            else:
                lift = rw.choice(["natural", "plunger"])
            crit = rw.choices(CRITICALITY_TIERS, weights=[0.2, 0.5, 0.3])[0]
            wlat, wlon = _scatter(rg, plat, plon, 0.03)   # wells sit tight around their pad
            wh_asset = add_asset(area, "WB", "wellhead", "VE", well_seq, scene_wh, wlat, wlon, crit)
            if lift == "esp":
                add_asset(area, "AL", "esp", "PU", well_seq, scene_wh, wlat, wlon, crit)
            elif lift == "gas_lift":
                add_asset(area, "AL", "gas_lift", "VL", well_seq, scene_wh, wlat, wlon, crit)
            wells.append({
                "well_id": well_id, "pad_area": area, "fl_id": f"FL-{wh_asset}",
                "name": f"{field['name']} {well_id}", "spud_date": f"{rw.randint(2010, 2022)}-0{rw.randint(1,9)}-15",
                "td_m": rw.randint(900, 3200), "deviation_type": rw.choice(["vertical", "deviated", "horizontal"]),
                "completion_type": completion, "lift_type": lift, "criticality": crit,
                "status": "producing", "wellhead_asset_id": wh_asset,
                "effective_from": today, "is_current": True,
            })
            pvt.append({
                "well_id": well_id,
                "cgr_bbl_mmscf": round(rw.uniform(0.5, 15) if completion == "conventional" else rw.uniform(0.0, 1.0), 2),
                "wgr_bbl_mmscf": round(rw.uniform(2, 60) if completion == "csg" else rw.uniform(0.5, 10), 2),
                "gor_scf_bbl": rw.randint(5000, 90000), "gas_sg": round(rw.uniform(0.56, 0.72), 3),
                "co2_frac_molpct": round(rw.uniform(0.2, 7.0), 2), "n2_frac_molpct": round(rw.uniform(0.3, 3.0), 2),
                "h2s_ppm": rw.randint(0, 400), "pvt_source": "OSDU",
                "effective_from": today, "is_current": True,
            })

    # =========================================================================================
    # Gathering network
    # =========================================================================================
    g = cfg["gathering"]
    nhubs = max(1, int(g.get("hubs", 1)))
    nseg = int(g["segments"])
    # distribute segments across the hubs roughly evenly
    seg_per = [nseg // nhubs] * nhubs
    for i in range(nseg - sum(seg_per)):
        seg_per[i % nhubs] += 1
    for h in range(nhubs):
        garea = f"GTH{h + 1:03d}"                              # GTH001 .. GTH0{nhubs}
        basin = AU_BASINS[h % len(AU_BASINS)]                  # spread the hubs across real AU gas basins
        glat, glon = _scatter(rg, basin[1], basin[2], 0.2)
        add_area(garea, f"Gathering network {garea} ({basin[0]} Basin)", glat, glon)
        add_system(garea, "GL")
        scene_gth = f"scene_{garea}"
        for s in range(seg_per[h]):
            slat, slon = _scatter(rg, glat, glon, 0.12)
            add_asset(garea, "GL", "gathering_line", "VL", s, scene_gth, slat, slon)

    # =========================================================================================
    # CPF trains + shared process units
    # =========================================================================================
    cpf = cpf_cfg = cfg["cpf"]
    # processing trains spread across the basins: Train 1 in the Surat Basin, Train 2 in Cooper-Eromanga
    # (the flagship vibration story lives on CPF002), the remaining trains fan out over the other basins.
    train_basins = [AU_BASINS[0], AU_BASINS[2]] + [b for i, b in enumerate(AU_BASINS) if i not in (0, 2)]
    cpf_lat, cpf_lon = AU_BASINS[0][1], AU_BASINS[0][2]
    first_train_area = cpf["trains"][0]["area"]
    for ti, train in enumerate(cpf["trains"]):
        area = train["area"]
        tb = train_basins[ti % len(train_basins)]
        tlat, tlon = _scatter(rg, tb[1], tb[2], 0.06)
        add_area(area, f"CPF {train['name']} ({tb[0]} Basin)", tlat, tlon)
        scene_cpf = f"scene_{area}"
        # separation + compression per train
        add_system(area, "SEP")
        add_asset(area, "SEP", "separator", "VE", 0, scene_cpf, tlat, tlon)
        add_system(area, "CMP")
        for c in range(int(cpf["compressors_per_train"])):
            add_asset(area, "CMP", "compressor", "CP", c, scene_cpf, tlat, tlon)
    # shared process units under the first train area (single TEG/amine/NGL/utilities/flare)
    area = first_train_area
    slat, slon = _scatter(rg, cpf_lat, cpf_lon, scatter * 0.05)
    if cpf.get("has_teg"):
        add_system(area, "DEH"); add_asset(area, "DEH", "teg_dehydration", "CL", 0, f"scene_{area}", slat, slon)
    if cpf.get("has_amine"):
        add_system(area, "AMN"); add_asset(area, "AMN", "amine", "CL", 0, f"scene_{area}", slat, slon)
    if cpf.get("has_ngl"):
        add_system(area, "NGL"); add_asset(area, "NGL", "ngl_stab", "CL", 0, f"scene_{area}", slat, slon)
    if cpf.get("has_utilities"):
        add_system(area, "UTL"); add_asset(area, "UTL", "utilities", "GT", 0, f"scene_{area}", slat, slon)
    if cpf.get("has_flare"):
        add_system(area, "FLR")
        add_asset(area, "FLR", "flare", "VE", 0, f"scene_{area}", slat, slon)
        add_asset(area, "FLR", "fire_gas", "GD", 0, f"scene_{area}", slat, slon)

    # =========================================================================================
    # Custody / export metering stations
    # =========================================================================================
    for si, st in enumerate(cfg["custody"]["stations"]):
        area = st["area"]
        cb = train_basins[si % len(train_basins)]      # custody at the export hubs (Wallumbilla / Moomba)
        mlat, mlon = _scatter(rg, cb[1], cb[2], 0.08)
        add_area(area, f"{st['name']} ({cb[0]} Basin)", mlat, mlon)
        add_system(area, "MET")
        add_asset(area, "MET", "metering", "FE", 0, f"scene_MET", mlat, mlon, crit="A")

    return {
        "functional_location": fl,
        "asset_master": assets,
        "tag_registry": tags,
        "well_master": wells,
        "pvt": pvt,
        "id_crossmap": crossmap,
    }


def _fl(fl_id, parent, level, code, name, area, system, cls, crit, eff):
    return {
        "fl_id": fl_id, "parent_fl_id": parent, "level": level, "code": code, "name": name,
        "area_code": area, "system_code": system, "class_code": cls, "criticality": crit,
        "effective_from": eff, "is_current": True,
    }


if __name__ == "__main__":
    # local smoke test (no spark): print cardinality
    import json, os, sys
    cfgpath = sys.argv[1] if len(sys.argv) > 1 else os.path.join(os.path.dirname(__file__), "..", "config", "twin_config.json")
    with open(cfgpath) as f:
        cfg = json.load(f) if cfgpath.endswith(".json") else __import__("yaml").safe_load(f)
    t = build_semantic_backbone(cfg)
    for k, v in t.items():
        print(f"{k:22s} {len(v):6d} rows")
