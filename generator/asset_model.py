"""Phase 1: the semantic backbone (the CONTRACT).

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

# tags whose signal is really an event carrier (safety/fg): handled by the event emitter too.
CRITICALITY_TIERS = ["A", "B", "C"]


def _rng(seed: int, salt: str) -> random.Random:
    """Deterministic sub-stream keyed by a salt, so structures are independent but reproducible."""
    return random.Random(f"{seed}:{salt}")


def _scatter(rng: random.Random, lat: float, lon: float, deg: float) -> tuple[float, float]:
    return round(lat + rng.uniform(-deg, deg), 6), round(lon + rng.uniform(-deg, deg), 6)


def site_layout(cfg: dict[str, Any]) -> dict[str, Any]:
    """Where every site sits, from config: the producing basins (pads or offshore platforms, wells per pad, CSG
    share, hubs), the gas plants (each at its real town, in its basin) and the custody points (each at a real
    export location).

    Onshore pads (BAR001..) and offshore platforms (PLT001..) are numbered basin by basin in config order, as are
    onshore gathering hubs (GTH001..) and offshore manifolds (MNF001..). Each basin's hubs take its pads
    round-robin and feed that basin's plants round-robin (the nearest plant when the basin has none), so gas is
    always processed where it is produced. A hub sits at the centre of the pads it gathers. Deterministic, and
    shared by build_semantic_backbone and supply_network so the map and the network always agree."""
    seed = int(cfg["seed"])
    rg = _rng(seed, "layout")
    basins = {b["name"]: b for b in cfg["basins"]}
    prefix = cfg["wellpads"]["prefix"]
    pads, hubs, geo, offshore = [], [], {}, set()
    cnt = {"pad": 0, "plt": 0, "gth": 0, "mnf": 0}
    for b in cfg["basins"]:
        off = bool(b.get("offshore"))
        locs = b.get("locations") or []
        mine = []
        for k in range(int(b["pads"])):
            if off:
                cnt["plt"] += 1; a = f"PLT{cnt['plt']:03d}"
                la, lo = locs[k % len(locs)] if locs else (b["lat"], b["lon"])
                geo[a] = _scatter(rg, float(la), float(lo), 0.04)
                offshore.add(a)
            else:
                cnt["pad"] += 1; a = f"{prefix}{cnt['pad']:03d}"
                geo[a] = _scatter(rg, float(b["lat"]), float(b["lon"]), float(b.get("spread", 0.3)))
            pads.append((a, b["name"])); mine.append(a)
        nh = max(1, int(b.get("hubs", 1)))
        for k in range(nh):
            if off:
                cnt["mnf"] += 1; h = f"MNF{cnt['mnf']:03d}"; offshore.add(h)
            else:
                cnt["gth"] += 1; h = f"GTH{cnt['gth']:03d}"
            hubs.append((h, b["name"]))
            fed = mine[k::nh] or mine
            la = sum(geo[x][0] for x in fed) / len(fed); lo = sum(geo[x][1] for x in fed) / len(fed)
            geo[h] = _scatter(rg, la, lo, 0.03 if off else 0.08)
    trains = {t["area"]: t["basin"] for t in cfg["cpf"]["trains"]}
    for t in cfg["cpf"]["trains"]:
        geo[t["area"]] = (float(t["lat"]), float(t["lon"]))
    meters = {st["area"]: st for st in cfg["custody"]["stations"]}
    for m, st in meters.items():
        geo[m] = (float(st["lat"]), float(st["lon"]))
    hubs_in, trains_in = {}, {}
    for a, b in hubs:
        hubs_in.setdefault(b, []).append(a)
    for a, b in trains.items():
        trains_in.setdefault(b, []).append(a)
    seen, pad_hub = {}, {}
    for a, b in pads:
        hs = hubs_in[b]; pad_hub[a] = hs[seen.get(b, 0) % len(hs)]; seen[b] = seen.get(b, 0) + 1
    hub_train = {}
    for b, hs in hubs_in.items():
        for k, a in enumerate(hs):
            ts = trains_in.get(b) or [min(trains, key=lambda t: (geo[t][0] - geo[a][0]) ** 2 + (geo[t][1] - geo[a][1]) ** 2)]
            hub_train[a] = ts[k % len(ts)]
    return {"basins": basins, "pads": pads, "hubs": hubs, "trains": trains, "meters": meters,
            "pad_hub": pad_hub, "hub_train": hub_train, "geo": geo, "offshore": offshore}


def site_condition(cfg: dict[str, Any], area: str) -> str:
    """The site's overall condition (green, amber or red), drawn once per site from config odds (seeded) or
    pinned for the scenario story sites. Work orders, trips and equipment wear all follow it, so the App's
    site colours land in realistic proportions: mostly healthy, some needing attention, a few critical."""
    sc = cfg.get("site_condition") or {}
    pin = (sc.get("pinned") or {}).get(area)
    if pin:
        return pin
    odds = sc.get("odds") or {"green": 0.72, "amber": 0.19, "red": 0.09}
    h = _rng(int(cfg["seed"]), "cond:" + area).random()
    return "red" if h < odds.get("red", 0.09) else ("amber" if h < odds.get("red", 0.09) + odds.get("amber", 0.19) else "green")


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

    def add_asset(area, system, etype, cls, seq_hint, scene_id, lat, lon, crit=None, ranges=None):
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
            if ranges and signal in ranges:                 # sized to the gas this asset actually carries
                rmax = max(rmax, ranges[signal])
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
    # Well pads / offshore platforms + wells + wellheads + artificial lift
    # =========================================================================================
    wp = cfg["wellpads"]
    gl_frac = float(wp["gas_lift_fraction"])
    lay = site_layout(cfg)
    geo = lay["geo"]
    rn = _rng(seed, "wellcount")
    pot = {}      # well -> test potential (10^3 m^3/d), so the network nodes can size their flow ranges

    well_seq = 0
    for area, bname in lay["pads"]:
        basin = lay["basins"][bname]
        off = area in lay["offshore"]
        plat, plon = geo[area]
        add_area(area, f"{'Wellhead platform' if off else 'Well pad'} {area} ({bname} Basin)", plat, plon)
        lo, hi = basin["wells_per_pad"]
        csg_frac = float(basin["csg_fraction"])
        r_lo, r_hi = basin.get("well_rate", [40, 160])
        c_lo, c_hi = basin.get("co2_molpct", [0.2, 7.0])
        h_lo, h_hi = basin.get("h2s_ppm", [0, 400])
        d_lo, d_hi = basin.get("td_m", [900, 3200])
        n_wells = rn.randint(int(lo), int(hi))
        add_system(area, "WB")
        add_system(area, "AL")
        scene_wh = f"scene_{area}_wh"
        for _ in range(n_wells):
            well_seq += 1
            well_id = f"{area}-W{well_seq:03d}"
            is_csg = rw.random() < csg_frac
            completion = "csg" if is_csg else "conventional"
            if is_csg:
                lift = "esp"
            elif off:
                lift = "natural"                     # offshore gas wells flow on reservoir pressure
            elif rw.random() < gl_frac:
                lift = "gas_lift"
            else:
                lift = rw.choice(["natural", "plunger"])
            crit = rw.choices(CRITICALITY_TIERS, weights=[0.2, 0.5, 0.3])[0]
            if off:
                crit = rw.choices(CRITICALITY_TIERS, weights=[0.6, 0.35, 0.05])[0]   # big offshore wells: mostly A
            potential = round(float(r_lo) + (float(r_hi) - float(r_lo)) * rw.random(), 1)
            wlat, wlon = _scatter(rg, plat, plon, 0.004 if off else 0.03)   # offshore: one deck; onshore: around the pad
            wh_asset = add_asset(area, "WB", "wellhead", "VE", well_seq, scene_wh, wlat, wlon, crit,
                                 ranges={"production_flow": round(potential * 1.5, -1)})
            pot[wh_asset] = potential
            if lift == "esp":
                add_asset(area, "AL", "esp", "PU", well_seq, scene_wh, wlat, wlon, crit)
            elif lift == "gas_lift":
                add_asset(area, "AL", "gas_lift", "VL", well_seq, scene_wh, wlat, wlon, crit)
            wells.append({
                "well_id": well_id, "pad_area": area, "fl_id": f"FL-{wh_asset}",
                "name": f"{field['name']} {well_id}", "spud_date": f"{rw.randint(2006 if off else 2010, 2022)}-0{rw.randint(1,9)}-15",
                "td_m": rw.randint(int(d_lo), int(d_hi)),
                "deviation_type": rw.choice(["deviated", "deviated", "horizontal"]) if off else ("vertical" if is_csg and rw.random() < 0.7 else rw.choice(["vertical", "deviated", "horizontal"])),
                "completion_type": completion, "lift_type": lift, "criticality": crit,
                "status": "producing", "wellhead_asset_id": wh_asset, "potential_e3m3d": potential,
                "plant": lay["hub_train"][lay["pad_hub"][area]],   # the gas plant the well's gas goes to (its turnarounds shut it in)
                "effective_from": today, "is_current": True,
            })
            pvt.append({
                "well_id": well_id,
                "cgr_bbl_mmscf": round(rw.uniform(0.0, 0.5) if is_csg else (rw.uniform(8, 40) if off else rw.uniform(0.5, 15)), 2),
                "wgr_bbl_mmscf": round(rw.uniform(20, 120) if is_csg else rw.uniform(0.5, 10), 2),
                "gor_scf_bbl": rw.randint(5000, 90000), "gas_sg": round(rw.uniform(0.56, 0.60) if is_csg else rw.uniform(0.60, 0.75), 3),
                "co2_frac_molpct": round(rw.uniform(float(c_lo), float(c_hi)), 2), "n2_frac_molpct": round(rw.uniform(0.3, 3.0), 2),
                "h2s_ppm": rw.randint(int(h_lo), int(h_hi)), "pvt_source": "OSDU",
                "effective_from": today, "is_current": True,
            })

    # how much gas each site carries at its test potential: sizes the flow ranges of hubs, plants and meters
    pad_pot = {}
    for w in wells:
        pad_pot[w["pad_area"]] = pad_pot.get(w["pad_area"], 0.0) + w["potential_e3m3d"]
    hub_pot, train_pot, meter_pot = {}, {}, {}
    for pa, h in lay["pad_hub"].items():
        hub_pot[h] = hub_pot.get(h, 0.0) + pad_pot.get(pa, 0.0)
    for h, t in lay["hub_train"].items():
        train_pot[t] = train_pot.get(t, 0.0) + hub_pot.get(h, 0.0)
    split = (cfg.get("network") or {}).get("train_custody") or {}
    for t, v in train_pot.items():
        for m, sh in (split.get(t) or {}).items():
            meter_pot[m] = meter_pot.get(m, 0.0) + v * float(sh)

    # =========================================================================================
    # Gathering hubs (onshore) and offshore manifold platforms
    # =========================================================================================
    g = cfg["gathering"]
    s_lo, s_hi = g.get("segments_per_hub", [3, 6])
    rsg = _rng(seed, "segments")
    for garea, bname in lay["hubs"]:
        off = garea in lay["offshore"]
        glat, glon = geo[garea]
        add_area(garea, f"{'Offshore manifold' if off else 'Gathering hub'} {garea} ({bname} Basin)", glat, glon)
        add_system(garea, "GL")
        scene_gth = f"scene_{garea}"
        hp = hub_pot.get(garea, 0.0)
        for s_ in range(rsg.randint(int(s_lo), int(s_hi))):
            slat, slon = _scatter(rg, glat, glon, 0.004 if off else 0.12)
            add_asset(garea, "GL", "gathering_line", "VL", s_, scene_gth, slat, slon,
                      ranges={"line_flow": round(hp * 1.3, -1)})

    # =========================================================================================
    # Gas plants: inlet separation + compression, then the process units the plant's gas needs
    # =========================================================================================
    cpf = cfg["cpf"]
    UNIT = {"DEH": ("teg_dehydration", "CL"), "AMN": ("amine", "CL"), "NGL": ("ngl_stab", "CL"), "UTL": ("utilities", "GT")}
    for train in cpf["trains"]:
        area = train["area"]
        tlat, tlon = geo[area]
        tp = train_pot.get(area, 0.0)
        add_area(area, f"{train['name']} {area} ({train['place']}, {train['basin']} Basin)", tlat, tlon)
        scene_cpf = f"scene_{area}"
        add_system(area, "SEP")
        add_asset(area, "SEP", "separator", "VE", 0, scene_cpf, tlat, tlon)
        add_system(area, "CMP")
        nc = int(train.get("compressors", cpf.get("compressors_per_train", 3)))
        for c in range(nc):
            add_asset(area, "CMP", "compressor", "CP", c, scene_cpf, tlat, tlon,
                      ranges={"throughput": round(tp / nc * 1.4, -1), "co2_massrate": round(tp / nc * 2.4, -1)})
        for u in train.get("units", ["DEH", "AMN", "NGL", "UTL", "FLR"]):
            ulat, ulon = _scatter(rg, tlat, tlon, 0.01)
            add_system(area, u)
            if u == "FLR":
                add_asset(area, "FLR", "flare", "VE", 0, scene_cpf, ulat, ulon, ranges={"co2_massrate": round(tp * 0.6, -1)})
                add_asset(area, "FLR", "fire_gas", "GD", 0, scene_cpf, ulat, ulon)
            else:
                et, cl = UNIT[u]
                add_asset(area, u, et, cl, 0, scene_cpf, ulat, ulon,
                          ranges={"fuel_gas_flow": round(tp * 0.05, -1), "co2_massrate": round(tp * 0.9, -1)})

    # =========================================================================================
    # Custody / export metering stations (one site each, at its export point)
    # =========================================================================================
    for st in cfg["custody"]["stations"]:
        area = st["area"]
        mlat, mlon = _scatter(rg, *geo[area], 0.01)
        add_area(area, f"{st['name']} {area} ({st['place']}, {st['basin']} Basin)", mlat, mlon)
        add_system(area, "MET")
        add_asset(area, "MET", "metering", "FE", 0, f"scene_{area}", mlat, mlon, crit="A",
                  ranges={"meter_flow": round(meter_pot.get(area, 0.0) * 1.3, -1)})

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


# =============================================================================================
# Supply network: the physical gas path as directed edges, for impact analysis
# =============================================================================================


def _reservoir_id(basin: str) -> str:
    return "RES-" + basin.split("-")[0].upper()          # RES-SURAT, RES-COOPER, RES-CARNARVON ...


def supply_network(cfg: dict[str, Any], bb: dict[str, list[dict]]) -> list[dict]:
    """Directed edges reservoir -> well pad -> gathering hub -> processing train -> compressor ->
    custody meter -> customer, each with the share of the from-node's outflow that takes the edge.

    Placement comes from site_layout (the same layout build_semantic_backbone uses), so the network always
    agrees with the map: gas is gathered and processed in its own basin. Deterministic.
    """
    names = {r["code"]: r["name"] for r in bb["functional_location"] if r.get("level") == "Area"}
    lay = site_layout(cfg)
    formation = {b["name"]: b.get("reservoir", b["name"]) for b in cfg["basins"]}
    pad_basin, hub_basin, train_basin = dict(lay["pads"]), dict(lay["hubs"]), lay["trains"]
    trains = list(train_basin)
    meter_basin = {a: st.get("basin") for a, st in lay["meters"].items()}

    edges = []

    def edge(fid, fname, fstop, tid, tname, tstop, share, basin):
        edges.append({"from_id": fid, "from_name": fname, "from_stop": fstop, "to_id": tid, "to_name": tname,
                      "to_stop": tstop, "share": round(float(share), 6) if share is not None else None, "basin": basin})

    # reservoir -> pad (the pad's own production decides the split, so share is flow-determined: null)
    # pad -> hub: every pad ties into one hub in its basin (round-robin when the basin has several)
    for pad, b in sorted(pad_basin.items()):
        rid = _reservoir_id(b)
        edge(rid, f"{formation.get(b, b)} ({b} Basin)", "reservoir", pad, names.get(pad, pad), "wells", None, b)
        hub = lay["pad_hub"][pad]
        edge(pad, names.get(pad, pad), "wells", hub, names.get(hub, hub), "gathering", 1.0, b)
    # hub -> train: a train in the hub's own basin (round-robin when the basin has several)
    for hub, b in sorted(hub_basin.items()):
        train = lay["hub_train"][hub]
        edge(hub, names.get(hub, hub), "gathering", train, names.get(train, train), "processing", 1.0, b)
    # train -> its compressors (parallel units share the train's throughput equally)
    comps = {}
    for a in bb["asset_master"]:
        if a["equipment_type"] == "compressor":
            comps.setdefault(a["area_code"], []).append(a["asset_id"])
    split = (cfg.get("network") or {}).get("train_custody") or {}
    for train in trains:
        units = sorted(comps.get(train, []))
        mshare = split.get(train) or {next(m for m, b in meter_basin.items() if b == train_basin[train]): 1.0}
        for u in units:
            edge(train, names.get(train, train), "processing", u, f"{u} compressor", "compression", 1.0 / len(units), train_basin[train])
            for m, s in sorted(mshare.items()):
                edge(u, f"{u} compressor", "compression", m, names.get(m, m), "custody", s, train_basin[train])
    # meter -> customer: each customer takes its weight of the delivery point's sales gas
    for i, c in enumerate(cfg.get("customers") or []):
        m = c["delivery_point"]
        edge(m, names.get(m, m), "custody", customer_id(i), c["name"], "customer", c["weight"], meter_basin.get(m))
    return edges


def customer_id(i: int) -> str:
    return f"CUS-{i + 1:03d}"


def pad_train(edges: list[dict]) -> dict[str, str]:
    """pad -> processing train it ultimately feeds (via its gathering hub)."""
    hub_of = {e["from_id"]: e["to_id"] for e in edges if e["from_stop"] == "wells"}
    train_of = {e["from_id"]: e["to_id"] for e in edges if e["from_stop"] == "gathering"}
    return {p: train_of[h] for p, h in hub_of.items()}
