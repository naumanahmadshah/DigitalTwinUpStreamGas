"""Layer 2: the state core (physics and behaviour), Data-gen 9.2.

One shared, deterministic model drives every emitter, so telemetry, allocation, custody, deferment,
emissions and economics reconcile by construction (9.7): a reservoir pressure drop shows up on the
tag, in the deferment, and in the allocation at the same time. Pure stdlib + math (serverless-safe).

Fidelity is "reduced but real" (9.10): reservoir material-balance decline, per-well nodal-style
deliverability with liquid loading, compression with efficiency/vibration degradation, dehydration
dewpoint and amine spec drift, emissions, and economics. Everything is a function of one field
clock + the seeded reference world + the scenario mods, so the whole field moves as a system.
"""
from __future__ import annotations
import hashlib
import math


def _h(*parts) -> float:
    """Deterministic 0..1 hash from string parts (stable per-entity jitter, seed-independent noise base)."""
    s = "|".join(str(p) for p in parts)
    return int(hashlib.md5(s.encode()).hexdigest()[:8], 16) / 0xFFFFFFFF


def ambient_c(t, seed: int) -> float:
    """Surat-basin-ish ambient temperature: seasonal + diurnal, degC. Drives energy/dewpoint/flare."""
    doy = t.timetuple().tm_yday
    hod = t.hour + t.minute / 60.0
    seasonal = 22 + 8 * math.cos(2 * math.pi * (doy - 15) / 365.0)  # summer peak ~Jan (S-hemisphere)
    diurnal = 6 * math.sin(2 * math.pi * (hod - 9) / 24.0)
    return round(seasonal + diurnal + (_h(seed, "amb", t.date()) - 0.5) * 2.0, 2)


def reservoir_frac(area: str, day_frac: float, base_decline: float = 0.18) -> float:
    """Fraction of initial reservoir energy remaining (1.0 -> about 1-base_decline over the window).
    Depletion is a curve, not a straight line: each area declines at its own rate with its own curvature
    (faster early in some, steadier in others), plus slow swings from aquifer support and interference
    with neighbouring wells. day_frac is 0..1 across the backfill."""
    jitter = 0.6 + 0.8 * _h(area, "decline")      # 0.6..1.4 x the base rate
    k = 0.3 + 2.4 * _h(area, "curve")             # curvature of the decline
    shape = (1 - math.exp(-k * day_frac)) / (1 - math.exp(-k))
    swing = 0.012 * math.sin(2 * math.pi * (day_frac * 4.7 + _h(area, "sw1"))) + 0.007 * math.sin(2 * math.pi * (day_frac * 11.3 + _h(area, "sw2")))
    return max(0.05, 1.0 - base_decline * jitter * shape + swing)


def plant_turnaround(plant: str, day) -> bool:
    """Planned maintenance turnaround: each gas plant shuts down for 3 to 6 days once a year (seeded per
    plant and year); every pad feeding it is shut in. Planned, so it is in the nomination too."""
    if not plant or day is None:
        return False
    start = 20 + int(320 * _h(plant, "ta", day.year))
    length = 3 + int(4 * _h(plant, "talen", day.year))
    doy = day.timetuple().tm_yday
    return start <= doy < start + length


def well_trip(well_id: str, day) -> bool:
    """Unplanned well trip: a few short outages a year per well (1 to 2 days, seeded), back on line after."""
    if day is None:
        return False
    import datetime as _dt
    return _h(well_id, "trip", day) < 0.006 or _h(well_id, "trip", day - _dt.timedelta(days=1)) < 0.003


def ops_factor(well: dict, day) -> float:
    """Day-to-day operating swing on a well's deliverability: slow pad-level variation (choke changes,
    water handling, backpressure from the gathering system) and a summer derate on compression capacity."""
    if day is None:
        return 1.0
    doy = day.timetuple().tm_yday
    pad = well.get("pad_area", "")
    swing = 1 + 0.035 * math.sin(2 * math.pi * (doy / 17.0 + _h(pad, "o1"))) + 0.025 * math.sin(2 * math.pi * (doy / 43.0 + _h(pad, "o2")))
    summer = 1 - 0.03 * max(0.0, math.cos(2 * math.pi * (doy - 15) / 365.0))   # hot Dec-Feb days cut compressor capacity
    return swing * summer


def well_rate_e3m3d(well: dict, res_frac: float, mods: dict, day=None, planned_only: bool = False) -> float:
    """Per-well daily gas rate (10^3 m^3/d). Nodal-style: potential scaled by reservoir energy, lift,
    completion, and scenario mods (deferment/liquid-loading cut it). With a day: the daily operating swing,
    the plant's planned turnaround (rate 0) and, unless planned_only, unplanned well trips (rate 0)."""
    if day is not None and (plant_turnaround(well.get("plant"), day) or (not planned_only and well_trip(well["well_id"], day))):
        return 0.0
    # the well's test potential, set per basin in config (Surat/Bowen CSG ~0.5-1.6 MMscf/d .. offshore ~25 MMscf/d)
    if well.get("potential_e3m3d"):
        potential = float(well["potential_e3m3d"])
    elif well["completion_type"] == "csg":
        potential = 15 + 30 * _h(well["well_id"], "pot")      # 15..45 e3m3/d (lower-rate, higher-count)
    else:
        potential = 40 + 120 * _h(well["well_id"], "pot")     # 40..160 e3m3/d
    lift_gain = {"esp": 1.15, "gas_lift": 1.1, "plunger": 0.9, "natural": 1.0}.get(well["lift_type"], 1.0)
    rate = potential * lift_gain * (0.35 + 0.65 * res_frac)   # deliverability tracks reservoir energy
    rate *= mods.get("prod_mult", 1.0)                        # deferment scenario
    rate *= ops_factor(well, day)                             # day-to-day operating swing
    if mods.get("liquid_loading", 0) > 0:
        rate *= (1.0 - 0.25 * mods["liquid_loading"])         # liquid loading chokes the well (believable derate)
    return max(0.0, round(rate, 2))


def _whp(wctx, tag):
    """Flowing wellhead pressure range (kPa) for the well's kind: (at a depleted reservoir, at full energy)."""
    w = (wctx or {}).get("well") or {}
    if w.get("completion_type") == "csg":
        base = 850 + 600 * _h(tag["tag_id"], "whp"); return base * 0.6, base * 1.25
    if str(w.get("pad_area", "")).startswith("PLT"):
        base = 11000 + 5000 * _h(tag["tag_id"], "whp"); return base * 0.6, base * 1.1
    base = 4000 + 4000 * _h(tag["tag_id"], "whp"); return base * 0.55, base * 1.15


def _scale(frac: float, lo: float, hi: float) -> float:
    return lo + (hi - lo) * max(0.0, min(1.0, frac))


def signal_value(sig: str, tag: dict, asset: dict, wctx: dict | None, ctx: dict, mods: dict, t, seed):
    """Return (value, quality) for one tag at time t. `ctx` carries day_frac, ambient_c, res_frac,
    degradation (0..1 for this asset). `mods` are the scenario perturbations for this asset/area."""
    rmin, rmax = float(tag["eng_range_min"]), float(tag["eng_range_max"])
    mid = (rmin + rmax) / 2.0
    noise = (_h(seed, tag["tag_id"], t) - 0.5)
    res = ctx["res_frac"]
    deg = ctx.get("degradation", 0.0)
    amb = ctx["ambient_c"]
    v = mid  # generic fallback: mid-range + light noise

    # ---- wells & lift -------------------------------------------------------------------
    if sig == "production_flow":
        v = wctx["rate"] if wctx else _scale(res, rmin, rmax)
    elif sig == "tubing_pressure":
        # flowing tubing-head pressure follows the reservoir: CSG wells run at a few hundred kPa to 1.5 MPa,
        # onshore conventional wells at 4-9 MPa, big offshore wells at 11-18 MPa. Liquid loading backs it up
        # (liquid column); a shut-in well builds up towards its static reservoir pressure.
        lo, hi = _whp(wctx, tag)
        v = (lo + (hi - lo) * res) * (1 + 0.12 * mods.get("liquid_loading", 0)) * (1 + 0.02 * math.sin(t.toordinal() / 6.0 + _h(tag["tag_id"], "tp") * 6))
        if wctx and wctx.get("shut"):
            v = hi * (0.95 + 0.25 * res)
    elif sig in ("casing_pressure_a", "casing_pressure_b"):
        lo, hi = _whp(wctx, tag)
        v = (lo + (hi - lo) * res) * (0.85 if sig.endswith("a") else 0.6) + mods.get("annulus_kick", 0) * 2000
    elif sig == "choke_position":
        v = 0 if (wctx and wctx.get("shut")) else 62 + 20 * _h(tag["tag_id"], "choke") - 45 * mods.get("liquid_loading", 0) + 4 * math.sin(t.toordinal() / 9.0 + _h(tag["tag_id"], "ch") * 6)
    elif sig == "wellhead_temp":
        v = 45 + 0.5 * amb + 8 * _h(tag["tag_id"], "wht")
    elif sig == "sssv_status":
        v = 100 if _h(tag["tag_id"], "sssv") > 0.06 else 0   # a few wells flag SSSV not-passed
    elif sig.startswith("esp_"):
        base = {"esp_frequency": 52, "esp_intake_press": 4200, "esp_motor_temp": 95,
                "esp_current": 90, "esp_vibration": 3.5}.get(sig, mid)
        v = base * (1 + 0.25 * deg if "vibration" in sig or "temp" in sig else 1.0)
    elif sig.startswith("gl_"):
        v = _scale(0.5 + 0.4 * _h(tag["tag_id"], sig), rmin, rmax)

    # ---- gathering ----------------------------------------------------------------------
    elif sig == "line_pressure":
        v = _scale(res, rmax * 0.4, rmax * 0.75) + mods.get("backpressure", 0) * 1500
    elif sig == "line_flow":
        v = ctx["flow"] if ctx.get("flow") is not None else _scale(res, rmax * 0.25, rmax * 0.7) * mods.get("prod_mult", 1.0)
    elif sig == "line_temp":
        # Gathering gas enters warm and cools toward ambient with distance from the inlet. The per-segment
        # offset (4..15 degC above ambient) is stable per line; short near-inlet trunks stay warm, long
        # downstream legs run close to ambient. In deep winter the coolest legs approach the gas-hydrate
        # formation temperature (~19-20 degC at line pressure), so a few segments show a tight approach
        # margin seasonally: a believable flow-assurance watch, not a fabricated crisis.
        v = amb + 4 + 11 * _h(tag["tag_id"], "lt")
    elif sig == "esd_status":
        v = 0 if mods.get("esd_trip", 0) else 100

    # ---- separation ---------------------------------------------------------------------
    elif sig == "sep_level":
        v = 48 + 10 * math.sin(t.hour / 3.8) + 12 * noise
    elif sig == "sep_pressure":
        v = _scale(res, rmax * 0.5, rmax * 0.85)
    elif sig in ("sep_temp",):
        v = amb + 18 + 5 * noise
    elif sig in ("sep_interface", "sep_dp"):
        v = _scale(0.4 + 0.3 * _h(tag["tag_id"], sig), rmin, rmax)

    # ---- compression (the vibration->constraint story) ---------------------------------
    elif sig == "vibration_overall":
        v = 2.4 + 1.2 * _h(tag["tag_id"], "vib") + 16.0 * deg     # climbs sharply with degradation
    elif sig == "bearing_temp":
        v = 62 + 0.3 * amb + 55 * deg
    elif sig == "seal_temp":
        v = 70 + 50 * deg
    elif sig == "polytropic_eff":
        v = 84 - 30 * deg - 3 * noise                            # efficiency falls as it degrades
    elif sig == "throughput":
        base = ctx["flow"] if ctx.get("flow") is not None else _scale(res, rmax * 0.45, rmax * 0.8)
        v = base * (1 - 0.4 * deg) * mods.get("prod_mult", 1.0)
    elif sig == "discharge_pressure":
        v = _scale(0.55 + 0.2 * res, rmin, rmax) * (1 + 0.06 * deg)
    elif sig == "suction_pressure":
        v = _scale(res, rmin, rmax * 0.7)
    elif sig in ("discharge_temp",):
        v = 120 + 0.4 * amb + 45 * deg
    elif sig in ("suction_temp",):
        v = amb + 10 + 4 * noise
    elif sig == "speed":
        v = _scale(0.55 + 0.3 * _h(tag["tag_id"], "spd"), rmin, rmax)
    elif sig == "power":
        v = _scale(0.5 + 0.35 * res, rmin, rmax) * (1 + 0.1 * deg)
    elif sig == "antisurge_valve":
        v = 12 + 55 * deg + 10 * noise                            # opens as surge margin shrinks

    # ---- dehydration / amine (spec drift story) ----------------------------------------
    # Treated-gas quality carries natural day-to-day movement (ambient-driven glycol/amine performance
    # + process noise) around a safe baseline; the TEG/amine degradation scenario then drifts it toward
    # the contractual spec. The baselines are NOT flat lines (real analyser trends always wiggle).
    elif sig == "water_dewpoint":
        # degC; -14 baseline, warmer ambient slightly worsens dewpoint; drift pushes toward the -10 spec
        v = -14 + 0.12 * (amb - 22) + 2.2 * noise + 22 * mods.get("dewpoint_drift", 0.0)
    elif sig == "reboiler_temp":
        v = 195 - 15 * mods.get("dewpoint_drift", 0.0) + 2.5 * noise
    elif sig == "h2s_outlet":
        v = 1.4 + 0.35 * noise + 0.05 * (amb - 22) + 8 * mods.get("dewpoint_drift", 0.0)   # ppm; approaches 4ppm spec
    elif sig == "co2_outlet":
        v = 1.2 + 0.14 * noise + 3 * mods.get("dewpoint_drift", 0.0)                        # mol%; approaches 3% spec
    elif sig == "foaming_index":
        v = 18 + 55 * mods.get("dewpoint_drift", 0.0)
    elif sig in ("contactor_pressure", "contactor_temp", "glycol_flow", "amine_circ", "reboiler_duty"):
        v = _scale(0.45 + 0.2 * res, rmin, rmax)

    # ---- NGL / stabilization -----------------------------------------------------------
    elif sig == "ngl_recovery":
        v = 88 - 10 * deg - 2 * noise
    elif sig == "product_rvp":
        v = 70 + 20 * noise
    elif sig in ("column_temp", "jt_pressure", "expander_speed"):
        v = _scale(0.5 + 0.2 * res, rmin, rmax)

    # ---- utilities ---------------------------------------------------------------------
    elif sig == "turbine_load":
        v = _scale(0.5 + 0.3 * res, rmin, rmax)
    elif sig == "fuel_gas_flow":
        # own-use fuel gas: about 2.7% of what the plant processes (compression drivers + power)
        v = ctx["flow"] * (0.025 + 0.004 * _h(tag["tag_id"], "fuel")) if ctx.get("flow") is not None else _scale(0.3 + 0.3 * res, rmin, rmax)
    elif sig == "instrument_air":
        v = _scale(0.7, rmin, rmax)

    # ---- flare / emissions -------------------------------------------------------------
    elif sig == "flare_flow":
        v = 5 + 8 * _h(tag["tag_id"], "flr") + 400 * mods.get("emissions", 0.0)
    elif sig == "combustion_temp":
        v = 850 + 120 * mods.get("emissions", 0.0)
    elif sig == "ch4_massrate":
        # Methane comes from real, distinct sources (not just the flare): compressor rod-packing/seal
        # vents (and worse as the unit degrades), TEG still-column vents, and flare combustion slip.
        et = asset.get("equipment_type")
        if et == "compressor":
            v = 2.5 + 2 * _h(tag["tag_id"], "ch4") + 4 * deg      # rod-packing/seal, a few kg/h, worse as it degrades
        elif et == "teg_dehydration":
            v = 3 + 2 * _h(tag["tag_id"], "ch4")                   # still-column vent
        else:
            v = 1 + 1 * _h(tag["tag_id"], "ch4") + 60 * mods.get("emissions", 0.0)   # flare slip + episode
    elif sig == "co2_massrate":
        # Stationary-combustion CO2 split across its real sources: compression fuel gas (the dominant
        # driver across the trains), utility turbines/gensets, and flaring. The total across sources is
        # sized so field carbon intensity lands ~55-65 kg CO2 / 10^3 m^3, a believable upstream figure.
        # Each source burns a share of the gas it handles (about 1.9 t CO2 per 10^3 m^3 burnt): compressor
        # drivers ~2% of their throughput, utility turbines ~0.7% and flaring ~0.4% of the plant's gas.
        et = asset.get("equipment_type")
        f = ctx.get("flow")
        if et == "compressor":
            v = (f * 1.58 if f is not None else 1000 + 400 * res) * (0.95 + 0.1 * _h(tag["tag_id"], "co2"))
        elif et == "utilities":
            v = f * 0.55 if f is not None else 7000 + 2500 * res
        else:
            v = (f * 0.32 if f is not None else 4500 + 1500 * res) + 9000 * mods.get("emissions", 0.0)
    elif sig == "nox_massrate":
        v = 8 + 6 * _h(tag["tag_id"], "nox") + 150 * mods.get("emissions", 0.0)
    elif sig == "gas_detection":
        v = 100 * mods.get("gas_release", 0.0) if mods.get("gas_release", 0) else 0.3 + 0.5 * (noise + 0.5)

    # ---- custody / metering ------------------------------------------------------------
    elif sig == "meter_flow":
        # the sales gas reaching this meter (the plants' output routed here), less any unaccounted-for gas
        base = ctx["flow"] if ctx.get("flow") is not None else _scale(res, rmax * 0.3, rmax * 0.75) * mods.get("prod_mult", 1.0)
        v = base * (1 + 0.012 * noise) * (1 - mods.get("uaf", 0.0))
    elif sig == "meter_pressure":
        v = _scale(0.6, rmin, rmax)
    elif sig == "meter_temp":
        v = amb + 6 + 3 * noise
    elif sig == "ghv":
        v = 38.5 + 1.5 * _h(asset["asset_id"], "ghv")
    elif sig == "wobbe":
        v = 48 + 2 * _h(asset["asset_id"], "wob")
    elif sig == "gc_c1":
        v = 90 + 5 * _h(asset["asset_id"], "c1") - 2 * noise
    elif sig == "gc_co2":
        v = 1.5 + 5 * _h(asset["asset_id"], "co2")
    elif sig == "gc_n2":
        v = 1.0 + 3 * _h(asset["asset_id"], "n2")

    # generic small noise + clamp to engineering range (flow signals sized from the real gas path get noise
    # relative to their own flow, so a small meter is not swamped by a big range)
    if (ctx.get("flow") is not None and sig in ("line_flow", "throughput", "fuel_gas_flow", "meter_flow", "co2_massrate")) \
            or sig in ("tubing_pressure", "casing_pressure_a", "casing_pressure_b", "production_flow"):
        v = v * (1 + 0.004 * noise)
    else:
        v = v + (rmax - rmin) * 0.01 * noise
    v = max(rmin, min(rmax, v))
    return round(v, 3)
