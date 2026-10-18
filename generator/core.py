"""Layer 3 orchestration: scenarios, per-tick context, and the telemetry/event emitters.

Because one field state (physics.py) drives every emitter, a single injected scenario propagates to
every downstream signal, event, allocation line and dollar at once (Data-gen 9.7). This is what makes
the research doc's headline true: the contextualized tag is the star, and only the lakehouse holds
every consequence against the same tag.
"""
from __future__ import annotations
import datetime as dt
import random
import physics


class Scenarios:
    """Parses cfg['scenarios'] into ramped perturbation windows near the end of the timeline, so a
    twin generated 'as of today' opens with the stories live. Each scenario ramps 0->1 over its window."""

    def __init__(self, cfg: dict, start: dt.datetime, end: dt.datetime):
        self.items = []
        total = (end - start).days or 1
        # stagger each enabled scenario's ramp window across the final ~120 days
        enabled = [s for s in cfg.get("scenarios", []) if s.get("enabled") and s.get("id") != "baseline"]
        for i, s in enumerate(enabled):
            # windows: start between 120 and 30 days before end, each ~45-70 days long, staggered
            lead = 120 - int(i * (90 / max(1, len(enabled))))
            w_start = end - dt.timedelta(days=lead)
            w_len = 45 + (i % 3) * 12
            self.items.append({
                "id": s["id"], "target": s.get("target"), "desc": s.get("desc", ""),
                "start": w_start, "full": w_start + dt.timedelta(days=w_len),
            })

    def _ramp(self, it, t) -> float:
        if t < it["start"]:
            return 0.0
        if t >= it["full"]:
            return 1.0
        return (t - it["start"]).total_seconds() / (it["full"] - it["start"]).total_seconds()

    def _matches(self, target, asset: dict, well: dict | None) -> bool:
        if not target:
            return False
        aid = asset["asset_id"]
        if target == aid:
            return True
        # area / area-system prefix (e.g. BAR007, CPF001, MET002, CPF001-FLR)
        if aid.startswith(target):
            return True
        if asset["area_code"] == target:
            return True
        if well and well.get("pad_area") == target:
            return True
        return False

    def mods_for(self, asset: dict, well: dict | None, t) -> dict:
        """Accumulate scenario mods active for this asset at time t."""
        mods: dict = {}
        for it in self.items:
            if not self._matches(it["target"], asset, well):
                continue
            f = self._ramp(it, t)
            if f <= 0:
                continue
            sid = it["id"]
            if sid == "reservoir_depletion":
                mods["prod_mult"] = min(mods.get("prod_mult", 1.0), 1.0 - 0.18 * f)
            elif sid == "liquid_loading_onset":
                mods["liquid_loading"] = max(mods.get("liquid_loading", 0.0), f)
            elif sid == "compressor_vibration":
                mods["degradation"] = max(mods.get("degradation", 0.0), f)
            elif sid == "dewpoint_drift" and asset["system_code"] in ("DEH", "AMN"):
                mods["dewpoint_drift"] = max(mods.get("dewpoint_drift", 0.0), f)
            elif sid == "emissions_event" and asset["system_code"] == "FLR":
                mods["emissions"] = max(mods.get("emissions", 0.0), f)
                if asset["equipment_type"] == "fire_gas" and 0.4 < f < 0.6:
                    mods["gas_release"] = 1.0
            elif sid == "deferment_event":
                mods["prod_mult"] = min(mods.get("prod_mult", 1.0), 1.0 - 0.30 * f)
            elif sid == "unaccounted_gas" and asset["system_code"] == "MET":
                mods["uaf"] = max(mods.get("uaf", 0.0), 0.06 * f)
        return mods

    def active_targets(self, t):
        """(id, target, ramp) for scenarios active at t: used by batch_sources to reconcile marts."""
        out = []
        for it in self.items:
            f = self._ramp(it, t)
            if f > 0:
                out.append((it["id"], it["target"], round(f, 3)))
        return out


class DataQuality:
    """Injects the OT/enterprise imperfections real data carries (Data-gen 9.6). Bronze keeps them
    verbatim; Silver's cleansing story depends on them being present."""

    def __init__(self, cfg: dict, seed: int):
        gen = cfg.get("generation", {})
        self.bad_frac = float(gen.get("bad_quality_fraction", 0.02))
        self.rng = random.Random(f"{seed}:dq")

    def apply(self, row: dict) -> dict:
        r = self.rng.random()
        if r < self.bad_frac:
            row["quality"] = self.rng.choice(["uncertain", "bad", "sensor_out", "manual_override"])
            if row["quality"] in ("bad", "sensor_out"):
                row["value"] = None
        # deliberate unit inconsistency on a slice of pressure tags (Silver normalizes bar->kPa)
        if row.get("unit") == "kPa" and self.rng.random() < 0.015 and row["value"] is not None:
            row["value"] = round(row["value"] / 100.0, 4)
            row["unit"] = "bar"
        # clock skew: source_time drifts from event_time
        skew = self.rng.uniform(-3, 8)
        row["source_time"] = row["event_time"] + dt.timedelta(seconds=skew)
        return row


def _source_system(asset: dict, sig: str) -> str:
    if sig in ("vibration_overall", "esp_vibration"):
        return "vibration_mon"
    if asset["system_code"] == "MET":
        return "custody_fc"
    if sig in ("ch4_massrate", "co2_massrate", "nox_massrate", "flare_flow", "combustion_temp"):
        return "cems"
    return "scada_dcs"


def emit_tick(backbone_idx, scenarios: Scenarios, dq: DataQuality, t, seq, day_frac, seed,
              measured_only=True):
    """Emit one telemetry row per (measured) tag at time t. backbone_idx bundles the lookups."""
    assets = backbone_idx["assets_by_id"]
    tags_by_asset = backbone_idx["tags_by_asset"]
    wells_by_wh = backbone_idx["wells_by_wh"]
    rows = []
    for asset_id, asset in assets.items():
        area = asset["area_code"]
        well = wells_by_wh.get(asset_id)
        mods = scenarios.mods_for(asset, well, t)
        res = physics.reservoir_frac(area, day_frac)
        amb = physics.ambient_c(t, seed)
        # per-asset degradation baseline (age + slow drift), scenario can raise it
        age = max(0, 2024 - int(asset.get("install_year") or 2015))
        deg = min(0.92, 0.010 * age + 0.04 * day_frac * physics._h(asset_id, "wear"))
        deg = max(deg, mods.get("degradation", 0.0))
        wctx = None
        if well is not None:
            wctx = {"rate": physics.well_rate_e3m3d(well, res, mods)}
        ctx = {"day_frac": day_frac, "ambient_c": amb, "res_frac": res, "degradation": deg}
        for tag in tags_by_asset.get(asset_id, []):
            if measured_only and tag["measured_or_calculated"] != "measured" and tag["cadence"] not in ("NRT",):
                # still emit calculated NRT signals (dewpoint, eff) daily; skip pure high-freq calc otherwise
                pass
            val = physics.signal_value(tag["signal"], tag, asset, wctx, ctx, mods, t, seed)
            k = tag["tag_id"]
            seq[k] = seq.get(k, 0) + 1
            src = _source_system(asset, tag["signal"])
            row = {
                "source_system": src, "tag_id": k, "asset_id": asset_id, "signal": tag["signal"],
                "event_time": t, "source_time": t, "value": val, "unit": tag["unit"],
                "quality": "good", "seq": seq[k],
                "velocity_rms": None, "accel_peak": None, "band_1x": None,
            }
            if src == "vibration_mon" and val is not None:
                row["velocity_rms"] = round(val, 3)
                row["accel_peak"] = round(val * 0.4 + 0.5, 3)
                row["band_1x"] = round(val * 0.6, 3)
            rows.append(dq.apply(row))
    return rows


def events_at(backbone_idx, scenarios: Scenarios, t, seq, seed):
    """Discrete safety/alarm/emission events (Data-gen 9.7).

    A believable *background* of nuisance process alarms every day (standing/chattering alarms are a
    fact of life in a real control room), plus rarer F&G detections and protective ESD trips, plus the
    scenario-driven criticals. Deterministic (seeded hash), so it reconciles across a re-run."""
    rows = []
    assets = backbone_idx["assets_by_id"]
    by_area = backbone_idx.get("assets_by_area")
    if by_area is None:
        by_area = {}
        for aid, a in assets.items():
            by_area.setdefault(a["area_code"], []).append(a)
        backbone_idx["assets_by_area"] = by_area
    day = t.date()
    for area, alist in by_area.items():
        if not alist:
            continue
        # background nuisance alarms: ~3-12 per area per day
        n = 3 + int(9 * physics._h(seed, "alarmrate", area, day))
        for j in range(n):
            a = alist[int(physics._h(seed, "alarmasset", area, day, j) * len(alist)) % len(alist)]
            hv = physics._h(seed, "alarmsev", area, day, j)
            sev = "critical" if hv > 0.97 else ("warning" if hv > 0.6 else "advisory")
            state = "bypassed" if physics._h(seed, "alarmstate", area, day, j) > 0.95 else "new"
            rows.append(_evt("dcs_alarm", a, "alarm", sev, state, t, seq, {"reason": "process alarm"}))
        # occasional fire & gas detector activation
        if physics._h(seed, "fg", area, day) > 0.985:
            rows.append(_evt("fire_gas", alist[0], "fg_detection", "warning", "new", t, seq,
                             {"reason": "F&G detector activation"}))
        # rare protective ESD trip (emergency shutdowns are serious + infrequent)
        if physics._h(seed, "esd", area, day) > 0.9985:
            rows.append(_evt("sis_esd", alist[0], "esd_trip", "critical", "tripped", t, seq,
                             {"reason": "protective trip"}))
    for (sid, target, f) in scenarios.active_targets(t):
        # fire an event at the mid-ramp crossing of certain scenarios (once, deterministically)
        if sid == "emissions_event" and 0.48 <= f <= 0.52:
            a = _first(assets, target)
            if a:
                rows.append(_evt("cems", a, "emission_exceedance", "alarm", "new", t, seq,
                                 {"pollutant": "CH4", "reason": "flare/vent episode + fugitive plume"}))
        if sid == "compressor_vibration" and 0.60 <= f <= 0.64:
            a = assets.get(target)
            if a:
                rows.append(_evt("dcs_alarm", a, "alarm", "advisory", "new", t, seq,
                                 {"tag": target, "reason": "high vibration - trending to constraint"}))
        if sid == "deferment_event" and 0.30 <= f <= 0.34:
            a = _first(assets, target)
            if a:
                rows.append(_evt("sis_esd", a, "esd_trip", "critical", "tripped", t, seq,
                                 {"area": target, "reason": "unplanned shutdown - production deferred"}))
    return rows


def _first(assets, target):
    if target in assets:
        return assets[target]
    for aid, a in assets.items():
        if target and aid.startswith(target):
            return a
    return None


def _evt(src, asset, etype, sev, state, t, seq, attrs):
    seq["_evt"] = seq.get("_evt", 0) + 1
    n = seq["_evt"]
    return {
        "source_system": src, "event_id": f"EVT-{n:08d}", "asset_id": asset["asset_id"],
        "tag_id": None, "area_code": asset["area_code"], "event_type": etype, "severity": sev,
        "state": state, "event_time": t, "source_time": t, "seq": n,
        "attributes": {k: str(v) for k, v in attrs.items()},
    }


def index_backbone(tables: dict) -> dict:
    """Build fast lookups from the backbone dict (asset_model.build_semantic_backbone output)."""
    assets_by_id = {a["asset_id"]: a for a in tables["asset_master"]}
    tags_by_asset: dict = {}
    for tg in tables["tag_registry"]:
        tags_by_asset.setdefault(tg["asset_id"], []).append(tg)
    wells_by_wh = {w["wellhead_asset_id"]: w for w in tables["well_master"]}
    return {"assets_by_id": assets_by_id, "tags_by_asset": tags_by_asset,
            "wells_by_wh": wells_by_wh, "tables": tables}
