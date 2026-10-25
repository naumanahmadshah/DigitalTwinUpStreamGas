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
    fl = node_flows(backbone_idx, scenarios, t, day_frac) if "pad_hub" in backbone_idx else {"well": {}}
    cond = backbone_idx.get("cond", {})
    for asset_id, asset in assets.items():
        area = asset["area_code"]
        well = wells_by_wh.get(asset_id)
        mods = scenarios.mods_for(asset, well, t)
        res = physics.reservoir_frac(area, day_frac)
        amb = physics.ambient_c(t, seed)
        # per-asset degradation baseline (age + slow drift), scenario can raise it; kit on a healthy site is
        # kept in good order (its wear stays low), so a green site reads green on every signal
        age = max(0, 2024 - int(asset.get("install_year") or 2015))
        deg = min(0.92, 0.010 * age + 0.04 * day_frac * physics._h(asset_id, "wear"))
        if cond.get(area) == "green":
            deg = min(deg, 0.06)
        deg = max(deg, mods.get("degradation", 0.0))
        wctx = None
        if well is not None:
            r = fl["well"].get(asset_id, physics.well_rate_e3m3d(well, res, mods, day=t.date()))
            wctx = {"rate": r, "well": well, "shut": r <= 0}
        # the gas this asset carries: a share of its hub (gathering segment), its plant's flow split across the
        # parallel compressors, the whole plant (process units) or the meter's sales gas
        et = asset["equipment_type"]
        f = fl.get(area)
        if f is not None and et == "gathering_line":
            f = f * (0.25 + 0.6 * physics._h(asset_id, "seg"))
        elif f is not None and et == "compressor":
            f = f / max(1, backbone_idx["ncomp"].get(area, 1))
        ctx = {"day_frac": day_frac, "ambient_c": amb, "res_frac": res, "degradation": deg, "flow": f}
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
        # rare protective ESD trip (emergency shutdowns are serious + infrequent); a healthy site has had none
        # in the last 90 days (the window the asset health counts trips over)
        end = backbone_idx.get("end")
        recent = end is not None and (end - t).days < 90
        if physics._h(seed, "esd", area, day) > 0.9985 and not (recent and backbone_idx.get("cond", {}).get(area) == "green"):
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


def index_backbone(tables: dict, cfg: dict | None = None) -> dict:
    """Build fast lookups from the backbone dict (asset_model.build_semantic_backbone output). With the config,
    also the gas path (pad -> hub -> plant -> custody meter) and each site's condition, so every tick can size
    each node's flow from the wells feeding it and keep healthy sites healthy."""
    assets_by_id = {a["asset_id"]: a for a in tables["asset_master"]}
    tags_by_asset: dict = {}
    for tg in tables["tag_registry"]:
        tags_by_asset.setdefault(tg["asset_id"], []).append(tg)
    wells_by_wh = {w["wellhead_asset_id"]: w for w in tables["well_master"]}
    idx = {"assets_by_id": assets_by_id, "tags_by_asset": tags_by_asset,
           "wells_by_wh": wells_by_wh, "tables": tables}
    if cfg:
        import asset_model, simclock
        lay = asset_model.site_layout(cfg)
        ncomp = {}
        for a in tables["asset_master"]:
            if a["equipment_type"] == "compressor":
                ncomp[a["area_code"]] = ncomp.get(a["area_code"], 0) + 1
        areas = {a["area_code"] for a in tables["asset_master"]}
        idx.update({"pad_hub": lay["pad_hub"], "hub_train": lay["hub_train"], "ncomp": ncomp,
                    "split": (cfg.get("network") or {}).get("train_custody") or {},
                    "shrink": float((cfg.get("network") or {}).get("processing_shrink", 0.03)),
                    "cond": {ar: asset_model.site_condition(cfg, ar) for ar in areas},
                    "end": simclock.timeline(cfg)[1]})
    return idx


def node_flows(idx, scenarios: "Scenarios", t, day_frac) -> dict:
    """Gas through every node of the path at time t (10^3 m^3/d): each well's rate, summed up the pads, hubs and
    plants, and split to the custody meters (less processing shrink). Returns {"well": {wellhead: rate}, node: flow}."""
    wr, flow = {}, {}
    for wh, w in idx["wells_by_wh"].items():
        a = idx["assets_by_id"].get(wh)
        r = physics.well_rate_e3m3d(w, physics.reservoir_frac(w["pad_area"], day_frac), scenarios.mods_for(a, w, t) if a else {}, day=t.date())
        wr[wh] = r
        flow[w["pad_area"]] = flow.get(w["pad_area"], 0.0) + r
    for pad, hub in idx.get("pad_hub", {}).items():
        flow[hub] = flow.get(hub, 0.0) + flow.get(pad, 0.0)
    for hub, tr in idx.get("hub_train", {}).items():
        flow[tr] = flow.get(tr, 0.0) + flow.get(hub, 0.0)
    for tr, sp in idx.get("split", {}).items():
        for m, sh in sp.items():
            flow[m] = flow.get(m, 0.0) + flow.get(tr, 0.0) * float(sh) * (1 - idx.get("shrink", 0.03))
    flow["well"] = wr
    return flow
