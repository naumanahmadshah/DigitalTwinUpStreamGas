"""Shared field clock so backfill (streaming) and batch sources agree on the same timeline.

The authored 2-year window always ENDS 'as of today' (Data-gen 9.8), so a twin generated on any day
reads as live now. Both run_generator (telemetry/events) and batch_sources (allocation/deferment/...)
call timeline() and day_frac(), so every emitter shares one start/end and the scenarios line up.
"""
from __future__ import annotations
import datetime as dt


def timeline(cfg: dict) -> tuple[dt.datetime, dt.datetime]:
    days = int(cfg.get("generation", {}).get("backfill_days", 730))
    end = dt.datetime.now(dt.timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0) + dt.timedelta(days=1)
    start = end - dt.timedelta(days=days)
    return start.replace(tzinfo=None), end.replace(tzinfo=None)


def day_frac(t: dt.datetime, start: dt.datetime, end: dt.datetime) -> float:
    total = (end - start).total_seconds() or 1.0
    return max(0.0, min(1.0, (t - start).total_seconds() / total))
