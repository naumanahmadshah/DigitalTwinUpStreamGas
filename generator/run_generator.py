"""Phase 2 driver: run the generator in a chosen mode (Data-gen 9.8 run modes).

  backfill : write ~2 years of history straight to the landing tables (fast, chunked, idempotent).
  batch    : write the reference/transactional/file sources (batch_sources.py).
  realtime : continuous tick onto land_telemetry for a bounded window (live demo).

All modes share one seeded field state + clock (simclock), so every emitter reconciles.
"""
from __future__ import annotations
import argparse, datetime as dt, os, sys, time
from pyspark.sql import SparkSession


def _load_cfg(path):
    with open(path) as f:
        if path.endswith(".json"):
            import json
            return json.load(f)
        import yaml
        return yaml.safe_load(f)


def _rows_to_df(spark, rows, schema):
    names = [f.name for f in schema.fields]
    data = [tuple(r.get(n) for n in names) for r in rows]
    return spark.createDataFrame(data, schema=schema)


def _flush(spark, rows, schema, fq):
    if not rows:
        return 0
    _rows_to_df(spark, rows, schema).write.format("delta").mode("append").saveAsTable(fq)
    return len(rows)


def run_backfill(spark, cfg, catalog, schema):
    import asset_model, core, records, simclock, physics
    seed = int(cfg["seed"])
    start, end = simclock.timeline(cfg)
    bb = asset_model.build_semantic_backbone(cfg)
    idx = core.index_backbone(bb)
    scen = core.Scenarios(cfg, start, end)
    dq = core.DataQuality(cfg, seed)

    tel_fq = records.fqtn(catalog, schema, "land_telemetry")
    evt_fq = records.fqtn(catalog, schema, "land_events")
    records.ensure_landing_tables(spark, catalog, schema)
    # idempotent: clear prior backfill so re-deploy does not double rows
    spark.sql(f"TRUNCATE TABLE {tel_fq}")
    spark.sql(f"TRUNCATE TABLE {evt_fq}")

    seq: dict = {}
    dense_from = end - dt.timedelta(days=14)     # denser cadence in the last fortnight (freshness tiles)
    tel_buf, evt_buf = [], []
    total_tel = total_evt = 0
    CHUNK_DAYS = 30
    day = start
    di = 0
    while day < end:
        df = simclock.day_frac(day, start, end)
        # one daily sample at 12:00 for every tag
        noon = day.replace(hour=12)
        tel_buf.extend(core.emit_tick(idx, scen, dq, noon, seq, df, seed))
        evt_buf.extend(core.events_at(idx, scen, noon, seq, seed))
        # denser recent window: 6 samples/day
        if day >= dense_from:
            for h in (2, 6, 10, 14, 18, 22):
                t = day.replace(hour=h)
                tel_buf.extend(core.emit_tick(idx, scen, dq, t, seq, simclock.day_frac(t, start, end), seed))
        di += 1
        if di % CHUNK_DAYS == 0:
            total_tel += _flush(spark, tel_buf, records.TELEMETRY_SCHEMA, tel_fq)
            total_evt += _flush(spark, evt_buf, records.EVENT_SCHEMA, evt_fq)
            print(f"  ...{day.date()}  telemetry so far={total_tel:,}  events={total_evt:,}")
            tel_buf, evt_buf = [], []
        day += dt.timedelta(days=1)
    total_tel += _flush(spark, tel_buf, records.TELEMETRY_SCHEMA, tel_fq)
    total_evt += _flush(spark, evt_buf, records.EVENT_SCHEMA, evt_fq)
    print(f"Backfill complete: {total_tel:,} telemetry rows, {total_evt:,} events over {start.date()}..{end.date()}")


def run_realtime(spark, cfg, catalog, schema, max_minutes):
    import asset_model, core, records, simclock
    seed = int(cfg["seed"])
    start, end = simclock.timeline(cfg)
    bb = asset_model.build_semantic_backbone(cfg)
    idx = core.index_backbone(bb)
    scen = core.Scenarios(cfg, start, end)
    dq = core.DataQuality(cfg, seed)
    records.ensure_landing_tables(spark, catalog, schema)
    tel_fq = records.fqtn(catalog, schema, "land_telemetry")
    seq: dict = {}
    deadline = time.time() + max_minutes * 60
    print(f"Realtime tick for up to {max_minutes} min...")
    while time.time() < deadline:
        t = dt.datetime.utcnow()
        rows = core.emit_tick(idx, scen, dq, t, seq, 1.0, seed)
        _flush(spark, rows, records.TELEMETRY_SCHEMA, tel_fq)
        time.sleep(15)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog", required=True)
    ap.add_argument("--schema", required=True)
    ap.add_argument("--config", required=True)
    ap.add_argument("--mode", default="backfill", choices=["backfill", "batch", "realtime"])
    ap.add_argument("--max-minutes", type=int, default=10)
    args = ap.parse_args()

    root = os.path.dirname(os.path.dirname(args.config))
    sys.path.insert(0, os.path.join(root, "generator"))
    cfg = _load_cfg(args.config)
    spark = SparkSession.builder.getOrCreate()

    if args.mode == "batch":
        import batch_sources
        batch_sources.generate(spark, cfg, args.catalog, args.schema)
    elif args.mode == "realtime":
        run_realtime(spark, cfg, args.catalog, args.schema, args.max_minutes)
    else:
        run_backfill(spark, cfg, args.catalog, args.schema)


if __name__ == "__main__":
    main()
