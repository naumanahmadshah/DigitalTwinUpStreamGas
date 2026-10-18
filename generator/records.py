"""Landing contract + canonical record schemas (Data-gen Part 9.4 / 9.9).

The generator's ONLY contract with the platform is where it lands. It writes two streaming
landing Delta tables (`land_telemetry`, `land_events`) that Bronze's streaming jobs read, plus
`src_*` CDC snapshot tables and files on the `landing` Volume (written by batch_sources.py).
Nothing here writes Silver/Gold and nothing actuates, the read-only boundary of Part 5.
"""
from __future__ import annotations
from pyspark.sql import types as T


def fqtn(catalog: str, schema: str, table: str) -> str:
    """Backtick-quote every identifier part so catalog/schema names with special characters work."""
    return f"`{catalog}`.`{schema}`.`{table}`"


# Telemetry envelope (RT/NRT streaming): lands to land_telemetry, split to bronze by source_system.
# Carries the vibration feature fields too (null for non-vibration tags).
TELEMETRY_SCHEMA = T.StructType([
    T.StructField("source_system", T.StringType()),   # scada_dcs, historian_pi, vibration_mon, custody_fc, cems
    T.StructField("tag_id",        T.StringType()),    # AREA-SYSTEM-CLASS-NNNN, join key to slv_tag_registry
    T.StructField("asset_id",      T.StringType()),
    T.StructField("signal",        T.StringType()),
    T.StructField("event_time",    T.TimestampType()), # ts_event: when it occurred in the field
    T.StructField("source_time",   T.TimestampType()), # ts_source: when the source stamped it (clock skew)
    T.StructField("value",         T.DoubleType()),
    T.StructField("unit",          T.StringType()),
    T.StructField("quality",       T.StringType()),    # good, uncertain, bad, sensor_out, manual_override
    T.StructField("seq",           T.LongType()),       # monotonic per-tag sequence, for ordering/exactly-once
    # vibration feature fields (null unless source_system = vibration_mon)
    T.StructField("velocity_rms",  T.DoubleType()),
    T.StructField("accel_peak",    T.DoubleType()),
    T.StructField("band_1x",       T.DoubleType()),
])

# Event envelope (RT + E): safety/F&G/ESD/emission-exceedance events -> land_events.
EVENT_SCHEMA = T.StructType([
    T.StructField("source_system", T.StringType()),    # sis_esd, fire_gas, cems, dcs_alarm
    T.StructField("event_id",      T.StringType()),
    T.StructField("asset_id",      T.StringType()),
    T.StructField("tag_id",        T.StringType()),
    T.StructField("area_code",     T.StringType()),
    T.StructField("event_type",    T.StringType()),    # alarm, esd_trip, fg_detection, valve_change, emission_exceedance
    T.StructField("severity",      T.StringType()),    # info, advisory, alarm, critical
    T.StructField("state",         T.StringType()),    # new, tripped, cleared, bypassed
    T.StructField("event_time",    T.TimestampType()),
    T.StructField("source_time",   T.TimestampType()),
    T.StructField("seq",           T.LongType()),
    T.StructField("attributes",    T.MapType(T.StringType(), T.StringType())),
])

_TEL_COMMENTS = {
    "__table__": "Streaming landing for all high-frequency OT telemetry (Data-gen GEN-process/vibration/custody/CEMS). Bronze BRZ-01..04 read this, splitting by source_system. Raw fidelity; quality flags preserved verbatim.",
    "source_system": "Emitter identity: scada_dcs, historian_pi, vibration_mon, custody_fc, cems.",
    "tag_id": "AREA-SYSTEM-CLASS-NNNN. The universal join key to slv_tag_registry.",
    "asset_id": "Asset the tag belongs to.",
    "signal": "What the tag measures (discharge_pressure, water_dewpoint, ...).",
    "event_time": "ts_event: field time the measurement occurred (UTC).",
    "source_time": "ts_source: time the source system stamped/published it (differs from event_time: clock skew).",
    "value": "Engineering value in `unit`.",
    "unit": "Engineering unit (kPa, degC, e3m3/d, mm/s, ...). Occasionally deliberately inconsistent (bar) to exercise Silver normalization.",
    "quality": "good, uncertain, bad, sensor_out, or manual_override. Kept verbatim; Silver filters.",
    "seq": "Monotonic per-tag sequence for ordering and exactly-once.",
    "velocity_rms": "Vibration velocity RMS feature (mm/s); null unless source_system=vibration_mon.",
    "accel_peak": "Vibration acceleration peak feature (g); null unless vibration.",
    "band_1x": "Vibration 1x running-speed band amplitude; null unless vibration.",
}
_EVT_COMMENTS = {
    "__table__": "Streaming landing for discrete field events (Data-gen GEN-safety-and-emissions-events): alarms, ESD/SIS trips, F&G detections, emission exceedances. Status/events only, read-only. Bronze BRZ-03 reads this.",
    "source_system": "sis_esd, fire_gas, cems, or dcs_alarm.",
    "event_id": "Unique event key.",
    "asset_id": "Asset the event is about.",
    "tag_id": "Tag the event is about, where applicable.",
    "area_code": "Area the event occurred in.",
    "event_type": "alarm, esd_trip, fg_detection, valve_change, or emission_exceedance.",
    "severity": "info, advisory, alarm, or critical (tiered alerting).",
    "state": "New state: new, tripped, cleared, or bypassed.",
    "event_time": "Field time of the event (UTC).",
    "source_time": "Source publish time.",
    "seq": "Sequence-of-events ordering key.",
    "attributes": "Event-specific payload (setpoint, measured value, bypass reason, ...).",
}

LANDING_TABLES = {
    "land_telemetry": (TELEMETRY_SCHEMA, _TEL_COMMENTS, "tag_id, event_time"),
    "land_events":    (EVENT_SCHEMA,     _EVT_COMMENTS, "asset_id, event_time"),
}


def ensure_landing_tables(spark, catalog: str, schema: str) -> None:
    """Create the streaming landing tables (idempotent) with COMMENTs and Liquid Clustering."""
    for table, (sch, comments, cluster) in LANDING_TABLES.items():
        name = fqtn(catalog, schema, table)
        cols = ", ".join(f"`{f.name}` {f.dataType.simpleString()}" for f in sch.fields)
        spark.sql(f"CREATE TABLE IF NOT EXISTS {name} ({cols}) USING delta CLUSTER BY ({cluster})")
        spark.sql(f"COMMENT ON TABLE {name} IS '{comments['__table__'].replace(chr(39), chr(39)*2)}'")
        for col, c in comments.items():
            if col == "__table__":
                continue
            spark.sql(f"ALTER TABLE {name} ALTER COLUMN `{col}` COMMENT '{c.replace(chr(39), chr(39)*2)}'")
