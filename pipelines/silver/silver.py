"""TwinV2 Silver — cleanse, conform, contextualize (Design Part 8.3 / Part 6).

Silver is where the twin becomes intelligible: a raw tag becomes "Compressor CPF002-CMP-03 discharge
pressure on Train 2". Telemetry is deduped, unit-normalized (bar->kPa), quality-filtered (flag kept),
time-aligned, and stream-static joined to the tag registry. Reference/transactional data is conformed
and asset-joined. Data-quality expectations run here. Everything keyed to tag_id / asset_id / fl.
"""
from __future__ import annotations
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

try:
    from pyspark import pipelines as dp
except ImportError:
    import dlt as dp

spark = SparkSession.builder.getOrCreate()
CATALOG = spark.conf.get("twin.catalog")
SCHEMA = spark.conf.get("twin.schema")


def _ext(t):
    """Setup-written backbone/src tables (not created by this pipeline) -> full name."""
    return f"`{CATALOG}`.`{SCHEMA}`.`{t}`"


def _tag_static():
    return (spark.read.table(_ext("tag_registry"))
            .select("tag_id", "asset_id", "fl_id", "area_code", "system_code", "class_code",
                    "signal", F.col("unit").alias("reg_unit"), "eng_range_min", "eng_range_max",
                    "description", "scene_id"))


def _assets_static():
    return (spark.read.table(_ext("asset_master"))
            .select("asset_id", "equipment_type", "area_code", "system_code", "criticality",
                    "scene_id", "hotspot_x", "hotspot_y", "gps_lat", "gps_lon"))


# ===== SLV-01 semantic model (the backbone, conformed) =======================================
def _ref(name, src, comment):
    @dp.table(name=name, comment=comment)
    def _fn():
        return spark.read.table(_ext(src))
    return _fn


s_fl = _ref("slv_functional_location", "functional_location", "SLV-01. Conformed ISO-14224 functional-location hierarchy.")
s_am = _ref("slv_asset_master", "asset_master", "SLV-01. Conformed asset master (class, criticality, 3D scene binding).")
s_tr = _ref("slv_tag_registry", "tag_registry", "SLV-01. THE CONTRACT: tag_id -> asset, unit, range, cadence, 3D hotspot.")
s_wm = _ref("slv_well_master", "well_master", "SLV-01. Conformed well master (lift + completion).")
s_pv = _ref("slv_pvt", "pvt", "SLV-01. Conformed per-well PVT / composition.")
s_xm = _ref("slv_id_crossmap", "id_crossmap", "SLV-01. Cross-system id map (historian<->EAM<->OSDU<->doc); stale links exposed.")


# ===== SLV-02 telemetry cleanse + contextualize ==============================================
def _conform_tel(bronze_name, silver_name, comment, extra_cols=None):
    @dp.table(name=silver_name, comment=comment)
    @dp.expect("value_present_or_flagged", "value IS NOT NULL OR quality <> 'good'")
    @dp.expect("known_quality", "quality IN ('good','uncertain','bad','sensor_out','manual_override')")
    def _fn():
        raw = (spark.readStream.table(bronze_name)
               .withWatermark("event_time", "10 minutes")
               .dropDuplicatesWithinWatermark(["tag_id", "event_time", "seq"]))
        norm = (raw
                .withColumn("value_norm", F.when(F.col("unit") == "bar", F.col("value") * 100.0).otherwise(F.col("value")))
                .withColumn("unit_norm", F.when(F.col("unit") == "bar", F.lit("kPa")).otherwise(F.col("unit")))
                .withColumn("gas_day", F.to_date("event_time")))
        reg = _tag_static().drop("asset_id", "signal")  # keep bronze asset_id + signal, enrich the rest
        out = norm.join(F.broadcast(reg), "tag_id", "left")
        out = out.withColumn("in_range",
                             (F.col("value_norm") >= F.col("eng_range_min")) &
                             (F.col("value_norm") <= F.col("eng_range_max")))
        keep = ["tag_id", "asset_id", "fl_id", "area_code", "system_code", "class_code", "signal",
                "event_time", "source_time", "gas_day", "value", "value_norm", "unit", "unit_norm",
                "quality", "in_range", "eng_range_min", "eng_range_max", "description", "scene_id", "seq"]
        keep += (extra_cols or [])
        return out.select(*[c for c in keep if c in out.columns])
    return _fn


sv_proc = _conform_tel("bronze_telemetry_process", "slv_telemetry_process",
                       "SLV-02. Cleansed, unit-normalized, de-duplicated process telemetry, contextualized to asset/functional-location and engineering range.")
sv_vib = _conform_tel("bronze_vibration_features", "slv_telemetry_vibration",
                      "SLV-02. Cleansed vibration features contextualized to the machine, with velocity_rms/accel_peak/band_1x.",
                      extra_cols=["velocity_rms", "accel_peak", "band_1x"])
sv_met = _conform_tel("bronze_custody_metering", "slv_metering",
                      "SLV-02. Cleansed custody metering + GC telemetry contextualized to the metering station.")
sv_emi = _conform_tel("bronze_emissions_cems", "slv_emissions_ts",
                      "SLV-02. Cleansed CEMS emission mass-rate telemetry contextualized to the source asset.")


# ===== SLV-03 event + alarm conform ==========================================================
@dp.table(name="slv_safety_events", comment="SLV-03. Conformed safety/F&G/ESD/emission events, asset-joined (area, equipment type, criticality).")
def slv_safety_events():
    ev = spark.readStream.table("bronze_safety_events").withColumn("gas_day", F.to_date("event_time"))
    return ev.join(F.broadcast(_assets_static().select("asset_id", "equipment_type", "criticality")), "asset_id", "left")


@dp.table(name="slv_alarms", comment="SLV-03. Alarm subset for rationalization/flood analytics (event_type='alarm').")
def slv_alarms():
    return (spark.read.table("bronze_safety_events").where("event_type = 'alarm'")
            .withColumn("gas_day", F.to_date("event_time")))


# ===== SLV-04 transactional conform ==========================================================
def _conform_txn(bronze_name, silver_name, comment, join_asset=True):
    @dp.table(name=silver_name, comment=comment)
    def _fn():
        df = spark.read.table(bronze_name)
        if join_asset and "asset_id" in df.columns:
            # only pull asset columns not already present, to avoid duplicate-column ambiguity
            acols = [c for c in ["equipment_type", "area_code", "criticality"] if c not in df.columns]
            df = df.join(F.broadcast(_assets_static().select("asset_id", *acols)), "asset_id", "left")
        return df
    return _fn


sv_wo = _conform_txn("bronze_workorders", "slv_workorders", "SLV-04. Conformed EAM work orders, asset-joined.")
sv_nt = _conform_txn("bronze_notifications", "slv_notifications", "SLV-04. Conformed EAM notifications, asset-joined.")
sv_fh = _conform_txn("bronze_failure_history", "slv_failure_history", "SLV-04. Conformed ISO-14224 failure history, asset-joined.")
sv_al = _conform_txn("bronze_allocation", "slv_allocation", "SLV-04. Conformed allocation (produced vs allocated per well per gas day).")
sv_df = _conform_txn("bronze_deferment", "slv_deferment", "SLV-04. Conformed deferment ledger (coded cause + dollars), asset-joined.")
sv_nm = _conform_txn("bronze_nominations", "slv_nominations", "SLV-04. Conformed nominations vs actuals.", join_asset=False)
sv_lb = _conform_txn("bronze_lab_assay", "slv_lab_assay", "SLV-04. Conformed LIMS gas-composition assays.", join_asset=False)
sv_pr = _conform_txn("bronze_custody_proving", "slv_custody_proving", "SLV-04. Conformed custody meter proving records.", join_asset=False)
sv_px = _conform_txn("bronze_prices", "slv_prices", "SLV-04. Conformed commercial price curve (gas + carbon).", join_asset=False)
sv_wx = _conform_txn("bronze_weather", "slv_weather", "SLV-04. Conformed weather/environment observations.", join_asset=False)
sv_hr = _conform_txn("bronze_workforce", "slv_workforce", "SLV-04. Conformed workforce roster (competency + availability).", join_asset=False)
sv_pm = _conform_txn("bronze_permits", "slv_permits", "SLV-04. Conformed HSE permits-to-work, asset-joined.")
sv_mc = _conform_txn("bronze_moc", "slv_moc", "SLV-04. Conformed OT-security / MOC log, asset-joined.")
sv_sp = _conform_txn("bronze_spares", "slv_spares", "SLV-04. Conformed spare-parts inventory (with BOM applicability).", join_asset=False)
sv_ct = _conform_txn("bronze_contracts", "slv_commercial_contracts", "SLV-04. Conformed gas-sales contracts (delivery point = custody station).", join_asset=False)
sv_dv = _conform_txn("bronze_dev_schedule", "slv_dev_schedule", "SLV-04. Conformed development / capex schedule.", join_asset=False)


# ===== SLV-05 document + geometry conform ====================================================
@dp.table(name="slv_document_entities", comment="SLV-05. Document intelligence stand-in: engineering documents linked to their area/asset (ai_parse_document in the design).")
def slv_document_entities():
    docs = spark.read.table("bronze_documents").select(
        F.col("source_file"),
        F.regexp_extract(F.col("source_file"), r"PID-([A-Z0-9]+)", 1).alias("area_code"),
        F.length("content").alias("bytes"))
    return docs.withColumn("doc_type", F.lit("P&ID")).withColumn("parsed", F.lit(True))


@dp.table(name="slv_geometry_manifest", comment="SLV-05. 3D geometry manifest: scene_id -> asset hotspot/coords/LOD. The join the 3D navigator uses to resolve a clicked hotspot to its gold record.")
def slv_geometry_manifest():
    return spark.read.table("bronze_geometry_manifest").select(
        "scene_id", "asset_id", "hotspot_x", "hotspot_y", "lod", "equipment_type")
