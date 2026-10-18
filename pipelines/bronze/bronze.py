"""TwinV2 Bronze: landing only (Design Part 8.2). Raw fidelity, append-only, minimal transform.

All five Part-5 load classes: RT/NRT streaming from the landing tables (split by source_system),
event streams, batch snapshots of the CDC src_* tables, and Auto Loader for files on the Volume.
Every row carries ingest metadata; historian quality flags preserved verbatim; Liquid Clustering +
Predictive Optimization (no manual partitioning). Every table is COMMENTed.
"""
from __future__ import annotations
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

try:
    from pyspark import pipelines as dp          # current Lakeflow API (2026)
except ImportError:                              # fallback for older runtimes
    import dlt as dp

spark = SparkSession.builder.getOrCreate()
CATALOG = spark.conf.get("twin.catalog")
SCHEMA = spark.conf.get("twin.schema")
VOL = f"/Volumes/{CATALOG}/{SCHEMA}/landing"


def _src(t):
    return f"`{CATALOG}`.`{SCHEMA}`.`{t}`"


# ===== BRZ-01..04 + BRZ-03 events: streaming from the landing tables ==========================
def _stream_tel(name, comment, sources):
    @dp.table(name=name, comment=comment, table_properties={"delta.enableChangeDataFeed": "true"})
    def _fn():
        return (spark.readStream.option("skipChangeCommits", "true").table(_src("land_telemetry"))
                .where(F.col("source_system").isin(sources))
                .withColumn("ingest_ts", F.current_timestamp()))
    return _fn


bz_proc = _stream_tel("bronze_telemetry_process",
                      "BRZ-01. Raw process telemetry (SCADA/DCS) from the streaming transport. Split by source_system=scada_dcs.",
                      ["scada_dcs"])
bz_vib = _stream_tel("bronze_vibration_features",
                     "BRZ-02. Raw machinery vibration features (Bently-Nevada class): velocity_rms, accel_peak, band_1x. source_system=vibration_mon.",
                     ["vibration_mon"])
bz_cems = _stream_tel("bronze_emissions_cems",
                      "BRZ-03. Raw CEMS emission mass-rate telemetry (CH4/CO2/NOx, flare). source_system=cems.",
                      ["cems"])
bz_cust = _stream_tel("bronze_custody_metering",
                      "BRZ-04. Raw custody flow-computer + GC telemetry (flow, GHV/Wobbe, composition). source_system=custody_fc.",
                      ["custody_fc"])


@dp.table(name="bronze_safety_events", comment="BRZ-03. Raw discrete safety/F&G/ESD/emission-exceedance events (status + events only, read-only).",
          table_properties={"delta.enableChangeDataFeed": "true"})
def bronze_safety_events():
    return (spark.readStream.option("skipChangeCommits", "true").table(_src("land_events"))
            .withColumn("ingest_ts", F.current_timestamp()))


# ===== BRZ-05 historian materialization (snapshot stand-in over the landing history) ==========
@dp.table(name="bronze_historian_tags", comment="BRZ-05. Historian materialization stand-in: daily snapshot of the landed telemetry (represents AVEVA PI via Delta Share). MB cadence.")
def bronze_historian_tags():
    return (spark.read.table(_src("land_telemetry"))
            .withColumn("ingest_ts", F.current_timestamp()))


@dp.table(name="bronze_af_context", comment="BRZ-05. PI Asset-Framework context stand-in: the tag registry (tag->asset->unit->scene). Snapshot.")
def bronze_af_context():
    return spark.read.table(_src("tag_registry")).withColumn("ingest_ts", F.current_timestamp())


# ===== BRZ-09/10 + others: batch snapshots of the CDC src_* tables ============================
def _snapshot(name, comment, src_table):
    @dp.table(name=name, comment=comment)
    def _fn():
        return spark.read.table(_src(src_table)).withColumn("ingest_ts", F.current_timestamp())
    return _fn


bz_wo = _snapshot("bronze_workorders", "BRZ-09. EAM work-order snapshot for CDC into Silver.", "src_eam_workorders")
bz_nt = _snapshot("bronze_notifications", "BRZ-09. EAM notifications snapshot.", "src_eam_notifications")
bz_fh = _snapshot("bronze_failure_history", "BRZ-09. EAM ISO-14224 failure-history snapshot.", "src_eam_failure_history")
bz_al = _snapshot("bronze_allocation", "BRZ-10. Production-accounting allocation snapshot (per well per gas day).", "src_pa_allocation")
bz_df = _snapshot("bronze_deferment", "BRZ-10. Production-accounting deferment ledger snapshot (coded cause + dollars).", "src_pa_deferment")
bz_nm = _snapshot("bronze_nominations", "BRZ-10. Nominations vs actuals snapshot (field daily).", "src_pa_nominations")
bz_hr = _snapshot("bronze_workforce", "BRZ-09. HR/workforce roster snapshot (crew competency + availability).", "src_hr_workforce")
bz_pm = _snapshot("bronze_permits", "BRZ-09. HSE permit-to-work snapshot.", "src_hse_permits")
bz_mc = _snapshot("bronze_moc", "BRZ-09. OT-security / management-of-change log snapshot.", "src_ot_moc_log")
bz_sp = _snapshot("bronze_spares", "BRZ-09. Spare-parts inventory snapshot (on-hand + lead time + BOM applicability).", "src_spares_inventory")
bz_ct = _snapshot("bronze_contracts", "BRZ-10. Gas-sales contracts snapshot (delivery point + daily qty + take-or-pay).", "src_commercial_contracts")
bz_dv = _snapshot("bronze_dev_schedule", "BRZ-10. Development / capex schedule snapshot (projects by area).", "src_dev_schedule")
bz_sn = _snapshot("bronze_supply_network", "BRZ-10. Supply-network snapshot: directed edges reservoir to customer with flow-split shares.", "src_supply_network")
bz_cu = _snapshot("bronze_customers", "BRZ-10. Customer book snapshot (fictional buyers, delivery point, contract terms).", "src_customers")
bz_cd = _snapshot("bronze_customer_deliveries", "BRZ-10. Daily custody-transfer deliveries per customer snapshot (nominated vs delivered).", "src_customer_deliveries")


# ===== BRZ-06/07/08/11/12: Auto Loader for files on the Volume ================================
def _autoloader(name, comment, subdir, fmt, opts=None):
    @dp.table(name=name, comment=comment)
    def _fn():
        rdr = (spark.readStream.format("cloudFiles")
               .option("cloudFiles.format", fmt))
        if fmt in ("csv", "json"):
            rdr = rdr.option("cloudFiles.inferColumnTypes", "true")
        if fmt == "csv":
            rdr = rdr.option("header", "true")
        for k, v in (opts or {}).items():
            rdr = rdr.option(k, v)
        return (rdr.load(f"{VOL}/{subdir}")
                .withColumn("ingest_ts", F.current_timestamp())
                .withColumn("source_file", F.col("_metadata.file_path")))
    return _fn


bz_lab = _autoloader("bronze_lab_assay", "BRZ-06. LIMS gas-composition sample files (Auto Loader, CSV).", "lab", "csv")
bz_prov = _autoloader("bronze_custody_proving", "BRZ-06. Custody meter-proving files (Auto Loader, CSV).", "proving", "csv")
bz_prc = _autoloader("bronze_prices", "BRZ-12. Commercial price curve files: gas + carbon price (Auto Loader, CSV).", "prices", "csv")
bz_wx = _autoloader("bronze_weather", "BRZ-12. Weather/environment files: ambient, wind, lightning, flood (Auto Loader, CSV).", "weather", "csv")
bz_wm = _autoloader("bronze_well_master_osdu", "BRZ-11. OSDU well master (Auto Loader, JSON).", "well_master", "json")
bz_pvt = _autoloader("bronze_pvt", "BRZ-11. OSDU PVT / fluid properties (Auto Loader, JSON).", "pvt", "json")
bz_geo = _autoloader("bronze_geometry_manifest", "BRZ-08. 3D geometry manifest: scene->asset hotspot (Auto Loader, JSON).", "geometry", "json")
bz_img = _autoloader("bronze_imagery", "BRZ-08. OGI/drone imagery manifest (Auto Loader, JSON).", "imagery", "json")
bz_doc = _autoloader("bronze_documents", "BRZ-07. Engineering documents (P&IDs/datasheets) as binary + metadata; parsing deferred to Silver.", "documents", "binaryFile")
