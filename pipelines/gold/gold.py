"""TwinV2 Gold: business marts + ML proxies + Lakebase-serving marts (Design Part 7 / 8.4).

MV-only: all logic lives here; the App and Genie issue plain SELECTs. Predictions are materialized as
Gold so every surface reads one governed copy; recommendations are always advisory (the Part 4
boundary). Every mart carries table + column COMMENTs so Genie and analysts read them correctly.
"""
from __future__ import annotations
from pyspark.sql import SparkSession
from pyspark.sql import functions as F, Window as W

try:
    from pyspark import pipelines as dp
except ImportError:
    import dlt as dp

spark = SparkSession.builder.getOrCreate()
CATALOG = spark.conf.get("twin.catalog")
SCHEMA = spark.conf.get("twin.schema")
GJ_PER_E3M3 = 38.0


def _node_flow():
    """Average gas through each supply-network node (well pad, hub, train, compressor, meter) over the
    latest 30 gas days, in 10^3 m^3/d: the larger of what flows in and what flows out."""
    n = spark.read.table("gld_supply_network")
    fin = n.groupBy(F.col("to_id").alias("node_id")).agg(F.sum("flow_e3m3d").alias("fi"))
    fout = n.groupBy(F.col("from_id").alias("node_id")).agg(F.sum("flow_e3m3d").alias("fo"))
    return fin.join(fout, "node_id", "full_outer").select(
        "node_id", F.greatest(F.coalesce("fi", F.lit(0.0)), F.coalesce("fo", F.lit(0.0))).alias("node_flow"))


def _ext(t):
    return f"`{CATALOG}`.`{SCHEMA}`.`{t}`"


def _daily(table, signals):
    """Wide per-asset daily average pivot of the given signals from a conformed telemetry table."""
    df = spark.read.table(table).where(F.col("signal").isin(signals))
    return df.groupBy("asset_id", "gas_day").pivot("signal", signals).agg(F.avg("value_norm"))


# ===== GLD-01 field production ===============================================================
@dp.table(name="gld_field_production", comment="Field production vs nomination by gas day. Serves the Command Centre and Genie production. Volumes in 10^3 m^3.")
def gld_field_production():
    nom = spark.read.table("slv_nominations").select(
        "gas_day", F.col("nominated_e3m3"), F.col("actual_e3m3"), F.col("field_potential_e3m3").alias("potential_e3m3"))
    return nom.withColumn("gap_to_nomination_e3m3", F.round(F.col("actual_e3m3") - F.col("nominated_e3m3"), 1)).orderBy("gas_day")


# ===== GLD-01 well scorecard =================================================================
@dp.table(name="gld_well_scorecard", comment="Per-well daily scorecard: production, wellhead conditions, lift, liquid-loading watch, SSSV status. Serves Wells tab and Genie wells.")
def gld_well_scorecard():
    tel = _daily("slv_telemetry_process",
                 ["production_flow", "tubing_pressure", "casing_pressure_a", "choke_position", "wellhead_temp", "sssv_status"])
    wm = spark.read.table("slv_well_master").select(
        F.col("well_id"), F.col("wellhead_asset_id").alias("asset_id"), "lift_type", "completion_type",
        "criticality", "status", "pad_area")
    tel = tel.join(F.broadcast(wm), "asset_id", "inner")
    alloc = spark.read.table("slv_allocation").select("gas_day", "well_id", "allocated_e3m3", "produced_e3m3")
    out = tel.join(alloc, ["well_id", "gas_day"], "left")
    # nodal/utilization proxy: produced vs a per-well 30-day rolling max potential
    # liquid loading: a flowing well choked back with its tubing-head pressure backed up well above its own norm
    # (a liquid column building), judged against the well itself since a CSG well and an offshore well run at
    # very different pressures. Shut-in days (no flow) are not loading.
    wmax = out.groupBy("well_id").agg(F.max("production_flow").alias("pot_flow"), F.avg("tubing_pressure").alias("tp_norm"))
    out = (out.join(F.broadcast(wmax), "well_id", "left")
           .withColumn("utilization_pct", F.round(100 * F.col("production_flow") / F.when(F.col("pot_flow") > 0, F.col("pot_flow")).otherwise(F.lit(None)), 1))
           .withColumn("liquid_loading_watch",
                       F.when((F.col("choke_position") < 40) & (F.col("production_flow") > 0) & (F.col("tubing_pressure") > F.col("tp_norm") * 1.03), F.lit("watch")).otherwise(F.lit("ok")))
           .withColumn("sssv_status", F.when(F.col("sssv_status") >= 50, F.lit("pass")).otherwise(F.lit("not_passed")))
           .drop("tp_norm"))
    return out.orderBy("well_id", "gas_day")


# ===== GLD-01 compression health (the vibration->constraint story) ===========================
@dp.table(name="gld_compression_health", comment="Per-compressor daily health: efficiency, vibration, throughput, anti-surge margin, health score, RUL proxy, production-at-risk. Serves the Compression tab, constraint ranking, and Genie plant.")
def gld_compression_health():
    proc = _daily("slv_telemetry_process",
                  ["suction_pressure", "discharge_pressure", "throughput", "polytropic_eff", "antisurge_valve", "bearing_temp"])
    vib = _daily("slv_telemetry_vibration", ["vibration_overall"])
    comp = spark.read.table("slv_asset_master").where("equipment_type='compressor'").select(
        "asset_id", "area_code", "criticality")
    out = (comp.join(proc, "asset_id", "left").join(vib, ["asset_id"], "left") if False else
           proc.join(vib, ["asset_id", "gas_day"], "outer").join(F.broadcast(comp), "asset_id", "inner"))
    out = (out
           .withColumn("antisurge_margin_pct", F.round(100 - F.coalesce(F.col("antisurge_valve"), F.lit(0)), 1))
           .withColumn("health_score", F.round(F.greatest(F.lit(0.0), F.least(F.lit(100.0),
                        100 - F.coalesce(F.col("vibration_overall"), F.lit(0)) * 3.0
                        - (F.lit(84.0) - F.coalesce(F.col("polytropic_eff"), F.lit(84.0))) * 1.5)), 1))
           .withColumn("rul_days_proxy", F.round(F.greatest(F.lit(5.0), 400 - F.coalesce(F.col("vibration_overall"), F.lit(0)) * 18), 0))
           )
    # production-at-risk = the gas this unit actually carries (its share of the plant, from the supply
    # network) x how far it has fallen below healthy (70): a healthy unit puts none at risk, a failing one all its gas.
    nf = _node_flow().withColumnRenamed("node_id", "asset_id")
    out = (out.join(F.broadcast(nf), "asset_id", "left")
           .withColumn("production_at_risk_e3m3d", F.round(F.coalesce(F.col("node_flow"), F.lit(0.0)) * F.greatest(F.lit(0.0), F.lit(70.0) - F.col("health_score")) / 70.0, 0))
           .drop("node_flow"))
    return out.orderBy("asset_id", "gas_day")


# ===== GLD-01 plant KPI (dewpoint/amine spec drift) ==========================================
@dp.table(name="gld_plant_kpi", comment="Processing stream KPIs + spec compliance: water dewpoint, H2S/CO2 slippage, NGL recovery. 'marginal' = within 20% of limit. Serves Processing tab and Genie spec.")
def gld_plant_kpi():
    piv = _daily("slv_telemetry_process", ["water_dewpoint", "h2s_outlet", "co2_outlet", "ngl_recovery", "reboiler_temp"])
    a = spark.read.table("slv_asset_master").select("asset_id", "area_code", "system_code", "equipment_type")
    out = piv.join(F.broadcast(a), "asset_id", "inner")
    out = (out
           .withColumn("dewpoint_on_spec", F.when(F.col("water_dewpoint").isNull(), None).otherwise(F.col("water_dewpoint") <= -10))
           .withColumn("h2s_on_spec", F.when(F.col("h2s_outlet").isNull(), None).otherwise(F.col("h2s_outlet") <= 4))
           .withColumn("co2_on_spec", F.when(F.col("co2_outlet").isNull(), None).otherwise(F.col("co2_outlet") <= 3))
           .withColumn("marginal", (F.col("water_dewpoint") > -12) | (F.col("h2s_outlet") > 3.2) | (F.col("co2_outlet") > 2.4)))
    return out.orderBy("asset_id", "gas_day")


# ===== GLD-01 flow assurance =================================================================
@dp.table(name="gld_flow_assurance", comment="Gathering-line flow assurance: line pressure/flow, hydrate/erosion risk, pig-due. Serves the Gathering tab.")
def gld_flow_assurance():
    piv = _daily("slv_telemetry_process", ["line_pressure", "line_flow", "line_temp"])
    gl = spark.read.table("slv_asset_master").where("system_code='GL'").select("asset_id", "area_code")
    out = piv.join(F.broadcast(gl), "asset_id", "inner")
    # Hydrate approach margin (subcooling), the industry-standard flow-assurance metric: how far the line
    # temperature sits ABOVE the gas-hydrate formation temperature at line pressure. Positive = safe above
    # the curve; a small margin means the segment is close to forming hydrates. The hydrate formation
    # temperature rises with pressure (log correlation calibrated to sweet-gas: ~19.5 degC at ~8600 kPa).
    thyd = (F.lit(10.1) * F.log(F.greatest(F.col("line_pressure"), F.lit(100.0))) - F.lit(72.0))
    margin = F.col("line_temp") - thyd
    out = (out
           .withColumn("hydrate_formation_temp_c", F.round(thyd, 1))
           .withColumn("hydrate_margin_c", F.round(margin, 1))
           .withColumn("hydrate_status", F.when(margin < 3, F.lit("at_risk"))
                       .when(margin < 8, F.lit("watch")).otherwise(F.lit("ok")))
           .withColumn("erosion_risk", F.round(F.coalesce(F.col("line_flow"), F.lit(0)) / 100.0, 1))
           .withColumn("pig_due_days", (F.abs(F.hash("asset_id")) % 30)))
    return out.orderBy("asset_id", "gas_day")


# ===== GLD-01 emissions ledger ===============================================================
@dp.table(name="gld_emissions_ledger", comment="Emissions ledger by source per gas day: CH4/CO2/NOx mass, flare volume, carbon intensity per 10^3 m^3. Serves the Emissions tab and Genie emissions.")
def gld_emissions_ledger():
    piv = _daily("slv_emissions_ts", ["ch4_massrate", "co2_massrate", "nox_massrate", "flare_flow"])
    a = spark.read.table("slv_asset_master").select("asset_id", "area_code", "equipment_type")
    prod = spark.read.table("slv_allocation").groupBy("gas_day").agg(F.sum("produced_e3m3").alias("field_e3m3"))
    out = (piv.join(F.broadcast(a), "asset_id", "left").join(prod, "gas_day", "left")
           .withColumn("ch4_kg", F.round(F.coalesce(F.col("ch4_massrate"), F.lit(0)) * 24, 1))
           .withColumn("co2_kg", F.round(F.coalesce(F.col("co2_massrate"), F.lit(0)) * 24, 1))
           .withColumn("nox_kg", F.round(F.coalesce(F.col("nox_massrate"), F.lit(0)) * 24, 1))
           .withColumn("carbon_intensity_kg_per_e3m3",
                       F.round(F.coalesce(F.col("co2_massrate"), F.lit(0)) * 24 / F.when(F.col("field_e3m3") > 0, F.col("field_e3m3")).otherwise(F.lit(None)), 3)))
    return out.orderBy("gas_day", "asset_id")


# ===== GLD-03 non-real-time context marts (research doc 4.1: context joined in Databricks) ====
@dp.table(name="gld_lab_assay", comment="Lab/assay results (LIMS) per well sample: gas composition (C1/CO2/N2), H2S, water and oil-in-water. Non-real-time context joined to the well. Serves the Lab & Assay surface.")
def gld_lab_assay():
    return (spark.read.table("slv_lab_assay")
            .select("asset_id", "sample_id", "sample_point", "well_id", F.to_date("sample_date").alias("sample_date"),
                    "c1_molpct", "co2_molpct", "n2_molpct", "h2s_ppm", "water_ppm", "oil_in_water_ppm")
            .orderBy(F.col("sample_date").desc()))


@dp.table(name="gld_commercial", comment="Commercial context by gas day: gas and carbon prices, contractual nomination vs actual vs unconstrained potential, delivery % and valued revenue. Serves the Commercial & Prices surface.")
def gld_commercial():
    p = spark.read.table("slv_prices").select(F.to_date("price_date").alias("gas_day"), "gas_price_gj", "carbon_price_t", "currency")
    n = spark.read.table("slv_nominations").select(F.to_date("gas_day").alias("gas_day"), "nominated_e3m3", "actual_e3m3", "field_potential_e3m3")
    return (n.join(p, "gas_day", "left")
            .withColumn("revenue_dollars", F.round(F.col("actual_e3m3") * 38.0 * F.coalesce(F.col("gas_price_gj"), F.lit(10.0)), 0))
            .withColumn("nomination_delivery_pct", F.round(100 * F.col("actual_e3m3") / F.when(F.col("nominated_e3m3") > 0, F.col("nominated_e3m3")).otherwise(F.lit(None)), 1))
            .orderBy(F.col("gas_day").desc()))


@dp.table(name="gld_weather", comment="Weather/environment/geospatial context by day: ambient temperature, wind, lightning and flood risk, with field geolocation. Drives compression energy, dewpoint margin, flaring and HSE. Serves the Weather & Environment surface.")
def gld_weather():
    return (spark.read.table("slv_weather")
            .select(F.to_date("obs_date").alias("obs_date"), "area_code", "ambient_c", "wind_kmh", "lightning_risk", "flood_risk", "lat", "lon")
            .orderBy(F.col("obs_date").desc()))


@dp.table(name="gld_workorders", comment="EAM work orders (Maximo/SAP PM) joined to asset context and to the assigned crew (HR), with the scheduled completion and an overdue flag. Non-real-time context. Serves the Work Orders & Reliability surface.")
def gld_workorders():
    wo = spark.read.table("slv_workorders")
    crew = spark.read.table("slv_workforce").select(F.col("crew_id").alias("assigned_crew_id"), F.col("name").alias("assigned_to"))
    return (wo.join(F.broadcast(crew), "assigned_crew_id", "left")
            .withColumn("overdue", (F.col("status").isin("open", "in_progress")) & (F.to_timestamp("target_completion") < F.current_timestamp()))
            .select("wo_id", "asset_id", "area_code", "equipment_type", "criticality", "wo_type", "priority",
                    "status", "assigned_to", "target_completion", "overdue", "description",
                    F.to_date("created_date").alias("created_date"), "cost")
            .orderBy(F.col("created_date").desc()))


# ===== GLD-03 cross-BU fusions (realistic integration keys only) =============================
@dp.table(name="gld_produced_water", comment="Produced-water management: per well per gas day, water volume = gas rate x the well's PVT water-gas ratio, with reinjection split and disposal cost. CSG water is a major cost + compliance driver. Join key: well_id (production <-> PVT).")
def gld_produced_water():
    w = spark.read.table("gld_well_scorecard").select("well_id", "gas_day", "pad_area", "completion_type", "produced_e3m3")
    pvt = spark.read.table("slv_pvt").where(F.col("is_current")).select("well_id", "wgr_bbl_mmscf")
    return (w.join(F.broadcast(pvt), "well_id", "left")
            .withColumn("water_m3", F.round(F.col("produced_e3m3") * F.coalesce(F.col("wgr_bbl_mmscf"), F.lit(5.0)) * 0.005615, 1))
            .withColumn("reinjected_m3", F.round(F.col("water_m3") * 0.7, 1))
            .withColumn("treated_m3", F.round(F.col("water_m3") * 0.3, 1))
            .withColumn("disposal_cost_dollars", F.round(F.col("water_m3") * 0.3 * 2.5 + F.col("water_m3") * 0.7 * 0.6, 0))
            .orderBy(F.col("gas_day").desc(), "well_id"))


@dp.table(name="gld_energy_efficiency", comment="Fuel-gas and energy efficiency by gas day: own-use fuel gas, flaring, and their share of production. Cutting own-use/flaring frees salable gas and lowers Scope 1. Join key: gas_day.")
def gld_energy_efficiency():
    # every plant's daily fuel gas and flare volume (each meter's daily average), summed across the plants
    fuel = (spark.read.table("slv_telemetry_process").where(F.col("signal") == "fuel_gas_flow")
            .groupBy("gas_day", "asset_id").agg(F.avg("value_norm").alias("v"))
            .groupBy("gas_day").agg(F.round(F.sum("v"), 1).alias("fuel_gas_e3m3")))
    flare = (spark.read.table("slv_emissions_ts").where(F.col("signal") == "flare_flow")
             .groupBy("gas_day", "asset_id").agg(F.avg("value_norm").alias("v"))
             .groupBy("gas_day").agg(F.round(F.sum("v"), 1).alias("flare_e3m3")))
    prod = spark.read.table("slv_allocation").groupBy("gas_day").agg(F.round(F.sum("produced_e3m3"), 1).alias("field_e3m3"))
    return (prod.join(fuel, "gas_day", "left").join(flare, "gas_day", "left")
            .withColumn("own_use_pct", F.round(100 * F.coalesce(F.col("fuel_gas_e3m3"), F.lit(0)) / F.when(F.col("field_e3m3") > 0, F.col("field_e3m3")), 2))
            .withColumn("flaring_pct", F.round(100 * F.coalesce(F.col("flare_e3m3"), F.lit(0)) / F.when(F.col("field_e3m3") > 0, F.col("field_e3m3")), 3))
            .orderBy(F.col("gas_day").desc()))


@dp.table(name="gld_carbon_compliance", comment="Carbon / Safeguard-Mechanism position by facility and reporting year: Scope-1 CO2 vs a best-practice baseline, surplus/shortfall and the ACCU cost to cover a shortfall at the carbon price. Join key: facility (area) + reporting year.")
def gld_carbon_compliance():
    cp = spark.read.table("slv_prices").agg(F.avg("carbon_price_t").alias("cp"))
    # each facility's Scope-1 CO2 against a baseline on the gas it processed (its throughput x the days reported)
    nf = _node_flow().withColumnRenamed("node_id", "area_code")
    e = (spark.read.table("gld_emissions_ledger").withColumn("reporting_year", F.year("gas_day"))
         .groupBy("area_code", "reporting_year")
         .agg(F.round(F.sum("co2_kg") / 1000.0, 0).alias("scope1_t"), F.countDistinct("gas_day").alias("days"))
         .join(F.broadcast(nf), "area_code", "left")
         .withColumn("prod_e3m3", F.coalesce(F.col("node_flow"), F.lit(0.0)) * F.col("days")))
    return (e.crossJoin(cp)
            .withColumn("baseline_t", F.round(F.col("prod_e3m3") * F.lit(0.050), 0))
            .withColumn("position_t", F.round(F.col("scope1_t") - F.col("prod_e3m3") * F.lit(0.050), 0))
            .withColumn("accu_cost_dollars", F.round(F.greatest(F.lit(0.0), F.col("scope1_t") - F.col("prod_e3m3") * F.lit(0.050)) * F.coalesce(F.col("cp"), F.lit(32.0)), 0))
            .select("reporting_year", F.col("area_code").alias("facility"), "scope1_t", "baseline_t", "position_t", "accu_cost_dollars")
            .orderBy(F.col("reporting_year").desc(), "facility"))


@dp.table(name="gld_methane_ldar", comment="Methane LDAR inventory: measured CH4 (CEMS) plus factor-based fugitive CH4 per component, converted to CO2-e (GWP 28), with detector activations and recoverable gas value. Measurement + emission-factor method keyed on asset / equipment type.")
def gld_methane_ldar():
    gp = spark.read.table("slv_prices").agg(F.avg("gas_price_gj").alias("gp"))
    a = spark.read.table("slv_asset_master").where(F.col("is_current")).select("asset_id", "area_code", "equipment_type")
    meas = spark.read.table("gld_emissions_ledger").groupBy("asset_id").agg(F.round(F.avg("ch4_kg"), 1).alias("meas_ch4_kg_d"))
    det = (spark.read.table("slv_safety_events").where(F.col("event_type").isin("fg_detection", "emission_exceedance"))
           .groupBy("asset_id").agg(F.count("*").alias("detections")))
    fac = (F.when(F.col("equipment_type") == "compressor", 8.0).when(F.col("equipment_type") == "wellhead", 2.0)
           .when(F.col("equipment_type") == "separator", 1.5).when(F.col("equipment_type").isin("teg_dehydration", "amine"), 3.0)
           .when(F.col("equipment_type") == "gathering_line", 0.6).otherwise(0.5))
    return (a.join(meas, "asset_id", "left").join(det, "asset_id", "left").crossJoin(gp)
            .withColumn("ch4_kg_d", F.round(F.coalesce(F.col("meas_ch4_kg_d"), F.lit(0.0)) + fac, 1))
            .withColumn("detections", F.coalesce(F.col("detections"), F.lit(0)))
            .withColumn("co2e_t_yr", F.round(F.col("ch4_kg_d") * 28 * 365 / 1000.0, 1))
            .withColumn("product_value_dollars_yr", F.round(F.col("ch4_kg_d") * 365 / 0.68 / 1000.0 * GJ_PER_E3M3 * F.coalesce(F.col("gp"), F.lit(10.0)), 0))
            .withColumn("survey_status", F.when(F.col("detections") > 0, F.lit("investigate")).when(F.col("ch4_kg_d") > 5, F.lit("monitor")).otherwise(F.lit("ok")))
            .select("asset_id", "area_code", "equipment_type", "ch4_kg_d", "co2e_t_yr", "detections", "product_value_dollars_yr", "survey_status")
            .orderBy("asset_id"))


@dp.table(name="gld_margin", comment="Molecule-to-market margin by gas day: netback (revenue minus lifting + carbon) and netback per GJ, with the contracted take-or-pay commitment and any shortfall. Ties production, cost and price to the trading book via custody delivery point + gas day.")
def gld_margin():
    econ = (spark.read.table("gld_economics").groupBy("gas_day")
            .agg(F.sum("revenue_dollars").alias("revenue"), F.sum("lifting_cost_dollars").alias("lifting"),
                 F.sum("carbon_cost_dollars").alias("carbon"), F.sum("deferred_dollars").alias("deferred"),
                 F.sum("produced_e3m3").alias("prod_e3m3")))
    con = spark.read.table("slv_commercial_contracts").agg(
        F.round(F.sum("daily_qty_e3m3"), 0).alias("committed_e3m3"),
        F.round(F.sum(F.col("daily_qty_e3m3") * F.col("take_or_pay_pct") / 100.0), 0).alias("top_floor_e3m3"))
    return (econ.crossJoin(con)
            .withColumn("netback_dollars", F.round(F.col("revenue") - F.coalesce(F.col("lifting"), F.lit(0)) - F.coalesce(F.col("carbon"), F.lit(0)), 0))
            .withColumn("netback_per_gj", F.round((F.col("revenue") - F.coalesce(F.col("lifting"), F.lit(0)) - F.coalesce(F.col("carbon"), F.lit(0))) / F.when(F.col("prod_e3m3") > 0, F.col("prod_e3m3") * F.lit(GJ_PER_E3M3)), 2))
            .withColumn("take_or_pay_shortfall_e3m3", F.round(F.greatest(F.lit(0.0), F.col("top_floor_e3m3") - F.col("prod_e3m3")), 1))
            .select("gas_day", "revenue", "netback_dollars", "netback_per_gj", F.round(F.col("deferred"), 0).alias("deferred"), "committed_e3m3", "take_or_pay_shortfall_e3m3")
            .orderBy(F.col("gas_day").desc()))


@dp.table(name="gld_spares_risk", comment="Spares & procurement risk: predictive remaining life vs the procurement lead time of the applicable spare. Flags assets whose part lead time beats their RUL. Join key: equipment type + area (maintenance BOM) <-> RUL.")
def gld_spares_risk():
    rul = spark.read.table("gld_pred_rul").select("asset_id", "area_code", "rul_days", "health_score")
    a = spark.read.table("slv_asset_master").where(F.col("is_current")).select("asset_id", "equipment_type")
    # the BINDING spare = the longest-lead part applicable to this equipment+area (its stock drives the risk)
    rk = W.partitionBy("applies_equipment_type", "for_area").orderBy(F.col("lead_time_days").desc(), F.col("on_hand").asc())
    sp = (spark.read.table("slv_spares").withColumn("_rk", F.row_number().over(rk)).where(F.col("_rk") == 1)
          .select("applies_equipment_type", "for_area", "lead_time_days", "on_hand", "part_id", "part_name", "procurement_type", "unit_cost"))
    out = (rul.join(F.broadcast(a), "asset_id", "left")
           .join(sp, (F.col("equipment_type") == sp["applies_equipment_type"]) & (F.col("area_code") == sp["for_area"]), "left")
           .withColumn("buffer_days", F.col("rul_days") - F.col("lead_time_days")))
    return (out.withColumn("exposure",
                           F.when(F.col("lead_time_days").isNull(), F.lit("ok"))
                           .when((F.col("on_hand") == 0) & (F.col("buffer_days") < 30), F.lit("at_risk"))
                           .when(F.col("buffer_days") < 60, F.lit("watch")).otherwise(F.lit("ok")))
            .select("asset_id", "area_code", "equipment_type", "rul_days",
                    F.coalesce(F.col("lead_time_days"), F.lit(0)).alias("lead_time_days"),
                    F.coalesce(F.col("on_hand"), F.lit(0)).alias("on_hand"), "exposure",
                    "part_id", "part_name", "procurement_type", "unit_cost")
            .orderBy("asset_id"))


@dp.table(name="gld_capital_plan", comment="Capital & decline-aware investment: development / workover projects by area with capex, expected uplift, annual value and a payback proxy. Join key: area + planned period <-> decline forecast.")
def gld_capital_plan():
    gp = spark.read.table("slv_prices").agg(F.avg("gas_price_gj").alias("gp"))
    dev = spark.read.table("slv_dev_schedule")
    return (dev.crossJoin(gp)
            .withColumn("annual_value_dollars", F.round(F.col("expected_uplift_e3m3d") * 365 * GJ_PER_E3M3 * F.coalesce(F.col("gp"), F.lit(10.0)), 0))
            .withColumn("payback_years", F.round(F.col("capex_dollars") / F.when(F.col("expected_uplift_e3m3d") * 365 * GJ_PER_E3M3 * F.coalesce(F.col("gp"), F.lit(10.0)) > 0, F.col("expected_uplift_e3m3d") * 365 * GJ_PER_E3M3 * F.coalesce(F.col("gp"), F.lit(10.0))), 2))
            .select("project_id", "area_code", "project_type", "capex_dollars", "expected_uplift_e3m3d", "annual_value_dollars", "payback_years", "planned_date", "status")
            .orderBy("area_code", "planned_date"))


@dp.table(name="gld_notifications", comment="EAM notifications (anomaly / inspection-due / corrosion / operator flags) joined to asset context. Non-real-time context. Serves the Work Orders & Reliability surface.")
def gld_notifications():
    return (spark.read.table("slv_notifications")
            .select("notif_id", "asset_id", "area_code", "equipment_type", "notif_type", "priority",
                    "status", "description", F.to_date("raised_date").alias("raised_date"))
            .orderBy(F.col("raised_date").desc()))


@dp.table(name="gld_failure_history", comment="ISO 14224 failure history joined to asset context: coded failure mode and downtime. Non-real-time reliability context. Serves the Work Orders & Reliability surface.")
def gld_failure_history():
    return (spark.read.table("slv_failure_history")
            .select("failure_id", "asset_id", "area_code", "equipment_type", "criticality",
                    F.to_date("failure_date").alias("failure_date"), "iso14224_code", "failure_mode", "downtime_hours")
            .orderBy(F.col("failure_date").desc()))


# ===== GLD-01 deferment ======================================================================
@dp.table(name="gld_deferment", comment="Deferment ledger with coded cause and dollar impact. Serves the Deferment surface and the Genie deferment question. Volumes 10^3 m^3.")
def gld_deferment():
    return spark.read.table("slv_deferment").select(
        "deferment_id", "gas_day", "asset_id", "well_id", "area_code", "cause_code", "cause_desc",
        "deferred_e3m3", "deferred_dollars", "equipment_type", "criticality").orderBy(F.col("gas_day").desc())


# ===== GLD-01 safety + alarms ================================================================
@dp.table(name="gld_safety_alarms", comment="Safety/alarm rollup by area per gas day: alarm counts, ESD trips, F&G detections, bypasses. Serves the Safety surface and Genie safety.")
def gld_safety_alarms():
    ev = spark.read.table("slv_safety_events")
    return (ev.groupBy("area_code", "gas_day")
            .agg(F.sum(F.when(F.col("event_type") == "alarm", 1).otherwise(0)).alias("alarm_count"),
                 F.sum(F.when(F.col("event_type") == "esd_trip", 1).otherwise(0)).alias("esd_trips"),
                 F.sum(F.when(F.col("event_type") == "fg_detection", 1).otherwise(0)).alias("fg_detections"),
                 F.sum(F.when(F.col("state") == "bypassed", 1).otherwise(0)).alias("bypasses"),
                 F.sum(F.when(F.col("severity") == "critical", 1).otherwise(0)).alias("critical_events"))
            .orderBy(F.col("gas_day").desc()))


# ===== GLD-01 custody allocation (UAF) =======================================================
@dp.table(name="gld_custody_allocation", comment="Custody metering vs allocation reconciliation by meter/period: metered, allocated, unaccounted-for gas (UAF). Serves the Custody tab and Genie custody. Volumes 10^3 m^3.")
def gld_custody_allocation():
    met = (spark.read.table("slv_metering").where(F.col("signal") == "meter_flow")
           .groupBy("asset_id", "gas_day").agg(F.round(F.avg("value_norm"), 1).alias("metered_e3m3")))
    gc = (spark.read.table("slv_metering").where(F.col("signal").isin(["ghv", "wobbe"]))
          .groupBy("asset_id", "gas_day").pivot("signal", ["ghv", "wobbe"]).agg(F.avg("value_norm")))
    # allocated = the back-allocated sales gas custody-transferred at this meter that day (its customers' deliveries)
    site = spark.read.table("slv_asset_master").select("asset_id", F.col("area_code").alias("delivery_point"))
    alloc = (spark.read.table("slv_customer_deliveries").withColumn("gas_day", F.to_date("gas_day"))
             .groupBy("delivery_point", "gas_day").agg(F.round(F.sum("delivered_e3m3"), 1).alias("field_allocated_e3m3")))
    met = met.withColumn("gas_day", F.to_date("gas_day")).join(F.broadcast(site), "asset_id", "left")
    out = (met.join(gc.withColumn("gas_day", F.to_date("gas_day")), ["asset_id", "gas_day"], "left").join(alloc, ["delivery_point", "gas_day"], "left").drop("delivery_point")
           .withColumn("uaf_e3m3", F.round(F.col("metered_e3m3") - F.col("field_allocated_e3m3"), 1))
           .withColumn("uaf_pct", F.round(100 * (F.col("metered_e3m3") - F.col("field_allocated_e3m3")) / F.when(F.col("metered_e3m3") > 0, F.col("metered_e3m3")).otherwise(F.lit(None)), 2)))
    return out.orderBy(F.col("gas_day").desc(), "asset_id")


# ===== GLD-01 integrity / RBI ================================================================
@dp.table(name="gld_integrity_rbi", comment="Risk-based-inspection register: corrosion rate, remaining life, inspection-due, production-at-risk if it slips. Serves the Integrity surface and Genie integrity.")
def gld_integrity_rbi():
    a = spark.read.table("slv_asset_master").select("asset_id", "area_code", "equipment_type", "criticality", "install_year")
    # gas exposed if the asset is out of service: a compressor its own flow, anything else its site's throughput
    nf = _node_flow()
    a = (a.join(F.broadcast(nf.withColumnRenamed("node_id", "asset_id").withColumnRenamed("node_flow", "own")), "asset_id", "left")
          .join(F.broadcast(nf.withColumnRenamed("node_id", "area_code").withColumnRenamed("node_flow", "site")), "area_code", "left"))
    return (a.withColumn("age_years", F.lit(2026) - F.col("install_year"))
            .withColumn("corrosion_rate_mmpy", F.round(0.05 + (F.abs(F.hash("asset_id")) % 30) / 100.0, 3))
            .withColumn("remaining_life_years", F.round(F.greatest(F.lit(0.5), 25 - F.col("age_years") * 0.8 - (F.abs(F.hash("asset_id")) % 5)), 1))
            .withColumn("inspection_due_days", (F.abs(F.hash(F.concat_ws("-", F.col("asset_id"), F.lit("insp")))) % 120) - 15)
            # if its inspection slips: up to 30% of that gas (more for criticality-A kit)
            .withColumn("production_at_risk_e3m3d", F.round(F.coalesce(F.col("own"), F.col("site"), F.lit(0.0))
                        * ((F.abs(F.hash("asset_id")) % 30) / 100.0) * F.when(F.col("criticality") == "A", 1.0).otherwise(0.6), 0))
            .drop("own", "site")
            .withColumn("rbi_status", F.when(F.col("remaining_life_years") < 3, F.lit("high"))
                        .when(F.col("inspection_due_days") < 30, F.lit("due")).otherwise(F.lit("ok"))))


# ===== GLD-01 maintenance / reliability ======================================================
@dp.table(name="gld_maintenance_reliability", comment="Per-asset maintenance + reliability rollup: open/completed WOs, PM compliance, failures, WO cost. Serves the Planning/Maintenance surface and Genie maintenance.")
def gld_maintenance_reliability():
    wo = spark.read.table("slv_workorders")
    agg = (wo.groupBy("asset_id", "area_code", "equipment_type", "criticality")
           .agg(F.sum(F.when(F.col("status").isin("open", "in_progress"), 1).otherwise(0)).alias("open_wos"),
                F.sum(F.when(F.col("status") == "completed", 1).otherwise(0)).alias("completed_wos"),
                F.sum(F.when((F.col("wo_type") == "PM") & (F.col("status") == "completed"), 1).otherwise(0)).alias("pm_completed"),
                F.sum(F.when(F.col("wo_type") == "PM", 1).otherwise(0)).alias("pm_total"),
                F.round(F.sum("cost"), 0).alias("wo_cost")))
    fh = spark.read.table("slv_failure_history").groupBy("asset_id").agg(F.count("*").alias("failures"))
    return (agg.join(fh, "asset_id", "left")
            .withColumn("pm_compliance_pct", F.round(100 * F.col("pm_completed") / F.when(F.col("pm_total") > 0, F.col("pm_total")).otherwise(F.lit(None)), 1))
            .fillna({"failures": 0}))


# ===== GLD-01 economics ======================================================================
@dp.table(name="gld_economics", comment="Per-asset economics by gas day: revenue, deferred dollars, carbon cost, lifting cost, cost-to-produce per GJ. Serves the Cost surface and Genie cost.")
def gld_economics():
    # join prices by day (coalesced to sensible defaults) rather than a scalar collect
    px = spark.read.table("slv_prices").select(F.col("price_date").alias("gas_day"),
                                               F.col("gas_price_gj"), F.col("carbon_price_t"))
    prod = spark.read.table("slv_allocation").groupBy("asset_id", "area_code", "gas_day").agg(F.sum("produced_e3m3").alias("produced_e3m3"))
    defer = spark.read.table("slv_deferment").groupBy("asset_id", "gas_day").agg(F.sum("deferred_dollars").alias("deferred_dollars"))
    emis = spark.read.table("slv_emissions_ts").where(F.col("signal") == "co2_massrate").groupBy("asset_id", "gas_day").agg((F.avg("value_norm") * 24 / 1000.0).alias("co2_t"))
    out = (prod.join(px, "gas_day", "left").join(defer, ["asset_id", "gas_day"], "left").join(emis, ["asset_id", "gas_day"], "left")
           .withColumn("gas_price_gj", F.coalesce(F.col("gas_price_gj"), F.lit(10.0)))
           .withColumn("carbon_price_t", F.coalesce(F.col("carbon_price_t"), F.lit(33.0)))
           .withColumn("revenue_dollars", F.round(F.col("produced_e3m3") * GJ_PER_E3M3 * F.col("gas_price_gj"), 0))
           .withColumn("carbon_cost_dollars", F.round(F.coalesce(F.col("co2_t"), F.lit(0)) * F.col("carbon_price_t"), 0))
           .withColumn("lifting_cost_dollars", F.round(F.col("produced_e3m3") * GJ_PER_E3M3 * F.col("gas_price_gj") * 0.18, 0))
           .withColumn("cost_to_produce_gj", F.round(F.col("gas_price_gj") * 0.18 + F.coalesce(F.col("co2_t"), F.lit(0)) * F.col("carbon_price_t") / F.when(F.col("produced_e3m3") > 0, F.col("produced_e3m3") * GJ_PER_E3M3).otherwise(F.lit(None)), 3))
           .fillna({"deferred_dollars": 0}))
    return out.orderBy(F.col("gas_day").desc(), "asset_id")


# ===== GLD-02 ML output marts (proxies; advisory) ============================================
@dp.table(name="gld_pred_rul", comment="ML proxy: compressor remaining-useful-life and 30-day failure probability (Mosaic AI stand-in). Advisory only.")
def gld_pred_rul():
    ch = spark.read.table("gld_compression_health")
    latest = ch.withColumn("rn", F.row_number().over(W.partitionBy("asset_id").orderBy(F.col("gas_day").desc()))).where("rn=1")
    return latest.select("asset_id", "area_code", F.col("rul_days_proxy").alias("rul_days"),
                         F.round(F.greatest(F.lit(0.0), F.least(F.lit(1.0), (F.lit(120.0) - F.col("rul_days")) / 120.0)), 3).alias("failure_prob_30d"),
                         "health_score", "vibration_overall")


@dp.table(name="gld_pred_liquid_loading", comment="ML proxy: per-well liquid-loading risk score + watch flag (Mosaic AI classifier stand-in). Advisory only.")
def gld_pred_liquid_loading():
    ws = spark.read.table("gld_well_scorecard")
    latest = ws.withColumn("rn", F.row_number().over(W.partitionBy("well_id").orderBy(F.col("gas_day").desc()))).where("rn=1")
    return latest.select("well_id", "asset_id", "pad_area", "lift_type",
                         # a watch well scores 0.6-1.0 and an ok well 0.05-0.35, each by how far it is choked back
                         F.round(F.when(F.col("liquid_loading_watch") == "watch", F.lit(0.6)).otherwise(F.lit(0.05))
                                 + F.when(F.col("liquid_loading_watch") == "watch", F.lit(0.4)).otherwise(F.lit(0.3))
                                 * F.least(F.lit(1.0), F.greatest(F.lit(0.0), F.coalesce((F.lit(70.0) - F.col("choke_position")) / 70.0, F.lit(0)))), 3).alias("risk_score"),
                         "liquid_loading_watch",
                         F.coalesce(F.col("allocated_e3m3"), F.lit(0)).alias("gas_upside_e3m3"))


@dp.table(name="gld_forecast_production", comment="ML proxy: 7-day field production forecast with band (Mosaic AI / ai_forecast stand-in). Advisory only.")
def gld_forecast_production():
    # computed inside the query (no driver-side collect, which sees no data while the pipeline plans)
    fp = spark.read.table("gld_field_production")
    st = fp.agg(F.avg("actual_e3m3").alias("mean"), F.stddev("actual_e3m3").alias("sd"), F.max("gas_day").alias("last"))
    h = spark.range(1, 8).select(F.col("id").cast("int").alias("horizon_day"))
    sd = F.coalesce(F.col("sd"), F.lit(0.0))
    return (st.where(F.col("mean").isNotNull()).crossJoin(h)
            .select(F.col("last").cast("string").alias("from_gas_day"), "horizon_day",
                    F.round("mean", 1).alias("forecast_e3m3"), F.round(F.col("mean") - 1.5 * sd, 1).alias("forecast_lo_e3m3"),
                    F.round(F.col("mean") + 1.5 * sd, 1).alias("forecast_hi_e3m3")))


# ===== helper: latest value per tag (for the 3D overlays and asset detail) ===================
@dp.table(name="gld_tag_latest", comment="Latest value + quality per tag (for 3D telemetry overlays and asset detail).")
def gld_tag_latest():
    parts = []
    for tbl in ["slv_telemetry_process", "slv_telemetry_vibration", "slv_metering", "slv_emissions_ts"]:
        parts.append(spark.read.table(tbl).select("tag_id", "asset_id", "signal", "value_norm", "quality", "event_time"))
    allt = parts[0]
    for p in parts[1:]:
        allt = allt.unionByName(p)
    w = W.partitionBy("tag_id").orderBy(F.col("event_time").desc())
    return (allt.withColumn("rn", F.row_number().over(w)).where("rn=1")
            .select("tag_id", "asset_id", "signal", F.round("value_norm", 3).alias("latest_value"),
                    F.col("quality").alias("latest_quality"), F.col("event_time").alias("latest_ts")))


# ===== App-serving backbone marts (synced to Lakebase) =======================================
# The App reads ONLY Lakebase, and only gold is synced there, so the semantic backbone the App needs
# (tag resolution, the location tree, site names, per-asset telemetry, safety events) is published
# here as gold rather than read from silver.
@dp.table(name="gld_tag_context",
          comment="Every registered tag resolved end to end: tag -> asset -> functional location -> latest value -> ids in every other system (PI historian, EAM, OSDU, P&ID). One row per tag. Serves the App's tag lookup, the location tree and the asset detail, and Genie tag questions. Join key: tag_id.",
          schema="""tag_id STRING NOT NULL COMMENT 'Contextualised tag (AREA-SYSTEM-CLASS-NNNN): the universal join key and 3D hotspot key.',
          asset_id STRING COMMENT 'Asset the tag sits on.',
          area_code STRING COMMENT 'Site (well pad, gathering hub, processing train, custody station).',
          system_code STRING COMMENT 'Process system (WB, AL, GL, SEP, CMP, DEH, AMN, NGL, UTL, FLR, MET).',
          class_code STRING COMMENT 'ISA-5.1 style instrument / equipment class.',
          signal STRING COMMENT 'Measured or derived signal name.',
          unit STRING COMMENT 'Engineering unit.',
          description STRING COMMENT 'Tag description.',
          measured_or_calculated STRING COMMENT 'Whether the value is measured or calculated.',
          cadence STRING COMMENT 'Sampling cadence.',
          eng_range_min DOUBLE COMMENT 'Normal operating range, low end.',
          eng_range_max DOUBLE COMMENT 'Normal operating range, high end.',
          equipment_type STRING COMMENT 'Equipment type of the asset.',
          criticality STRING COMMENT 'Asset criticality.',
          manufacturer STRING COMMENT 'Asset manufacturer.',
          install_year INT COMMENT 'Year the asset was installed.',
          fl_name STRING COMMENT 'Functional location name.',
          fl_level STRING COMMENT 'Functional location level.',
          latest_value DOUBLE COMMENT 'Latest reading.',
          latest_quality STRING COMMENT 'Quality flag of the latest reading.',
          latest_ts TIMESTAMP COMMENT 'Time of the latest reading.',
          historian_tag STRING COMMENT 'PI historian tag.',
          eam_fl STRING COMMENT 'EAM functional location.',
          osdu_ref STRING COMMENT 'OSDU reference.',
          doc_entity STRING COMMENT 'P&ID / document entity.',
          is_stale BOOLEAN COMMENT 'True when the cross-system mapping is stale.'""")
def gld_tag_context():
    t = spark.read.table("slv_tag_registry")
    a = spark.read.table("slv_asset_master").where("is_current").select("asset_id", "equipment_type", "criticality", "manufacturer", "install_year")
    f = spark.read.table("slv_functional_location").where("is_current").select("fl_id", F.col("name").alias("fl_name"), F.col("level").alias("fl_level"))
    l = spark.read.table("gld_tag_latest").select("tag_id", "latest_value", "latest_quality", "latest_ts")
    x = spark.read.table("slv_id_crossmap").select(F.col("entity").alias("tag_id"), "historian_tag", "eam_fl", "osdu_ref", "doc_entity", "is_stale")
    return (t.join(a, "asset_id", "left").join(f, "fl_id", "left").join(l, "tag_id", "left").join(x, "tag_id", "left")
            .select("tag_id", "asset_id", "area_code", "system_code", "class_code", "signal", "unit", "description",
                    "measured_or_calculated", "cadence", "eng_range_min", "eng_range_max", "equipment_type", "criticality",
                    F.col("manufacturer").cast("string").alias("manufacturer"), F.col("install_year").cast("int").alias("install_year"),
                    "fl_name", "fl_level", "latest_value", "latest_quality", "latest_ts",
                    "historian_tag", "eam_fl", "osdu_ref", "doc_entity", "is_stale"))


@dp.table(name="gld_asset_master",
          comment="Current asset register: one row per asset with its site, system, class, equipment type, criticality, maker, install year and tag count. Serves the App's location tree and Genie asset questions. Join key: asset_id.",
          schema="""asset_id STRING NOT NULL COMMENT 'Asset id.',
          fl_id STRING COMMENT 'Functional location the asset is installed at.',
          area_code STRING COMMENT 'Site.',
          system_code STRING COMMENT 'Process system.',
          class_code STRING COMMENT 'Equipment class.',
          equipment_type STRING COMMENT 'Equipment type.',
          criticality STRING COMMENT 'Asset criticality.',
          manufacturer STRING COMMENT 'Manufacturer.',
          install_year INT COMMENT 'Year installed.',
          n_tags BIGINT COMMENT 'Registered tags on the asset.'""")
def gld_asset_master():
    a = spark.read.table("slv_asset_master").where("is_current")
    n = spark.read.table("slv_tag_registry").groupBy("asset_id").agg(F.count("*").alias("n_tags"))
    return (a.join(n, "asset_id", "left")
            .select("asset_id", "fl_id", "area_code", "system_code", "class_code", "equipment_type", "criticality",
                    F.col("manufacturer").cast("string").alias("manufacturer"), F.col("install_year").cast("int").alias("install_year"),
                    F.coalesce(F.col("n_tags"), F.lit(0)).cast("bigint").alias("n_tags")))


@dp.table(name="gld_functional_location",
          comment="Current functional-location hierarchy (field -> site -> system -> position). One row per location. Serves the App's Asset & Document Explorer and Genie. Join key: fl_id.",
          schema="""fl_id STRING NOT NULL COMMENT 'Functional location id.',
          parent_fl_id STRING COMMENT 'Parent location.',
          level STRING COMMENT 'Hierarchy level (Field, Area, System, Position).',
          code STRING COMMENT 'Location code.',
          name STRING COMMENT 'Location name.',
          area_code STRING COMMENT 'Site the location sits in.',
          system_code STRING COMMENT 'Process system.',
          class_code STRING COMMENT 'Equipment class.',
          criticality STRING COMMENT 'Criticality.'""")
def gld_functional_location():
    return (spark.read.table("slv_functional_location").where("is_current")
            .select("fl_id", "parent_fl_id", "level", "code", "name", "area_code", "system_code", "class_code", "criticality"))


@dp.table(name="gld_sites",
          comment="Every site (well pad, gathering hub, processing train, custody station) with its proper name and asset and tag counts. Serves site names on the App's maps, weather tiles and location tree, and Genie. Join key: area_code.",
          schema="""area_code STRING NOT NULL COMMENT 'Site code.',
          name STRING COMMENT 'Site name, e.g. Well pad BAR045 (Bowen Basin).',
          n_assets BIGINT COMMENT 'Current assets at the site.',
          n_tags BIGINT COMMENT 'Registered tags at the site.'""")
def gld_sites():
    nm = spark.read.table("slv_functional_location").where("is_current AND level = 'Area'").groupBy("area_code").agg(F.first("name").alias("name"))
    a = spark.read.table("gld_asset_master").groupBy("area_code").agg(F.count("*").alias("n_assets"), F.sum("n_tags").alias("n_tags"))
    return (a.join(nm, "area_code", "left")
            .select("area_code", F.coalesce("name", "area_code").alias("name"),
                    F.col("n_assets").cast("bigint").alias("n_assets"), F.col("n_tags").cast("bigint").alias("n_tags")))


@dp.table(name="gld_asset_signal_series",
          comment="Per-asset telemetry trend for charting: the average of each process signal over 5-gas-day buckets across the full history (about 146 points per signal). Serves the App's asset-detail telemetry charts and Genie trend questions. Key: asset_id + signal + gas_day.",
          schema="""asset_id STRING NOT NULL COMMENT 'Asset id.',
          signal STRING NOT NULL COMMENT 'Process signal.',
          gas_day DATE NOT NULL COMMENT 'First gas day of the 5-day bucket.',
          unit STRING COMMENT 'Engineering unit.',
          value DOUBLE COMMENT 'Average value over the bucket.'""")
def gld_asset_signal_series():
    t = spark.read.table("slv_telemetry_process").where(F.col("value").isNotNull())
    epoch = F.to_date(F.lit("2000-01-01"))
    b = F.date_add(epoch, (F.floor(F.datediff(F.col("gas_day"), epoch) / 5) * 5).cast("int"))
    return (t.withColumn("bucket", b).groupBy("asset_id", "signal", "bucket")
            .agg(F.max("unit").alias("unit"), F.round(F.avg("value"), 3).alias("value"))
            .select("asset_id", "signal", F.col("bucket").alias("gas_day"), "unit", "value"))


@dp.table(name="gld_safety_events",
          comment="Safety events worth an operator's attention: every ESD trip, F&G detection and emission exceedance, plus any other critical alarm, with a readable event label and the asset context. One row per event. Serves the App's Safety & Alarms list and Genie. Join key: event_id.",
          schema="""event_id STRING NOT NULL COMMENT 'Event id.',
          event_time TIMESTAMP COMMENT 'When the event happened.',
          event_type STRING COMMENT 'Event type code.',
          event STRING COMMENT 'Readable event: ESD trip, F&G detection, Emission exceedance or Critical alarm.',
          asset_id STRING COMMENT 'Asset the event is on.',
          area_code STRING COMMENT 'Site.',
          equipment_type STRING COMMENT 'Equipment type.',
          severity STRING COMMENT 'Severity (critical / warning / advisory).',
          gas_day DATE COMMENT 'Gas day of the event.'""")
def gld_safety_events():
    ev = spark.read.table("slv_safety_events").where(
        F.col("event_type").isin("esd_trip", "fg_detection", "emission_exceedance") | (F.col("severity") == "critical"))
    label = (F.when(F.col("event_type") == "esd_trip", "ESD trip").when(F.col("event_type") == "fg_detection", "F&G detection")
             .when(F.col("event_type") == "emission_exceedance", "Emission exceedance").otherwise("Critical alarm"))
    return ev.select("event_id", "event_time", "event_type", label.alias("event"), "asset_id", "area_code",
                     "equipment_type", "severity", "gas_day")


# ===== GLD-03 app-serving marts (Lakebase-synced, write-back) ================================
@dp.table(name="gld_asset_state", comment="Per-asset current health + 3D pin colour + geo + production-at-risk + open WOs/trips. The App's primary serving mart (synced to Lakebase). Advisory only.")
def gld_asset_state():
    am = spark.read.table("slv_asset_master").select(
        "asset_id", "area_code", "system_code", "equipment_type", "criticality", "scene_id",
        "hotspot_x", "hotspot_y", "gps_lat", "gps_lon")
    # compressor health where available
    ch = spark.read.table("gld_compression_health")
    ch = ch.withColumn("rn", F.row_number().over(W.partitionBy("asset_id").orderBy(F.col("gas_day").desc()))).where("rn=1") \
           .select("asset_id", "health_score", "production_at_risk_e3m3d", "vibration_overall")
    # open WOs + recent trips
    wo = spark.read.table("slv_workorders").groupBy("asset_id").agg(
        F.sum(F.when(F.col("status").isin("open", "in_progress"), 1).otherwise(0)).alias("open_wos"),
        F.sum(F.when((F.col("priority") == "P1") & (F.col("status").isin("open", "in_progress")), 1).otherwise(0)).alias("open_p1"))
    # protective trips in the last 90 days count against health (an old trip, long since investigated, does not)
    trips = (spark.read.table("slv_safety_events").where("event_type='esd_trip'")
             .where(F.col("event_time") >= F.date_sub(F.current_date(), 90)).groupBy("asset_id").agg(F.count("*").alias("trips")))
    tl = spark.read.table("gld_tag_latest").where("signal='vibration_overall'").select("asset_id", F.col("latest_value").alias("latest_vibration"))
    out = (am.join(ch, "asset_id", "left").join(wo, "asset_id", "left").join(trips, "asset_id", "left").join(tl, "asset_id", "left")
           .fillna({"open_wos": 0, "open_p1": 0, "trips": 0}))
    # generic health for non-compressors: start 100, penalise open WOs / trips
    out = out.withColumn("health_score", F.coalesce(F.col("health_score"),
                         F.greatest(F.lit(0.0), 100 - F.col("open_p1") * 30 - F.col("open_wos") * 6 - F.col("trips") * 12)))
    out = (out
           .withColumn("production_at_risk_e3m3d", F.coalesce(F.col("production_at_risk_e3m3d"), F.lit(0.0)))
           .withColumn("pin_colour", F.when((F.col("health_score") < 40) | (F.col("open_p1") > 0), F.lit("red"))
                       .when((F.col("health_score") < 70) | (F.col("open_wos") > 0), F.lit("amber")).otherwise(F.lit("green")))
           .withColumn("is_red", (F.col("pin_colour") == "red").cast("int"))
           .withColumn("is_amber", (F.col("pin_colour") == "amber").cast("int")))
    return out


@dp.table(name="gld_constraint_ranking", comment="Ranked field constraints right now (what is limiting the field). Serves the Command Centre and Genie ('the field constraint'). Ranked by production_at_risk_e3m3d.")
def gld_constraint_ranking():
    ch = spark.read.table("gld_compression_health")
    ch = ch.withColumn("rn", F.row_number().over(W.partitionBy("asset_id").orderBy(F.col("gas_day").desc()))).where("rn=1")
    comp = ch.select(F.lit("compression").alias("constraint_type"), "asset_id", "area_code", "health_score", "production_at_risk_e3m3d")
    ll = spark.read.table("gld_pred_liquid_loading").where("liquid_loading_watch='watch'").select(
        F.lit("liquid loading").alias("constraint_type"), F.col("asset_id"), F.col("pad_area").alias("area_code"),
        (F.lit(100.0) - F.col("risk_score") * 100).alias("health_score"), F.col("gas_upside_e3m3").alias("production_at_risk_e3m3d"))
    uaf = (spark.read.table("gld_custody_allocation")
           .withColumn("rn", F.row_number().over(W.partitionBy("asset_id").orderBy(F.col("gas_day").desc()))).where("rn=1")
           .where("abs(uaf_pct) > 2")   # metered and allocated diverge either way (a meter reading low or high)
           .select(F.lit("custody UAF").alias("constraint_type"), "asset_id", F.lit(None).cast("string").alias("area_code"),
                   F.lit(None).cast("double").alias("health_score"), F.abs(F.col("uaf_e3m3")).alias("production_at_risk_e3m3d")))
    allc = comp.unionByName(ll).unionByName(uaf)
    w = W.orderBy(F.col("production_at_risk_e3m3d").desc_nulls_last())
    return allc.withColumn("rank", F.row_number().over(w)).orderBy("rank")


@dp.table(name="gld_recommendations", comment="Ranked advisory actions (shadow mode, human-in-the-loop). advisory_only=true and the twin never actuates (Design Part 4). Serves the Recommendation Inbox; synced to Lakebase for accept/reject/annotate write-back.")
def gld_recommendations():
    cr = spark.read.table("gld_constraint_ranking").limit(12)
    reco = (cr
            .withColumn("rec_id", F.concat_ws("|", F.col("asset_id"), F.col("constraint_type")))
            .withColumn("advisory_only", F.lit(True))
            .withColumn("hitl_status", F.lit("proposed"))
            .withColumn("maturity_level", F.when(F.col("constraint_type") == "compression", F.lit("prescriptive (shadow)")).otherwise(F.lit("predictive")))
            .withColumn("recommendation", F.when(F.col("constraint_type") == "compression",
                        F.concat(F.lit("Raise a priority spare order and pull the fly-in crew forward; de-rate "), F.col("asset_id"), F.lit(" to buy time (rising vibration; efficiency loss; take-or-pay at risk).")))
                        .when(F.col("constraint_type") == "liquid loading",
                        F.concat(F.lit("Intervene on "), F.col("asset_id"), F.lit(": velocity string / foam / plunger to clear liquid loading (ranked by gas upside).")))
                        .otherwise(F.concat(F.lit("Investigate "), F.col("constraint_type"), F.lit(" on "), F.col("asset_id"), F.lit(": reconcile metered vs allocated and prove the meter."))))
            .withColumn("evidence", F.when(F.col("constraint_type") == "compression",
                        F.lit("Cross-domain: open P1 WO + bearing spare 6 weeks out + only certified tech on leave 3 days + firmware change 2 days pre-anomaly + take-or-pay volume at risk.")).otherwise(F.lit("Derived from gold marts; advisory only."))))
    return reco.select("rec_id", "asset_id", "constraint_type", "recommendation", "production_at_risk_e3m3d",
                       "advisory_only", "hitl_status", "maturity_level", "evidence")


# ===== GLD impact analysis: supply network + customers ========================================
# Flows are the average gas day over the latest 30 gas days, so the Sankey, the customer list and the
# impact PDF all describe the same recent operating state.
STOP_ORDER = {"reservoir": 1, "wells": 2, "gathering": 3, "processing": 4, "compression": 5, "custody": 6, "customer": 7}


def _last_days(df, n=30):
    d = df.withColumn("_d", F.to_date("gas_day"))
    mx = d.agg(F.max("_d").alias("mx"))
    return d.crossJoin(mx).where(F.col("_d") > F.date_sub(F.col("mx"), n)).drop("mx")


@dp.table(name="gld_customer_supply",
          comment="Customer supply position (fictional buyers): contract terms (MDQ, annual quantity, price, take-or-pay, curtailment priority) with the latest-30-gas-day average delivered vs nominated energy, utilisation, revenue per day and 12-month volume. Join key: delivery_point (custody meter) + customer_id. Serves impact analysis + Genie.",
          schema="""customer_id STRING COMMENT 'Customer id.',
          customer_name STRING COMMENT 'Buyer (fictional).',
          segment STRING COMMENT 'Customer segment (retail, power generation, industrial, LNG export, mining, distribution, spot market).',
          location STRING COMMENT 'Where the customer takes the gas.',
          delivery_point STRING COMMENT 'Custody meter the gas is delivered at.',
          contract_id STRING COMMENT 'GSA / LNG SPA id.',
          contract_type STRING COMMENT 'Firm GSA, LNG SPA, Interruptible or Spot.',
          priority INT COMMENT 'Curtailment priority: 1 firm (curtailed last) .. 3 spot (curtailed first).',
          mdq_tj DOUBLE COMMENT 'Maximum daily quantity (TJ/d).',
          acq_pj DOUBLE COMMENT 'Annual contract quantity (PJ/yr).',
          price_gj DOUBLE COMMENT 'Contract price (AUD/GJ); for spot buyers the average price received over the window.',
          take_or_pay_pct INT COMMENT 'Take-or-pay floor (% of MDQ); 0 = none.',
          contract_end STRING COMMENT 'Contract end date.',
          delivered_tj_d DOUBLE COMMENT 'Average energy delivered per gas day, latest 30 days (TJ/d).',
          nominated_tj_d DOUBLE COMMENT 'Average nomination per gas day, latest 30 days (TJ/d).',
          delivered_e3m3_d DOUBLE COMMENT 'Average volume delivered per gas day, latest 30 days (10^3 m^3/d).',
          utilisation_pct DOUBLE COMMENT 'Delivered as a share of MDQ (%).',
          revenue_per_day DOUBLE COMMENT 'Average revenue per gas day, latest 30 days (AUD/d).',
          delivered_pj_12m DOUBLE COMMENT 'Energy delivered over the latest 365 gas days (PJ).',
          shortfall_days_30d INT COMMENT 'Gas days in the window delivered below nomination.',
          as_of_day STRING COMMENT 'Latest gas day in the delivery data: the end of the 30-day window.'""")
def gld_customer_supply():
    c = spark.read.table("slv_customers")
    dl = spark.read.table("slv_customer_deliveries")
    w30 = _last_days(dl, 30).groupBy("customer_id").agg(
        F.avg("delivered_tj").alias("delivered_tj_d"), F.avg("nominated_tj").alias("nominated_tj_d"),
        F.avg("delivered_e3m3").alias("delivered_e3m3_d"), F.avg("revenue_dollars").alias("revenue_per_day"),
        F.avg("price_gj").alias("px"),
        F.sum(F.when(F.col("delivered_tj") < F.col("nominated_tj") - 0.5, 1).otherwise(0)).alias("short"))
    y = _last_days(dl, 365).groupBy("customer_id").agg((F.sum("delivered_tj") / 1000.0).alias("pj12"))
    o = c.join(w30, "customer_id", "left").join(y, "customer_id", "left").crossJoin(dl.agg(F.max("gas_day").alias("as_of_day")))
    return o.select(
        "customer_id", "customer_name", "segment", "location", "delivery_point", "contract_id", "contract_type",
        F.col("priority").cast("int").alias("priority"), F.round("mdq_tj", 1).alias("mdq_tj"), F.round("acq_pj", 2).alias("acq_pj"),
        F.round(F.coalesce("price_gj", "px"), 2).alias("price_gj"), F.col("take_or_pay_pct").cast("int").alias("take_or_pay_pct"),
        "contract_end", F.round("delivered_tj_d", 1).alias("delivered_tj_d"), F.round("nominated_tj_d", 1).alias("nominated_tj_d"),
        F.round("delivered_e3m3_d", 0).alias("delivered_e3m3_d"),
        F.round(F.col("delivered_tj_d") / F.col("mdq_tj") * 100, 1).alias("utilisation_pct"),
        F.round("revenue_per_day", 0).alias("revenue_per_day"), F.round("pj12", 2).alias("delivered_pj_12m"),
        F.coalesce(F.col("short"), F.lit(0)).cast("int").alias("shortfall_days_30d"),
        F.col("as_of_day").cast("string").alias("as_of_day"))


@dp.table(name="gld_supply_network",
          comment="Supply network with flow: every link in the physical gas path (reservoir -> well pad -> gathering hub -> processing train -> compressor -> custody meter -> customer) with its average flow over the latest 30 gas days. Pad flows come from back-allocated production; meter-to-customer flows from custody deliveries. Serves the impact-analysis Sankey.",
          schema="""from_id STRING COMMENT 'Upstream node id.',
          from_name STRING COMMENT 'Upstream node name.',
          from_stop STRING COMMENT 'Upstream value-chain stop.',
          to_id STRING COMMENT 'Downstream node id.',
          to_name STRING COMMENT 'Downstream node name.',
          to_stop STRING COMMENT 'Downstream value-chain stop.',
          basin STRING COMMENT 'Gas basin the link sits in.',
          flow_e3m3d DOUBLE COMMENT 'Average gas flow on the link, latest 30 gas days (10^3 m^3/d).',
          flow_tj_d DOUBLE COMMENT 'The same flow as energy (TJ/d, at 38 GJ per 10^3 m^3).'""")
def gld_supply_network():
    e = spark.read.table("slv_supply_network")
    al = _last_days(spark.read.table("slv_allocation"), 30)
    # average gas day per pad: total over the window / days in it (counted in the query, not on the driver,
    # where a pipeline sees no data while it plans and the average became a 30-day total)
    pad = al.groupBy("area_code").agg((F.sum("allocated_e3m3") / F.countDistinct("_d")).alias("pf"))

    def step(stop, inflow, key="from_id"):
        return (e.where(F.col("from_stop") == stop).join(inflow, e[key] == inflow["nid"], "inner")
                .withColumn("flow", F.col("nf") * F.coalesce(F.col("share"), F.lit(1.0))).drop("nid", "nf"))

    padin = pad.select(F.col("area_code").alias("nid"), F.col("pf").alias("nf"))
    res_pad = (e.where(F.col("from_stop") == "reservoir").join(padin, e["to_id"] == padin["nid"], "inner")
               .withColumn("flow", F.col("nf")).drop("nid", "nf"))
    pad_hub = step("wells", padin)
    agg = lambda df: df.groupBy(F.col("to_id").alias("nid")).agg(F.sum("flow").alias("nf"))
    hub_tr = step("gathering", agg(pad_hub))
    tr_cmp = step("processing", agg(hub_tr))
    cmp_met = step("compression", agg(tr_cmp))
    # scale compressor->meter links so each meter's inflow equals what its customers received (processing shrink)
    dl = _last_days(spark.read.table("slv_customer_deliveries"), 30)
    cust = dl.groupBy("customer_id", "delivery_point").agg((F.sum("delivered_e3m3") / F.countDistinct("_d")).alias("cf"))
    met_out = cust.groupBy("delivery_point").agg(F.sum("cf").alias("mo"))
    met_in = cmp_met.groupBy("to_id").agg(F.sum("flow").alias("mi"))
    k = met_in.join(met_out, met_in["to_id"] == met_out["delivery_point"]).select(F.col("to_id").alias("kid"), (F.col("mo") / F.col("mi")).alias("k"))
    cmp_met = cmp_met.join(k, cmp_met["to_id"] == k["kid"], "left").withColumn("flow", F.col("flow") * F.coalesce(F.col("k"), F.lit(1.0))).drop("kid", "k")
    met_cus = (e.where(F.col("from_stop") == "custody").join(cust, e["to_id"] == cust["customer_id"], "inner")
               .withColumn("flow", F.col("cf")).drop("customer_id", "delivery_point", "cf"))
    cols = ["from_id", "from_name", "from_stop", "to_id", "to_name", "to_stop", "basin", "flow"]
    out = res_pad.select(*cols)
    for part in (pad_hub, hub_tr, tr_cmp, cmp_met, met_cus):
        out = out.unionByName(part.select(*cols))
    return out.select(*cols[:-1], F.round("flow", 1).alias("flow_e3m3d"), F.round(F.col("flow") * GJ_PER_E3M3 / 1000.0, 2).alias("flow_tj_d"))


@dp.table(name="gld_supply_nodes",
          comment="Supply-network nodes: every stop in the gas path (reservoirs, well pads, gathering hubs, processing trains, compressors, custody meters, customers) with its column order, throughput and current health (worst asset state on the stop; customers amber when short of nomination). Serves the impact-analysis Sankey.",
          schema="""node_id STRING COMMENT 'Node id (reservoir, area code, compressor asset id or customer id).',
          node_name STRING COMMENT 'Node name.',
          stop STRING COMMENT 'Value-chain stop: reservoir, wells, gathering, processing, compression, custody, customer.',
          stop_order INT COMMENT 'Column order in the gas path (1 reservoir .. 7 customer).',
          basin STRING COMMENT 'Gas basin.',
          throughput_e3m3d DOUBLE COMMENT 'Gas through the node, latest 30 gas days (10^3 m^3/d): the larger of its inflow and outflow.',
          throughput_tj_d DOUBLE COMMENT 'The same throughput as energy (TJ/d).',
          status STRING COMMENT 'Health: red, amber or green.'""")
def gld_supply_nodes():
    n = spark.read.table("gld_supply_network")
    outs = n.groupBy(F.col("from_id").alias("node_id")).agg(F.first("from_name").alias("nm_o"), F.first("from_stop").alias("st_o"),
                                                             F.first("basin").alias("b_o"), F.sum("flow_e3m3d").alias("fo"))
    ins = n.groupBy(F.col("to_id").alias("node_id")).agg(F.first("to_name").alias("nm_i"), F.first("to_stop").alias("st_i"),
                                                          F.first("basin").alias("b_i"), F.sum("flow_e3m3d").alias("fi"))
    nodes = outs.join(ins, "node_id", "full_outer")
    st = spark.read.table("gld_asset_state")
    worst = lambda df, key: df.groupBy(F.col(key).alias("sid")).agg(
        F.when(F.max(F.col("pin_colour") == "red").cast("int") == 1, "red")
         .when(F.max(F.col("pin_colour") == "amber").cast("int") == 1, "amber").otherwise("green").alias("s"))
    area_s, asset_s = worst(st, "area_code"), worst(st, "asset_id")
    cs = spark.read.table("gld_customer_supply").select(F.col("customer_id").alias("sid"),
                                                        F.when(F.col("shortfall_days_30d") > 0, "amber").otherwise("green").alias("s"))
    stat = area_s.unionByName(asset_s).unionByName(cs).groupBy("sid").agg(F.first("s").alias("s"))
    om = F.create_map(*[x for kv in STOP_ORDER.items() for x in (F.lit(kv[0]), F.lit(kv[1]))])
    o = (nodes.join(stat, nodes["node_id"] == stat["sid"], "left")
         .withColumn("stop", F.coalesce("st_o", "st_i"))
         .withColumn("tp", F.greatest(F.coalesce("fo", F.lit(0.0)), F.coalesce("fi", F.lit(0.0)))))
    return o.select("node_id", F.coalesce("nm_o", "nm_i").alias("node_name"), "stop",
                    om[F.col("stop")].cast("int").alias("stop_order"), F.coalesce("b_o", "b_i").alias("basin"),
                    F.round("tp", 1).alias("throughput_e3m3d"), F.round(F.col("tp") * GJ_PER_E3M3 / 1000.0, 2).alias("throughput_tj_d"),
                    F.coalesce(F.col("s"), F.lit("green")).alias("status"))


# ===== GLD commercial actions: the top moves across the Commercial & Finance reports ==========
# One candidate action per commercial lever (deferment, capital, contract supply, contract price, carbon,
# methane), each valued on one common basis (AUD at stake per year), then ranked. The App's Commercial &
# Finance briefing shows the top three and Genie explains them. Pure DataFrame logic (no driver collects).
def _money(c):
    """Human dollars for action text: $108M, $4.2M, $95k."""
    a = F.abs(c)
    return (F.when(a >= 1e9, F.format_string("$%.1fB", c / 1e9)).when(a >= 1e8, F.format_string("$%.0fM", c / 1e6))
            .when(a >= 1e6, F.format_string("$%.1fM", c / 1e6)).when(a >= 1e3, F.format_string("$%.0fk", c / 1e3))
            .otherwise(F.format_string("$%.0f", c)))


def _site_name(c):
    """A site's name for a sentence: drop the '(place, basin)' suffix; lower-case a generic lead ('well pad ...')."""
    s = F.regexp_replace(c, r"\s*\([^)]*\)\s*$", "")
    return F.when(s.rlike("^(Well pad|Wellhead platform|Gathering hub|Manifold)"),
                  F.concat(F.lower(F.substring(s, 1, 1)), F.substring(s, 2, 200))).otherwise(s)


def _day(c):
    return F.date_format(F.to_date(c), "d MMMM yyyy")


@dp.table(name="gld_commercial_actions",
          comment="Top commercial actions: one candidate per commercial lever (recover the largest deferment cause, approve the best-paying capital project, close the supply gap on short contracts, re-price an under-market contract at renewal, cover the Safeguard shortfall, repair methane leaks), each valued as AUD at stake per year and ranked. Serves the App's Commercial & Finance briefing (top 3) and Genie's commercial-actions answers. Advisory only.",
          schema="""rank INT COMMENT 'Rank by value at stake (1 = the most valuable action).',
          lever STRING COMMENT 'Commercial lever: deferment, capital, contract_supply, contract_price, carbon or methane.',
          action STRING COMMENT 'The action, as an imperative headline.',
          detail STRING COMMENT 'Why: the evidence behind the action, with its figures.',
          value_dollars_yr DOUBLE COMMENT 'Value at stake (AUD per year) on the basis in value_basis; the common ranking currency.',
          value_basis STRING COMMENT 'How value_dollars_yr is worked out.',
          module_key STRING COMMENT 'App report where the evidence lives (opened from the briefing).',
          module_label STRING COMMENT 'Name of that report.',
          focus_ids STRING COMMENT 'Ids to highlight on that report (rows, bars), separated by |.',
          focus_kpis STRING COMMENT 'KPI tile labels (lower-case fragments) to highlight on that report, separated by |.',
          data_as_of STRING COMMENT 'Latest gas day in the data behind the actions.'""")
def gld_commercial_actions():
    sites = spark.read.table("gld_sites").select("area_code", _site_name(F.col("name")).alias("site"))
    asof = spark.read.table("gld_economics").agg(F.max("gas_day").cast("string").alias("data_as_of"))

    # 1. deferment: the cause that lost the most money in the last 30 gas days, where most of it happened
    d = _last_days(spark.read.table("gld_deferment"), 30)
    tot = d.agg(F.sum("deferred_dollars").alias("tot"))
    cause = (d.groupBy("cause_code", "cause_desc").agg(F.sum("deferred_dollars").alias("usd"))
             .withColumn("_r", F.row_number().over(W.orderBy(F.desc("usd")))).where("_r = 1").drop("_r"))
    at = (d.join(cause.select("cause_code"), "cause_code").groupBy("area_code").agg(F.sum("deferred_dollars").alias("site_usd"))
          .withColumn("_r", F.row_number().over(W.orderBy(F.desc("site_usd")))).where("_r = 1").drop("_r").join(sites, "area_code", "left"))
    ids = d.join(cause.select("cause_code"), "cause_code").agg(F.concat_ws("|", F.array_distinct(F.collect_list("asset_id"))).alias("aids"))
    short = F.regexp_replace(F.col("cause_desc"), r"\s*\([^)]*\)\s*$", "")
    head = (F.when(F.col("cause_code") == "COMP-CONSTRAINT", F.lit("Restore compression capacity at "))
            .when(F.col("cause_code") == "WELL-TRIP", F.lit("Stop the repeat well trips at "))
            .when(F.col("cause_code") == "WELL-LL", F.lit("Unload the liquid-loading wells at "))
            .when(F.col("cause_code") == "RES-DECL", F.lit("Re-plan the declining wells at "))
            .when(F.col("cause_code") == "UNPLANNED-SD", F.lit("Fix the causes of unplanned shutdowns at "))
            .otherwise(F.lit("Recover the deferred gas at ")))
    where = F.when(F.col("site_usd") >= F.col("usd") * 0.995, F.concat(F.lit("all of it at "), F.coalesce("site", "area_code")))\
             .otherwise(F.concat(_money(F.col("site_usd")), F.lit(" of it at "), F.coalesce("site", "area_code")))
    defer = (cause.crossJoin(tot).crossJoin(at).crossJoin(ids).select(
        F.lit("deferment").alias("lever"),
        F.concat(head, F.coalesce("site", "area_code")).alias("action"),
        F.format_string("%s deferred %s of gas in the last 30 days (%d%% of all deferment), %s. Recovering it is worth about %s a year.",
                        short, _money(F.col("usd")), F.round(F.col("usd") / F.col("tot") * 100).cast("int"), where,
                        _money(F.col("usd") * 365 / 30)).alias("detail"),
        (F.col("usd") * 365 / 30).alias("value_dollars_yr"),
        F.lit("This cause's deferment over the last 30 gas days, annualised").alias("value_basis"),
        F.lit("deferment").alias("module_key"), F.lit("Deferment & Losses").alias("module_label"),
        F.concat_ws("|", F.col("cause_desc"), F.col("aids")).alias("focus_ids"),
        F.lit("total deferment").alias("focus_kpis")))

    # 2. capital: the most valuable project that pays back inside a year but is not yet approved
    cp = (spark.read.table("gld_capital_plan").where(F.col("status").isin("proposed", "fel-2") & (F.col("payback_years") < 1))
          .withColumn("_r", F.row_number().over(W.orderBy(F.desc("annual_value_dollars")))).where("_r = 1").drop("_r")
          .join(sites, "area_code", "left"))
    stage = F.when(F.col("status") == "fel-2", F.lit("at FEL-2 (concept select)")).otherwise(F.lit("only proposed"))
    capital = cp.select(
        F.lit("capital").alias("lever"),
        F.concat(F.lit("Approve "), F.col("project_id"), F.lit(", the "), F.regexp_replace("project_type", "_", " "),
                 F.lit(" at "), F.coalesce("site", "area_code")).alias("action"),
        F.format_string("%s of capex adds %.0f ×10³ m³/d of gas, worth %s a year, and pays back in %.1f months. It is still %s, with a planned start of %s.",
                        _money(F.col("capex_dollars")), F.col("expected_uplift_e3m3d"), _money(F.col("annual_value_dollars")),
                        F.col("payback_years") * 12, stage, _day(F.col("planned_date"))).alias("detail"),
        F.col("annual_value_dollars").alias("value_dollars_yr"),
        F.lit("The project's annual value: its expected uplift at today's gas price").alias("value_basis"),
        F.lit("capital_plan").alias("module_key"), F.lit("Capital & Decline-Aware Investment").alias("module_label"),
        F.col("project_id").alias("focus_ids"), F.lit("expected uplift").alias("focus_kpis"))

    # 3. contract supply: firm contracts that received less than they nominated over the last 30 gas days
    cs = spark.read.table("gld_customer_supply")
    gap = (cs.where(F.col("contract_type") != "Spot").withColumn("gap_tj", F.col("nominated_tj_d") - F.col("delivered_tj_d"))
           .where("gap_tj > 0.3").withColumn("usd_yr", F.col("gap_tj") * 1000 * F.col("price_gj") * 365))
    gsum = gap.agg(F.count("*").alias("n"), F.sum("gap_tj").alias("gap_all"), F.sum("usd_yr").alias("usd_all"),
                   F.concat_ws("|", F.collect_list("contract_id"), F.collect_list("customer_name")).alias("cids"))
    gtop = gap.withColumn("_r", F.row_number().over(W.orderBy(F.desc("usd_yr")))).where("_r = 1").drop("_r")
    supply = gtop.crossJoin(gsum).select(
        F.lit("contract_supply").alias("lever"),
        F.when(F.col("n") > 1, F.format_string("Close the supply gap on %d contracts, led by %s (%s)", F.col("n"), F.col("customer_name"), F.col("contract_id")))
         .otherwise(F.format_string("Close the supply gap on %s (%s)", F.col("customer_name"), F.col("contract_id"))).alias("action"),
        F.format_string("%s nominated %.1f TJ/d more than it received over the last 30 days (short on %d days). Across %d contracts the field is %.1f TJ/d behind nominations: about %s a year of sales and shortfall exposure.",
                        F.col("customer_name"), F.col("gap_tj"), F.col("shortfall_days_30d"), F.col("n"), F.col("gap_all"),
                        _money(F.col("usd_all"))).alias("detail"),
        F.col("usd_all").alias("value_dollars_yr"),
        F.lit("Nominated gas not delivered over the last 30 gas days, at each contract's price, annualised").alias("value_basis"),
        F.lit("commercial").alias("module_key"), F.lit("Commercial & Prices").alias("module_label"),
        F.col("cids").alias("focus_ids"), F.lit("delivery vs nomination").alias("focus_kpis"))

    # 4. contract price: the firm contract ending inside 18 months that sells furthest below the spot market
    mkt = cs.where(F.col("contract_type") == "Spot").agg(F.avg("price_gj").alias("mkt"))
    rp = (cs.where(F.col("contract_type") != "Spot").crossJoin(mkt)
          .withColumn("days_left", F.datediff(F.to_date("contract_end"), F.to_date("as_of_day")))
          .where((F.col("days_left").between(0, 548)) & (F.col("price_gj") < F.col("mkt") - 0.25))
          .withColumn("usd_yr", (F.col("mkt") - F.col("price_gj")) * F.col("delivered_tj_d") * 1000 * 365)
          .withColumn("_r", F.row_number().over(W.orderBy(F.desc("usd_yr")))).where("_r = 1").drop("_r"))
    price = rp.select(
        F.lit("contract_price").alias("lever"),
        F.format_string("Re-price %s (%s) at renewal", F.col("customer_name"), F.col("contract_id")).alias("action"),
        F.format_string("The contract sells at $%.2f/GJ, $%.2f below today's spot market of $%.2f/GJ, and ends on %s (%d months away). Renewing at market adds about %s a year.",
                        F.col("price_gj"), F.col("mkt") - F.col("price_gj"), F.col("mkt"), _day(F.col("contract_end")),
                        F.round(F.col("days_left") / 30.4).cast("int"), _money(F.col("usd_yr"))).alias("detail"),
        F.col("usd_yr").alias("value_dollars_yr"),
        F.lit("Gap to the spot price x the contract's delivered volume, per year").alias("value_basis"),
        F.lit("commercial").alias("module_key"), F.lit("Commercial & Prices").alias("module_label"),
        F.concat_ws("|", F.col("contract_id"), F.col("customer_name")).alias("focus_ids"), F.lit("sales gas price").alias("focus_kpis"))

    # 5. carbon: this year's Safeguard shortfall, annualised from the days reported so far
    cc = spark.read.table("gld_carbon_compliance")
    yr = cc.agg(F.max("reporting_year").alias("yr"))
    doy = (spark.read.table("gld_emissions_ledger").withColumn("y", F.year("gas_day")).join(yr, F.col("y") == F.col("yr"))
           .agg(F.dayofyear(F.max("gas_day")).alias("doy")))
    over = cc.join(yr, cc["reporting_year"] == yr["yr"]).where("position_t > 0").drop("yr").join(sites, cc["facility"] == sites["area_code"], "left")
    osum = over.agg(F.count("*").alias("n"), F.sum("position_t").alias("t"), F.sum("accu_cost_dollars").alias("usd"),
                    F.concat_ws("|", F.collect_list("facility")).alias("fids"), F.max("reporting_year").alias("ry"))
    otop = over.withColumn("_r", F.row_number().over(W.orderBy(F.desc("accu_cost_dollars")))).where("_r = 1").drop("_r")
    carbon = otop.crossJoin(osum).crossJoin(doy).select(
        F.lit("carbon").alias("lever"),
        F.format_string("Cover the Safeguard shortfall at %d facilities, led by %s", F.col("n"), F.coalesce("site", "facility")).alias("action"),
        F.format_string("%d facilities are %s t CO₂-e over their Safeguard baselines so far in %d. Covering that with ACCUs costs about %s a year at today's carbon price; %s is the largest at %s so far.",
                        F.col("n"), F.format_number(F.col("t"), 0), F.col("ry"), _money(F.col("usd") * 365 / F.col("doy")),
                        F.coalesce("site", "facility"), _money(F.col("accu_cost_dollars"))).alias("detail"),
        (F.col("usd") * 365 / F.col("doy")).alias("value_dollars_yr"),
        F.lit("ACCU cost of this year's shortfall so far, annualised").alias("value_basis"),
        F.lit("emissions").alias("module_key"), F.lit("Emissions, Carbon & Safeguard").alias("module_label"),
        F.col("fids").alias("focus_ids"), F.lit("accu cost|position vs baseline").alias("focus_kpis"))

    # 6. methane: components flagged by LDAR, valued as the gas kept for sale once repaired
    ld = spark.read.table("gld_methane_ldar").where(F.col("survey_status").isin("investigate", "monitor"))
    lsum = ld.agg(F.count("*").alias("n"), F.sum("product_value_dollars_yr").alias("usd"), F.sum("co2e_t_yr").alias("t"),
                  F.concat_ws("|", F.slice(F.array_sort(F.collect_list(F.struct((-F.col("product_value_dollars_yr")).alias("k"), F.col("asset_id").alias("a")))), 1, 25).getField("a")).alias("aids"))
    methane = lsum.where("n > 0").select(
        F.lit("methane").alias("lever"),
        F.format_string("Repair the methane leaks on %d components", F.col("n")).alias("action"),
        F.format_string("Leak detection has flagged %d components. Repairing them keeps about %s a year of gas for sale and avoids %s t CO₂-e a year.",
                        F.col("n"), _money(F.col("usd")), F.format_number(F.col("t"), 0)).alias("detail"),
        F.col("usd").alias("value_dollars_yr"),
        F.lit("Market value of the escaping gas, per year").alias("value_basis"),
        F.lit("methane_ldar").alias("module_key"), F.lit("Methane LDAR").alias("module_label"),
        F.col("aids").alias("focus_ids"), F.lit("investigate").alias("focus_kpis"))

    allc = defer.unionByName(capital).unionByName(supply).unionByName(price).unionByName(carbon).unionByName(methane)
    return (allc.withColumn("rank", F.row_number().over(W.orderBy(F.desc("value_dollars_yr")))).crossJoin(asof)
            .select(F.col("rank").cast("int").alias("rank"), "lever", "action", "detail", F.round("value_dollars_yr", 0).alias("value_dollars_yr"),
                    "value_basis", "module_key", "module_label", "focus_ids", "focus_kpis", "data_as_of")
            .orderBy("rank"))
