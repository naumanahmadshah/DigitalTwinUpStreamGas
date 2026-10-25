"""Phase 1 driver: write the semantic backbone to Delta with table + column COMMENTs.

Runs as a serverless spark_python_task. `__file__` is undefined under exec(), so we derive the
generator dir from the --config path and import asset_model. Config is rendered to JSON by
deploy.sh, so the cluster needs only stdlib.
"""
from __future__ import annotations
import argparse, os, sys
from pyspark.sql import SparkSession, types as T

# (table_comment, [(col, spark_type, comment), ...]): the standing DDL convention: everything commented.
SCHEMAS = {
    "functional_location": (
        "ISO 14224 functional-location hierarchy (Site>Area>System>Asset). SCD2-ready reference spine of the twin (Design 6.1).",
        [
            ("fl_id", "string", "Functional-location node id. Primary key."),
            ("parent_fl_id", "string", "Parent node id; null at the Site root."),
            ("level", "string", "Hierarchy level: Site, Area, System, or Asset."),
            ("code", "string", "Short code for the node (area, system, or class code)."),
            ("name", "string", "Human-readable node name."),
            ("area_code", "string", "Area this node belongs to."),
            ("system_code", "string", "System code within the area (WB, CMP, DEH, AMN, MET, ...)."),
            ("class_code", "string", "ISA-5.1 class code where applicable."),
            ("criticality", "string", "Criticality tier A/B/C where applicable."),
            ("effective_from", "string", "SCD2 effective-from date (YYYY-MM-DD)."),
            ("is_current", "boolean", "SCD2 current-version flag."),
        ],
    ),
    "asset_master": (
        "Physical equipment register keyed to the functional-location model, with class, criticality, and 3D scene binding.",
        [
            ("asset_id", "string", "Asset id. Primary key; referenced by tag_registry.asset_id."),
            ("fl_id", "string", "Functional-location node. FK to functional_location."),
            ("area_code", "string", "Area code."),
            ("system_code", "string", "System code."),
            ("class_code", "string", "ISA-5.1 class code of the asset."),
            ("equipment_type", "string", "Equipment type (compressor, wellhead, separator, metering, ...)."),
            ("criticality", "string", "Criticality tier A/B/C."),
            ("scene_id", "string", "3D scene the asset appears in (Navigator)."),
            ("hotspot_x", "double", "Normalized x (0..1) of the asset hotspot in its scene."),
            ("hotspot_y", "double", "Normalized y (0..1) of the asset hotspot in its scene."),
            ("gps_lat", "double", "Asset latitude for the geospatial map (synthetic)."),
            ("gps_lon", "double", "Asset longitude for the geospatial map (synthetic)."),
            ("manufacturer", "string", "Equipment manufacturer (synthetic)."),
            ("install_year", "int", "Year installed (drives age/integrity)."),
            ("effective_from", "string", "SCD2 effective-from date."),
            ("is_current", "boolean", "SCD2 current-version flag."),
        ],
    ),
    "tag_registry": (
        "THE CONTRACT. One row per signal: tag_id (AREA-SYSTEM-CLASS-NNNN) resolves to its asset, unit, engineering range, cadence, and 3D hotspot. Every telemetry point and every Silver join binds here.",
        [
            ("tag_id", "string", "AREA-SYSTEM-CLASS-NNNN (e.g. BAR008-WB-VL-2555). Primary key and universal join key."),
            ("asset_id", "string", "Asset this signal belongs to. FK to asset_master."),
            ("fl_id", "string", "Functional-location node. FK to functional_location."),
            ("area_code", "string", "Area code parsed from the tag."),
            ("system_code", "string", "System code parsed from the tag."),
            ("class_code", "string", "ISA-5.1 class code parsed from the tag."),
            ("signal", "string", "What the tag measures (discharge_pressure, water_dewpoint, gc_c1, ...)."),
            ("unit", "string", "Engineering unit of the value."),
            ("eng_range_min", "double", "Lower engineering range for validity checks."),
            ("eng_range_max", "double", "Upper engineering range for validity checks."),
            ("description", "string", "Human-readable description used by Genie and the App."),
            ("measured_or_calculated", "string", "measured or calculated/derived."),
            ("cadence", "string", "Expected cadence class: RT, NRT, MB, B, or E."),
            ("scene_id", "string", "3D scene id the hotspot lives in."),
            ("hotspot_x", "double", "Normalized x (0..1) of the hotspot within the scene."),
            ("hotspot_y", "double", "Normalized y (0..1) of the hotspot within the scene."),
            ("gps_lat", "double", "Approximate asset latitude for the map (synthetic)."),
            ("gps_lon", "double", "Approximate asset longitude for the map (synthetic)."),
        ],
    ),
    "well_master": (
        "Well header and completion master (OSDU-sourced in the design). One row per well with lift type and completion.",
        [
            ("well_id", "string", "Well id. Primary key."),
            ("pad_area", "string", "Well pad or offshore wellhead platform (area) the well is drilled from."),
            ("fl_id", "string", "Wellhead functional-location node. FK to functional_location."),
            ("name", "string", "Well name."),
            ("spud_date", "string", "Spud date (YYYY-MM-DD)."),
            ("td_m", "int", "Total depth in metres."),
            ("deviation_type", "string", "vertical, deviated, or horizontal."),
            ("completion_type", "string", "csg (coal seam gas) or conventional (onshore sandstone or offshore)."),
            ("lift_type", "string", "Artificial-lift type: natural, gas_lift, esp, or plunger."),
            ("criticality", "string", "Criticality tier A/B/C."),
            ("status", "string", "producing or shut_in."),
            ("wellhead_asset_id", "string", "The wellhead asset for this well. FK to asset_master."),
            ("potential_e3m3d", "double", "Well test potential (10^3 m^3/d): the rate the well can deliver at full reservoir energy."),
            ("effective_from", "string", "SCD2 effective-from date."),
            ("is_current", "boolean", "SCD2 current-version flag."),
        ],
    ),
    "pvt": (
        "Per-well fluid properties (PVT, CGR/WGR/GOR, composition). Reference inputs for allocation, deferment, emissions.",
        [
            ("well_id", "string", "Well id. FK to well_master."),
            ("cgr_bbl_mmscf", "double", "Condensate-gas ratio (bbl/MMscf)."),
            ("wgr_bbl_mmscf", "double", "Water-gas ratio (bbl/MMscf)."),
            ("gor_scf_bbl", "int", "Gas-oil ratio (scf/bbl)."),
            ("gas_sg", "double", "Gas specific gravity."),
            ("co2_frac_molpct", "double", "CO2 mole fraction (mol %)."),
            ("n2_frac_molpct", "double", "N2 mole fraction (mol %)."),
            ("h2s_ppm", "int", "H2S content (ppm)."),
            ("pvt_source", "string", "Source system (OSDU)."),
            ("effective_from", "string", "SCD2 effective-from date."),
            ("is_current", "boolean", "SCD2 current-version flag."),
        ],
    ),
    "id_crossmap": (
        "Cross-system id map: historian tag <-> EAM functional location <-> OSDU well <-> document entity. Single-source-of-truth governance (Design 6.1). Some links deliberately stale to exercise contextualization.",
        [
            ("entity", "string", "Canonical entity (the tag_id)."),
            ("historian_tag", "string", "The raw historian tag name (PI-style)."),
            ("eam_fl", "string", "EAM functional-location id."),
            ("osdu_ref", "string", "OSDU reference where applicable."),
            ("doc_entity", "string", "Linked engineering-document entity (P&ID id)."),
            ("is_stale", "boolean", "True where the cross-link is stale/missing (contextualization gap)."),
        ],
    ),
}

_SPARK_TYPES = {"string": T.StringType(), "double": T.DoubleType(), "int": T.IntegerType(),
                "boolean": T.BooleanType(), "timestamp": T.TimestampType()}


def _struct(cols):
    return T.StructType([T.StructField(c, _SPARK_TYPES[t], True) for (c, t, _) in cols])


def _esc(s: str) -> str:
    return s.replace("'", "''")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog", required=True)
    ap.add_argument("--schema", required=True)
    ap.add_argument("--config", required=True)
    args = ap.parse_args()

    # Serverless exec(): derive the generator dir from the config path (<root>/config/x.json -> <root>/generator).
    root = os.path.dirname(os.path.dirname(args.config))
    sys.path.insert(0, os.path.join(root, "generator"))
    import asset_model  # noqa: E402
    import records  # noqa: E402

    with open(args.config) as f:
        if args.config.endswith(".json"):
            import json
            cfg = json.load(f)
        else:
            import yaml
            cfg = yaml.safe_load(f)

    spark = SparkSession.builder.getOrCreate()
    tables = asset_model.build_semantic_backbone(cfg)

    for name, rows in tables.items():
        table_comment, cols = SCHEMAS[name]
        colnames = [c for (c, _, _) in cols]
        schema = _struct(cols)
        data = [tuple(row.get(c) for c in colnames) for row in rows]
        df = spark.createDataFrame(data, schema=schema)
        fq = records.fqtn(args.catalog, args.schema, name)
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(fq)
        spark.sql(f"COMMENT ON TABLE {fq} IS '{_esc(table_comment)}'")
        for (c, _, comment) in cols:
            spark.sql(f"ALTER TABLE {fq} ALTER COLUMN `{c}` COMMENT '{_esc(comment)}'")
        print(f"wrote {fq}: {len(rows)} rows, {len(cols)} columns commented")

    print("Phase 1 semantic backbone complete.")


if __name__ == "__main__":
    main()
