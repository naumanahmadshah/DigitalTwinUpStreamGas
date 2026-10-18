# Gas Upstream Digital Twin

A complete, self-contained digital twin for an upstream gas field, deployable to any Databricks
workspace with a single command. It generates about two years of realistic synthetic OT and IT data,
refines it through a bronze to silver to gold medallion, and serves a rich single-page application, a
curated Genie space, an AI/BI dashboard and Lakebase write-back.

Starting from a raw computer (macOS, Linux or Windows), an internet connection and a Databricks
workspace where you have **workspace-admin** rights, you run one command and get a running app. There
is nothing to pre-create and nothing to configure beyond pointing it at your workspace.

---

## Contents

- [Architecture](#architecture)
- [The medallion in detail](#the-medallion-in-detail)
- [Application features](#application-features)
- [Deploy](#deploy)
- [Requirements and prerequisites](#requirements-and-prerequisites)
- [After deployment: what to open](#after-deployment-what-to-open)
- [Redeploying](#redeploying)
- [Project layout](#project-layout)
- [Troubleshooting](#troubleshooting)
- [Cleanup](#cleanup)

---

## Architecture

Data flows left to right: synthetic sources are generated into Unity Catalog, refined through the
medallion on serverless Lakeflow, and served to the app, Genie and the dashboard. Every emitter is
driven by one seeded field state, so the marts reconcile across production, reliability, emissions and
commercial views.

![Gas Upstream Digital Twin architecture](docs/architecture.png)

The one join key that ties everything together is the **tag id** (for example `CPF002-CMP-AT-1018`),
decoded as `AREA-SYSTEM-CLASS-NNNN`. It resolves through the semantic backbone to its asset, functional
location, live value and its ids in every other system (PI historian, EAM, OSDU, P&ID). The app's
**Asset & Document Explorer** demonstrates this resolution live.

### What gets deployed

| Component | What it is |
|-----------|------------|
| **Catalog + schema** | A Unity Catalog catalog (default `gas_upstream_digital_twin`) and schema (`digital_twin`) holding every table, mart and the landing volume. |
| **Data generator** | A serverless job that builds the semantic backbone and backfills telemetry, events, work orders, lab assays, weather, prices, spares and more. |
| **Medallion pipeline** | A serverless Lakeflow (DLT) pipeline: bronze (raw) to silver (conformed) to gold (curated marts). |
| **The app** | A FastAPI + single-page application: Command Centre, reservoir-to-customer impact analysis (with PDF export), Field Map, Assets map, 3D site views, the Gas flow chain and Analytics tabs, a human-in-the-loop recommendation inbox, an asset explorer, and a link to Genie. |
| **Genie space** | "Asset Intelligence Genie": a curated natural-language analytics space over the gold marts only, opened in Databricks One from the app's lamp icon. |
| **AI/BI dashboard** | A published dashboard, "Gas Upstream Digital Twin", with five pages: Field Command Centre, Wells & Lift, Compression Health, Deferment & Emissions, Custody & UAF. |
| **Lakebase** | A Postgres instance used for recommendation write-back (accept / reject / annotate, audited). |

---

## The medallion in detail

Everything lands in one catalog and schema, parameterised as bundle variables. All tables and columns
carry COMMENTs.

- **Generation (the `setup` job).** `build_backbone` writes the semantic backbone (tag registry, asset
  master, functional-location hierarchy, well master, PVT, id crossmap: 923 assets, 5,569 tags, 500
  wells). `run_generator` backfills `land_telemetry` and `land_events` (about 4.5M telemetry rows and
  363K events over two years). `batch_sources` writes the `src_*` snapshot tables and the files on the
  landing volume (lab, proving, prices, weather, OSDU well/PVT, geometry, imagery, documents), plus the
  supply network (reservoir to customer) and a fictional customer book of 28 buyers with two years of
  daily custody deliveries that reconcile to metered production.

- **Landing.** Streaming sources land in the Delta tables `land_telemetry` and `land_events`; the file
  sources are written to the Unity Catalog volume `/Volumes/<catalog>/<schema>/landing` and ingested by
  Auto Loader in bronze. The deploy creates that volume up front, since the generator writes files into
  it but cannot create the volume object itself. The semantic backbone is the join spine: every tag id
  (`AREA-SYSTEM-CLASS-NNNN`) resolves through it to an asset, a functional location and the matching ids
  in PI historian, EAM, OSDU and P&ID, so all downstream marts reconcile on one key.

- **Bronze (raw fidelity, about 31 tables).** Streams `land_telemetry` split by source system (process
  telemetry, vibration features, CEMS emissions, custody metering), streams safety/alarm events,
  materialises historian and PI Asset-Framework stand-ins, snapshots the `src_*` CDC tables, and uses
  Auto Loader for the volume files. Raw rows are preserved with ingest metadata.

- **Silver (conformed, typed, contextualized, about 33 tables).** Cleanses and types the telemetry,
  joins it to the tag registry, applies SCD where needed on reference and master data, and conforms the
  operational records (work orders, notifications, failures, allocation, deferment, nominations,
  permits, spares, customers and deliveries, the supply network) and context (lab, weather, prices,
  proving, documents, geometry).

- **Gold (35 curated marts, the serving layer).** Business marts and ML proxies the app, Genie and the
  dashboard consume: field production, well scorecard, compression health, plant KPI, asset state, tag
  latest, constraint ranking, flow assurance, custody allocation, integrity/RBI, safety alarms,
  emissions ledger, deferment, economics, commercial, weather, lab assay, work orders, notifications,
  failure history, maintenance reliability, produced water, energy efficiency, carbon compliance,
  methane LDAR, margin, spares risk, capital plan, remaining useful life, liquid-loading risk,
  production forecast, recommendations, and the impact-analysis marts (supply network with flows, supply
  nodes, customer supply). Gold is the only layer Genie is allowed to query.

---

## Application features

The app ("Databricks Asset Intelligence") opens on the Command Centre. The left menu has six items:
Command Centre, Assets map, Gas flow chain, Analytics, Recommendation Inbox and Asset & Document
Explorer. A short tour follows; the full screen-by-screen guide is in
[`docs/NAVIGATION.md`](docs/NAVIGATION.md).

**Command Centre and what opens from it**
- **Command Centre** - the field rate, the difference from commitment, the current bottleneck, the
  value chain as five pictures (the stop under pressure is red), production against commitment, and
  tiles for deferment, carbon intensity, red assets, ESD trips and open work orders. Everything is
  clickable.
- **Impact analysis** (the flow icon on the top tile) - the whole gas path as a Sankey: reservoirs,
  well pads, gathering hubs, processing trains, compressors, custody meters and the customers the gas
  is custody-transferred to. Click any stop to highlight everything upstream that feeds it and
  everything downstream it supplies; a speech bubble lists the affected customers with the gas each
  would lose (TJ/d and % of its supply). **Download Impact Details (PDF)** exports the full upstream,
  downstream and customer tables.
- **Field Map** (the map icon on the top tile) - every site across the Australian gas basins, grouped
  into "N sites" bubbles; hover a site for its details, click it to open its **3D site view**.
- **3D site view** - the site's equipment in 3D; click an asset or its tag to slide in its full detail.

**Menu**
- **Assets map** - every asset (wellhead, gathering line, compressor, separator, meter, plant unit) on
  the map, one typed marker each; click one for its detail.
- **Gas flow chain** - the five value-chain stops as tabs: Reservoirs & Wells, Gathering & Flow
  Assurance (hydrate margin), Processing / CPF (dewpoint and spec margins), Compression Health (the
  vibration-to-remaining-life story), Custody Transfer & Export (metered vs allocated, UAF).
- **Analytics** - every cross-cutting report as a tab: Integrity & RBI, Safety & Alarms, Emissions &
  Carbon, Deferment & Losses, Cost & Economics, Planning & Maintenance, Lab & Assay, Commercial &
  Prices, Weather & Environment (a per-site weather map), Produced Water Management, Fuel-Gas & Energy
  Efficiency, Carbon / Safeguard Compliance, Methane LDAR, Molecule-to-Market Margin, Spares &
  Procurement Risk (what to procure, lead time vs remaining life), Capital & Decline-Aware Investment.
- **Recommendation Inbox** - prescriptive recommendations with their evidence and linked open work
  orders: accept / reject / annotate, written back and audited to Lakebase, with Undo.
- **Asset & Document Explorer** - resolve any tag end to end on the left (decoded key, asset link,
  functional location, latest reading with its normal range, and its ids in every other system) and
  browse the physical-location tree on the right (field to area to asset to tag).

**Across every screen**
- Sortable tables with an **i** on every column; table rows open a detail panel (a work-order row opens
  the work order, any other row opens that asset's detail with telemetry charts, work orders,
  notifications, failures, procurement, lab, deferment and live tags, plus **View site**).
- Every work-order id anywhere is a link to the work order.
- The Genie lamp (top right) opens **Asset Intelligence Genie** in Databricks One.
- A connection pool and concurrent queries keep screens fast against the SQL warehouse; Lakebase
  provides the audited write-back.

---

## Deploy

Clone or copy this folder onto your computer, open a terminal in it, and run the command for your OS.

**macOS / Linux**

```bash
chmod +x scripts/*.sh
scripts/deploy.sh --host https://your-workspace.cloud.databricks.com
```

**Windows (PowerShell)**

```powershell
.\scripts\deploy.ps1 -WorkspaceUrl https://your-workspace.cloud.databricks.com
```

That is the whole thing. A browser window opens for you to sign in (OAuth); the script then runs end to
end and prints the app URL and catalog at the finish. Passing `--host` always signs you in to exactly
that workspace, so you cannot accidentally deploy to a stale cached one. If you omit the URL, the script
asks for it, so a bare `scripts/deploy.sh` works too (with `--profile NAME` instead, it reuses that
profile's existing sign-in).

### Options

| macOS/Linux flag | Windows flag | Default | Meaning |
|------------------|--------------|---------|---------|
| `--host URL` | `-WorkspaceUrl URL` | prompted | Workspace URL. If omitted, the script asks for it. |
| `--catalog NAME` | `-Catalog NAME` | `gas_upstream_digital_twin` | Target catalog. Created if it does not exist. |
| `--schema NAME` | `-Schema NAME` | `digital_twin` | Target schema. |
| `--warehouse ID` | `-Warehouse ID` | auto | Use a specific SQL warehouse instead of auto-detect/create. |
| `--db-instance NAME` | `-DbInstance NAME` | `gas-upstream-twin-db` | Lakebase instance name. |
| `--app-name NAME` | `-AppName NAME` | `gas-upstream-twin` | Databricks App name (unique in the workspace). |
| `--profile NAME` | `-Profile NAME` | derived from URL | CLI auth profile. By default a per-workspace name is derived from the URL (e.g. `gastwin-dbc-xxxx`), so each workspace gets its own and logins never conflict. |
| `--skip-data` | `-SkipData` | off | Deploy only; do not (re)generate data. Use for code-only redeploys. |

### What happens, and how long it takes

The script runs in phases and tells you where it is. End to end is roughly **20 to 35 minutes**, most of
it data generation and the first medallion run.

1. Prerequisites and CLI, then sign-in.
2. Find or create a serverless SQL warehouse.
3. Create the catalog, schema and `landing` volume (via SQL, so it works on Default Storage workspaces).
4. Create the Lakebase instance (a few minutes to provision).
5. Deploy the bundle (pipeline, jobs, app) and grant the app read access.
6. **Generate the data** (the longest step).
7. Run the medallion pipeline (bronze to silver to gold).
8. Build the Genie space and the dashboard.
9. Start the app and wire it to Genie and Lakebase.

---

## Requirements and prerequisites

**On your computer** (the script installs what it can):

- An internet connection.
- The **Databricks CLI**. If it is missing, the script installs it (official installer on macOS/Linux,
  `winget` on Windows). See the [Databricks CLI install guide](https://docs.databricks.com/dev-tools/cli/install.html) to do it yourself.
- **Python 3** on macOS/Linux only (used to read JSON responses). It ships with most systems; if not,
  install the Xcode Command Line Tools (`xcode-select --install`) or your distro's `python3` package.
  Windows needs nothing beyond the CLI.

**In the workspace** (workspace-admin satisfies these):

- Unity Catalog enabled, with permission to create a catalog (or an existing catalog you can pass in).
- **Serverless compute** available (jobs, Lakeflow pipelines and SQL warehouse).
- **Databricks Apps** enabled.
- **Lakebase** (Database Instances) available in the workspace region.
- **Genie** available.

The one hard requirement is admin rights to create a catalog. Everything else the script creates or
auto-detects; there are no config files to edit.

---

## After deployment: what to open

When the script finishes it prints a summary like this:

```
  App:        https://gas-upstream-twin-xxxxxxxx.<region>.databricksapps.com
  Catalog:    gas_upstream_digital_twin.digital_twin
  Genie:      https://your-workspace.cloud.databricks.com/one/agents/<id>
  Dashboard:  open Dashboards > "Gas Upstream Digital Twin"
  Warehouse:  <id>
```

- **App**: open the printed URL and start at the **Command Centre**.
- **Catalog**: open **Catalog** and browse `gas_upstream_digital_twin.digital_twin`. The `gld_*` tables
  are the curated marts; `slv_*` are conformed; `bronze_*` are raw.
- **Genie**: open the printed Genie link (it opens **Asset Intelligence Genie** in Databricks One), or
  click the lamp at the top right of the app, and ask questions over the marts.
- **Dashboard**: open **Dashboards** and select "Gas Upstream Digital Twin".

Start with [`docs/NAVIGATION.md`](docs/NAVIGATION.md) for a guided tour of the app.

---

## Redeploying

The deploy is designed to run once per workspace against an empty catalog. To redeploy from scratch
(fresh data and resources), run a full cleanup to return the workspace to a clean state, then deploy
again:

```bash
scripts/cleanup.sh --host https://your-workspace.cloud.databricks.com --yes   # macOS/Linux
scripts/deploy.sh  --host https://your-workspace.cloud.databricks.com
```

```powershell
.\scripts\cleanup.ps1 -WorkspaceUrl https://your-workspace.cloud.databricks.com -Yes   # Windows
.\scripts\deploy.ps1  -WorkspaceUrl https://your-workspace.cloud.databricks.com
```

Pass the same workspace URL to both; the cleanup and deploy each derive the matching profile from it.
See [Cleanup](#cleanup) for exactly what the cleanup removes.

---

## Project layout

```
gas-upstream-digital-twin/
  databricks.yml        # bundle: name, variables, includes, target
  resources/            # bundle resources, split by concern
    pipeline.yml        #   the medallion DLT pipeline
    jobs.yml            #   setup (data gen), surfaces (Genie+dashboard), lakebase seed
    app.yml             #   the Databricks App
  app/                  # FastAPI backend (main.py) + single-page UI (static/)
  generator/            # synthetic OT/IT data generation
  pipelines/            # bronze / silver / gold transforms
  serving/              # Genie space, dashboard and Lakebase builders
  config/               # twin_config.yaml (field size, scenarios, seed)
  scripts/              # deploy, cleanup, generate_data (.sh + .ps1), shared helpers in lib/
  docs/                 # architecture.html, architecture.png, NAVIGATION.md, TROUBLESHOOTING.md
```

---

## Troubleshooting

Common issues and fixes are in [`docs/TROUBLESHOOTING.md`](docs/TROUBLESHOOTING.md). The deploy scripts
are idempotent: if a step fails, fix the cause and re-run the same command. Highlights:

- **Catalog creation**: the script creates the catalog via SQL, which works on both standard metastores
  and Default Storage workspaces. If it still cannot, create the catalog once in the UI or pass
  `--catalog <existing>`.
- **Lakebase not ready**: Lakebase must be available in your workspace region; provisioning takes a few
  minutes.
- **No Genie lamp in the header**: the Genie space was not found at deploy time; re-run with `--skip-data`
  to rebuild surfaces and restart the app.

---

## Cleanup

`scripts/cleanup.sh` removes everything this project created and returns the workspace to a clean,
pre-deploy state so `deploy.sh` can run fresh. It removes the app, the AI/BI dashboard, the Genie space,
the Lakebase instance, the medallion pipeline and jobs (and the bundle's workspace folder), and the
catalog itself with all generated data. A pre-existing or shared SQL warehouse is left untouched; only a
project-created one named "Gas Twin Warehouse" is removed.

```bash
scripts/cleanup.sh --host https://your-workspace.cloud.databricks.com         # macOS/Linux; add --yes to skip the prompt
```

```powershell
.\scripts\cleanup.ps1 -WorkspaceUrl https://your-workspace.cloud.databricks.com   # Windows; add -Yes to skip the prompt
```

Pass the same workspace URL you deployed with. It prompts for confirmation before deleting (it is
destructive). The catalog, schema, Lakebase and app names default to the deploy values; override with
`--catalog`, `--schema`, `--db-instance`, `--app-name` if you changed them at deploy time.
