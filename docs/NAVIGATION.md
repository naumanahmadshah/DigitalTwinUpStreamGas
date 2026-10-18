# Navigating the app

A guided tour of the Gas Upstream Digital Twin app ("Databricks Asset Intelligence"). The left menu has
six items: **Command Centre, Assets map, Gas flow chain, Analytics, Recommendation Inbox, Asset &
Document Explorer**. The highlighted item always has the content area pointing back at it like a
speech bubble.

## Three ways to read it

- **Executive view**: start at the Command Centre (production against commitment, the bottleneck,
  the stage that is under pressure), then open the Field Map.
- **Engineer view**: walk the Gas flow chain stop by stop, then the Analytics tabs, opening any asset's
  detail as you go.
- **Commercial / impact view**: open Impact analysis from the Command Centre and click a stop to see
  which customers depend on it and how much gas and revenue is at risk.

## Everywhere

- **Tiles** (the KPI cards) carry an **i** with their definition; some open a related screen or a full
  list when clicked.
- **Tables**: click a column header to sort; every header has an **i** explaining the column. Clicking a
  row opens the matching **detail panel**: a work-order row opens the work order, any other row opens
  that asset's detail.
- **Asset detail** (from any table, map or 3D site): type, area, criticality, health score,
  production at risk and open work orders; the recommendation for the asset; telemetry charts (click
  one to enlarge); work orders, notifications, failure history, what to order, lab assays, deferment
  and live tags. **View site** (top of the panel) opens the asset's site in 3D; the area link opens
  Planning & Maintenance.
- **Work orders**: every work-order id anywhere (tables, asset detail, recommendation evidence) is a
  link to the work order. Opened from an asset, the work order has a back arrow to that asset.
- **Charts**: click a telemetry chart to enlarge it (the enlarged view scrolls on its own; close with
  the x, a click outside, or Esc). Bar charts that compare each bar with the average use three teal
  shades, explained by a key above the chart.
- **Header** (top right): the Genie lamp opens **Asset Intelligence Genie** in Databricks One (the
  business-user interface) in a new tab.
- **Loading**: while a screen loads, the app dims behind a translucent blur with a blue flame.

## Screen by screen

1. **Command Centre** - the executive home.
   - Top tile: the **field rate**, the **difference from commitment** (green above, red below), the
     **bottleneck** (the single asset putting the most production at risk), then two icons: the
     **Field Map** and **Impact analysis**.
   - The value chain as five pictures (Reservoir, Wells, Gathering, Processing, Custody). The stop
     under the most pressure is red with its production at risk; the next is amber. A picture grows
     when you hover it; click it to open that stop in the Gas flow chain.
   - **Production against commitment**: actual vs nominated over the last two months, with any
     shortfall shaded.
   - Bottom tiles: deferment ($ lost production), carbon intensity, red assets, ESD trips and open work
     orders; each opens the matching screen.

2. **Impact analysis** (the flow icon on the Command Centre's top tile) - the complete gas path as a
   Sankey, one column per stop: Reservoir, Well pads, Gathering, Processing, Compression, Custody,
   Customers. Band width is the average daily flow over the latest 30 gas days; stations in alarm have
   a red outline; hover any stop or band for its name and flow.
   - Click a stop: everything upstream that feeds it and downstream that it supplies stays lit while
     the rest fades. A speech bubble on the right lists the customers it supplies, each with the gas it
     would lose (TJ/d and share of its supply), plus the totals and revenue at risk per day. The
     bubble scrolls when the list is long; click a customer to trace that customer's own supply path.
   - **Download Impact Details (PDF)** saves a landscape report: summary, then tables of the upstream
     stops, downstream stops and customers (segment, location, meter, contract, priority, maximum daily
     quantity, delivered gas, gas and revenue at risk, contract end).
   - Click empty space or **Clear** to reset.

3. **Field Map** (the map icon on the Command Centre's top tile) - every site (well pad, gathering
   network, processing train, custody meter) across the Australian gas basins. Group bubbles count
   **sites**; a single site's bubble takes the colour of its worst asset (red, amber or green). Hover a
   site for its name, its assets (red, amber, healthy) and production at risk; click it to open the
   **3D site view**.

4. **3D site view** (opened from the Field Map, from **View site** in an asset's detail, or from the
   Command Centre's red assets tile) - the site's equipment in 3D with a floating tag over each asset.
   - The left list shows every value-chain stop (site) and its asset count; click one to switch sites.
   - Drag to rotate, scroll to zoom. Hover an asset for a quick tooltip.
   - Click an asset or its tag: the full asset detail slides in from the right. Click empty ground (or
     the x) to slide it away.

5. **Assets map** - every asset (wellhead, gathering line, compressor, separator, meter and plant
   unit) on the Australia map, one typed marker each (W well, G gathering, P processing, C compression,
   M custody). Group bubbles count **assets**; pumps and gas lift sit on their wellhead, so each well is
   one marker. Click an asset to open its detail.

6. **Gas flow chain** - the five value-chain stops as folder tabs:
   - *Reservoirs & Wells* - producing / CSG wells, utilisation, liquid-loading watch and SSSV status;
     the reservoir pressure trend; gas produced by each well (shaded against the average); wells by
     utilisation; and the liquid-loading watchlist ranked by risk.
   - *Gathering & Flow Assurance* - segments on hydrate watch, the minimum hydrate margin, pig runs
     due and line pressure; the worst-case hydrate margin by segment; and every segment's line
     conditions against its hydrate point.
   - *Processing Facility / CPF* - streams off or near dewpoint spec and NGL recovery; the water
     dewpoint and H2S trends; and each stream against its spec.
   - *Compression Health* - fleet health, the shortest remaining life and production at risk; health by
     unit; and the compressor fleet table (the vibration-to-remaining-life story is on CPF002).
   - *Custody Transfer & Export* - the worst unaccounted-for gas (UAF), metering stations, the UAF
     trend and the custody reconciliation.

7. **Analytics** - every cross-cutting report as a folder tab (each opens with the data period it
   covers):
   - *Integrity & RBI* - equipment due for inspection within 30 days, high-risk equipment, remaining
     life by area, and the inspection-due list.
   - *Safety & Alarms* - ESD trips, alarms and critical events; ESD trips by area; recent safety events
     (click one for its asset).
   - *Emissions & Carbon* - carbon intensity, total CH4 and CO2; the intensity trend; CH4 by source;
     the emissions ledger.
   - *Deferment & Losses* - total deferment in dollars and gas, by coded cause and by day; deferment by
     cause and asset.
   - *Cost & Economics* - revenue, deferment and carbon cost, revenue by day, and economics by asset.
   - *Planning & Maintenance* - open and P1 work orders, assets with failures, work-order cost, open
     notifications and recorded failures (these two open full lists); work orders by area, by state
     and failures by mode; the work-order table and the maintenance backlog.
   - *Lab & Assay* - samples across the value chain (wellhead, inlet, treated and sales gas), C1 and
     H2S; CO2 by sample point; recent assays.
   - *Commercial & Prices* - last month's sale revenue, cost of generation, profit, carbon and water
     disposal cost, the sales gas price and delivery vs nomination; monthly revenue, cost and profit;
     the price trend; the daily commercial ledger.
   - *Weather & Environment* - the hottest and windiest site (named; click to open it), sites at flood
     or lightning risk, and a map of every site coloured by average temperature. Click a site for its
     temperature, wind and risk history.
   - *Produced Water Management* - produced water, reinjection and disposal cost, the trend and
     produced water by well.
   - *Fuel-Gas & Energy Efficiency* - own-use and flaring shares, fuel gas, their trends and the
     daily ledger.
   - *Carbon / Safeguard Compliance* - Scope 1 this year, position against the Safeguard baseline and
     ACCU cost; Scope 1 by facility; the position by facility and year.
   - *Methane LDAR* - CH4 and CO2-e, components to investigate, CH4 by area, and methane by component
     with the annual value of the escaping gas.
   - *Molecule-to-Market Margin* - netback (dollars and per GJ), take-or-pay shortfall days, their
     trends and the daily margin and commitment ledger.
   - *Spares & Procurement Risk* - assets at risk and on watch, the shortest remaining life, remaining
     life by asset, and exactly what to procure (part / repair / service) where the lead time beats the
     remaining life.
   - *Capital & Decline-Aware Investment* - projects, capex and expected uplift; capex by area; the
     development and capital schedule with payback.

8. **Recommendation Inbox** - prescriptive recommendations with their cross-domain evidence and the
   asset's open work orders (linked). **Accept / Reject / Annotate** is written back and audited to
   Lakebase; once decided, the card shows an **Undo** instead.

9. **Asset & Document Explorer** - the semantic backbone, made tangible. Tiles count the tags, assets
   and functional locations. On the **left**, type a tag, asset, area or signal and it resolves end to
   end: the decoded tag key, the asset (a link to its detail), the functional location, the latest
   sensor reading with its normal operating range, and its ids in every other system (PI historian,
   EAM, OSDU, P&ID). On the **right**, browse the physical-location tree (field to area to asset to
   tag); click an asset for its detail, click a tag to resolve it on the left.

## Also deployed

- **Asset Intelligence Genie** (in Databricks One): ask questions in plain language over the gold
  marts, including the customer and supply-network marts.
- **AI/BI dashboard "Gas Upstream Digital Twin"** (Dashboards): five pages, Field Command Centre,
  Wells & Lift, Compression Health, Deferment & Emissions, and Custody & UAF.
