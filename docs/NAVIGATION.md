# Navigating the app

A guided tour of the Gas Upstream Digital Twin app ("Databricks Asset Atlas"). The left menu has
five icons: **Command Centre, Gas flow chain, Analytics, Recommendation Inbox, Asset & Document
Explorer** (hover an icon to see its name). The highlighted item always has the content area pointing
back at it like a speech bubble.

## Three ways to read it

- **Executive view:**\
  Start at the Command Centre (production against commitment, the bottleneck,
  the stage that is under pressure), then open the Field Map.
- **Engineer view:**\
  Walk the Gas flow chain stop by stop, then the Analytics tabs, opening any asset's
  detail as you go.
- **Commercial / impact view:**\
  Open Impact analysis from the Command Centre and click a stop to see
  which customers depend on it and how much gas and revenue is at risk.

## Everywhere

- **Tiles** (the KPI cards) carry an **i** with their definition; some open a related screen or a full
  list when clicked.
- **Tables:**\
  Click a column header to sort; every header has an **i** explaining the column. Clicking a
  row opens the matching **detail panel**: a work-order row opens the work order, any other row opens
  that asset's detail.
- **Asset detail** (from any table, map or site view):\
  Type, area, criticality, health score,
  production at risk and open work orders; the recommendation for the asset; telemetry charts (click
  one to enlarge); work orders, notifications, failure history, what to order, lab assays, deferment
  and live tags. **View site** (top of the panel) opens the asset's site view; the area link opens
  Planning & Maintenance.
- **Work orders:**\
  Every work-order id anywhere (tables, asset detail, recommendation evidence) is a
  link to the work order. Opened from an asset, the work order has a back arrow to that asset.
- **Charts:**\
  Click a telemetry chart to enlarge it (the enlarged view scrolls on its own; close with
  the x, a click outside, or Esc). Bar charts that compare each bar with the average use three teal
  shades, explained by a key above the chart.
- **Header** (top right):\
  The Genie lamp opens **Asset Intelligence Genie** in Databricks One (the
  business-user interface) in a new tab.
- **Loading:**\
  While a screen loads, the app dims behind a translucent blur with a blue flame.

## Screen by screen

1. **Command Centre:**\
   The executive home.
   - Top tile: the **field rate**, the **difference from commitment** (green above, red below), the
     **bottleneck** (the single asset putting the most production at risk), then two icons: the
     **Field Map** and **Impact analysis**.
   - The value chain as five pictures (Reservoir, Wells, Gathering, Processing, Custody). The stop
     under the most pressure is red with its production at risk; the one to watch (the next most at
     risk) is amber, also with its production at risk. Hover a picture for what its colour means.
   - **Click anything red or amber** (a stop, the bottleneck, or the difference from commitment when
     it is below): the screen where the cause lives opens with a banner saying why, and the cause
     coloured to match: its table rows (moved to the top), chart bars, chart lines and KPI tiles.
     For example, amber Wells opens Reservoirs & Wells with the liquid-loading wells highlighted;
     red Processing opens Compression Health with the failing compressors highlighted. The banner's
     x clears the highlight.
   - **Production against commitment:**\
     Actual vs nominated over the last two months, with any
     shortfall shaded.
   - Bottom tiles: deferment ($ lost production), carbon intensity, red assets, ESD trips and open work
     orders; each opens the matching screen.

2. **Impact analysis** (the flow icon on the Command Centre's top tile):\
   The complete gas path as a
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

3. **Field Map** (the map icon on the Command Centre's top tile):\
   Every site (well pad, offshore platform, gathering hub, offshore manifold, gas plant, custody meter)
   across the operator's ten basins, onshore and offshore, all over Australia. Group bubbles count
   **sites**; a single site's bubble takes the colour of its worst asset (red, amber or green). Hover a
   site for its name, its assets (red, amber, healthy) and production at risk; click it to open the
   **Site view**.

4. **Site view** (opened from the Field Map, from **View site** in an asset's detail, or from the
   Command Centre's red assets tile):\
   The site's equipment with a floating tag over each asset, in a hand-painted graphic-novel style.
   - Onshore sites stand on a fenced gravel pad in the outback; the site ute is painted in the site's
     status colour. Offshore platforms and manifolds stand on a steel deck over a soft blue sea, with a
     crew boat (in the site's status colour), a supply vessel, buoys, gulls, a passing whale and other
     platforms on the horizon.
   - The left list groups the sites (well pads, offshore platforms, gathering hubs, offshore
     manifolds, gas plants, custody) with each site's place underneath; click a heading to fold it,
     click a site to switch to it.
   - Drag to rotate, scroll to zoom. Click an asset and the site engineer runs to it and says what he
     makes of it.
   - Click an asset or its tag: the full asset detail slides in from the right. Click empty ground (or
     the x) to slide it away.

5. **Gas flow chain:**\
   The five value-chain stops as folder tabs:
   - *Reservoirs & Wells:*\
     Producing / CSG wells, utilisation, liquid-loading watch and SSSV status;
     wellhead pressure by basin as % of its starting level (the drift is depletion, the spikes are
     shut-ins building up during plant turnarounds); the average rate per well by basin; wells by
     utilisation; and the liquid-loading watchlist ranked by risk, its watch wells tinted amber.
   - *Gathering & Flow Assurance:*\
     Segments on hydrate watch, the minimum hydrate margin, pig runs
     due and line pressure; the worst-case hydrate margin by segment; and every segment's line
     conditions against its hydrate point.
   - *Processing Facility / CPF:*\
     Streams off or near dewpoint spec and NGL recovery; the water
     dewpoint and H2S trends; and each stream against its spec.
   - *Compression Health:*\
     Fleet health, the shortest remaining life and production at risk; health by
     unit; and the compressor fleet table (the vibration-to-remaining-life story is on CPF002).
   - *Custody Transfer & Export:*\
     The worst unaccounted-for gas (UAF), metering stations, the UAF
     trend and the custody reconciliation.

6. **Analytics:**\
   Every cross-cutting report as a folder tab (each opens with the data period it
   covers):
   - *Integrity & RBI:*\
     Equipment due for inspection within 30 days, high-risk equipment, remaining
     life by area, and the inspection-due list.
   - *Safety & Alarms:*\
     ESD trips, alarms and critical events; ESD trips by area; recent safety events
     (click one for its asset).
   - *Emissions & Carbon:*\
     Carbon intensity, total CH4 and CO2; the intensity trend; CH4 by source;
     the emissions ledger.
   - *Deferment & Losses:*\
     Total deferment in dollars and gas, by coded cause and by day; deferment by
     cause and asset.
   - *Cost & Economics:*\
     Revenue, deferment and carbon cost, revenue by day, and economics by asset.
   - *Planning & Maintenance:*\
     Open and P1 work orders, assets with failures, work-order cost, open
     notifications and recorded failures (these two open full lists); work orders by area, by state
     and failures by mode; the work-order table and the maintenance backlog.
   - *Lab & Assay:*\
     Samples across the value chain (wellhead, inlet, treated and sales gas), C1 and
     H2S; CO2 by sample point; recent assays.
   - *Commercial & Prices:*\
     Last month's sale revenue, cost of generation, profit, carbon and water
     disposal cost, the sales gas price and delivery vs nomination; monthly revenue, cost and profit;
     the price trend; the daily commercial ledger.
   - *Weather & Environment:*\
     The hottest and windiest site (named; click to open it), sites at flood
     or lightning risk, and a map of every site coloured by average temperature. Click a site for its
     temperature, wind and risk history.
   - *Produced Water Management:*\
     Produced water, reinjection and disposal cost, the trend and
     produced water by well.
   - *Fuel-Gas & Energy Efficiency:*\
     Own-use and flaring shares, fuel gas, their trends and the
     daily ledger.
   - *Carbon / Safeguard Compliance:*\
     Scope 1 this year, position against the Safeguard baseline and
     ACCU cost; Scope 1 by facility; the position by facility and year.
   - *Methane LDAR:*\
     CH4 and CO2-e, components to investigate, CH4 by area, and methane by component
     with the annual value of the escaping gas.
   - *Molecule-to-Market Margin:*\
     Netback (dollars and per GJ), take-or-pay shortfall days, their
     trends and the daily margin and commitment ledger.
   - *Spares & Procurement Risk:*\
     Assets at risk and on watch, the shortest remaining life, remaining
     life by asset, and exactly what to procure (part / repair / service) where the lead time beats the
     remaining life.
   - *Capital & Decline-Aware Investment:*\
     Projects, capex and expected uplift; capex by area; the
     development and capital schedule with payback.

7. **Recommendation Inbox:**\
   Prescriptive recommendations with their cross-domain evidence and the
   asset's open work orders (linked). **Accept / Reject / Annotate** is written back and audited to
   Lakebase; once decided, the card shows an **Undo** instead.

8. **Asset & Document Explorer:**\
   The semantic backbone, made tangible. Tiles count the tags, assets
   and functional locations. On the **left**, type a tag, asset, area or signal and it resolves end to
   end: the decoded tag key, the asset (a link to its detail), the functional location, the latest
   sensor reading with its normal operating range, and its ids in every other system (PI historian,
   EAM, OSDU, P&ID). On the **right**, browse the physical-location tree (field to basin to site to
   asset to tag): each basin opens to its sites along the gas path; click an asset for its detail,
   click a tag to resolve it on the left.

## Also deployed

- **Lakebase** (Compute > Lakebase, project gas-upstream-twin-lakebase):\
  The app's only database. Every
  gold mart the app shows is a Lakebase synced table, refreshed right after the medallion and then every
  5 minutes by the "Lakebase sync" job, so the app's numbers can trail a fresh medallion run by up to
  5 minutes. The compute scales to zero when idle, so the first screen after a quiet spell takes a moment.

- **Asset Intelligence Genie** (in Databricks One):\
  Ask questions in plain language over the gold
  marts, including the customer and supply-network marts.
- **AI/BI dashboard "Gas Upstream Digital Twin"** (Dashboards):\
  Five pages, Field Command Centre,
  Wells & Lift, Compression Health, Deferment & Emissions, and Custody & UAF.
