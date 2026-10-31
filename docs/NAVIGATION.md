# Navigating the app

A guided tour of the Gas Upstream Digital Twin app ("Databricks Asset Atlas"). The left menu has
six icons: **Command Centre, Gas flow chain, Analytics, Commercial & Finance, Recommendation Inbox,
Asset Explorer** (hover an icon to see its name). The highlighted item always has the content area pointing
back at it like a speech bubble.

## Three ways to read it

- **Executive view:**\
  Start at the Command Centre (production against commitment, the bottleneck,
  the stage that is under pressure), then open the Field Map.
- **Engineer view:**\
  Walk the Gas flow chain stop by stop, then the Analytics tabs, opening any asset's
  detail as you go; the money side is under Commercial & Finance.
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
  Maintenance.
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
     under the most pressure is red, with a hopping comic-style red exclamation mark beside its picture;
     the one to watch (the next most at risk) is amber, with an amber mark. Any of the five can be flagged, following the live
     constraint ranking. Hover a picture for why, with its production at risk.
   - **Click anything red or amber** (a stop, the bottleneck, or the difference from commitment when
     it is below): the screen where the cause lives opens with a banner saying why, and the cause
     coloured to match: its table rows (moved to the top), chart bars, chart lines and KPI tiles.
     For example, amber Wells opens Reservoirs & Wells with the liquid-loading wells highlighted;
     red Processing opens Compression Health with the failing compressors highlighted; a red
     difference from commitment opens Commercial & Prices (under Commercial & Finance) with the
     shortfall days highlighted. The banner's x clears the highlight.
   - **Production against commitment:**\
     Actual vs nominated over the last two months, with any
     shortfall shaded.
   - Bottom tiles: deferment ($ lost production), carbon intensity, red assets, ESD trips and open work
     orders; each opens the matching screen.

2. **Impact analysis** (the flow icon on the Command Centre's top tile):\
   The complete gas path as a
   Sankey, one column per stop: Reservoir, Pads & platforms, Gathering, Processing, Compression,
   Custody, Customers. Band width is the average daily flow over the latest 30 gas days; stations in
   alarm have a red outline; every customer is named (small ones with a leader line to their band);
   hover any stop or band for its name and flow.
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
   The site's equipment with a floating tag over each asset, in a hand-painted graphic-novel style. It
   opens on the complete site; scroll in to read the tags.
   - Onshore sites stand on a fenced gravel pad in the outback; the site ute is painted in the site's
     status colour. Offshore platforms and manifolds stand on a steel deck over a deep blue sea, with a
     crew boat (in the site's status colour), a supply vessel, buoys, gulls, a passing whale and other
     platforms on the horizon.
   - Each site has its own wind: the windsock, the flare flame and its smoke all point downwind.
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
     unit on the latest gas day; and the compressor fleet table, worst first (the
     vibration-to-remaining-life story is on the CPF002 Moomba compressors).
   - *Custody Transfer & Export:*\
     The worst unaccounted-for gas (UAF), metering stations, the UAF
     trend and the custody reconciliation.

6. **Analytics:**\
   The operational and HSE reports as folder tabs, on one line (each opens with the data period it
   covers):
   - *Integrity & RBI:*\
     Equipment due for inspection within 30 days, high-risk equipment, remaining
     life by area, and the inspection-due list.
   - *Safety & Alarms:*\
     ESD trips, alarms and critical events; ESD trips by area; recent safety events
     (click one for its asset).
   - *Emissions, Carbon & Safeguard:*\
     Carbon intensity, total CH4 and CO2, Scope 1 this year, the position
     against the Safeguard baseline and the ACCU cost; the intensity trend, CH4 by source and Scope 1
     by facility; the emissions ledger and the Safeguard position by facility and year.
   - *Maintenance:*\
     Open and P1 work orders, assets with failures, work-order cost, open
     notifications and recorded failures (these two open full lists); work orders by area, by state
     and failures by mode; the work-order table and the maintenance backlog.
   - *Lab & Assay:*\
     Samples across the value chain (wellhead, inlet, treated and sales gas), C1 and
     H2S; CO2 by sample point; recent assays.
   - *Weather:*\
     The hottest and windiest site (named; click to open it), sites at flood or lightning
     risk, and a map of every site coloured by average temperature. Click a site for its temperature,
     wind and risk history.
   - *Produced Water:*\
     Produced water, reinjection and disposal cost, the trend and produced water by
     well.
   - *Fuel Gas & Energy:*\
     Own-use and flaring shares, fuel gas, their trends and the daily ledger.
   - *Methane LDAR:*\
     CH4 and CO2-e, components to investigate, CH4 by area, and methane by component
     with the annual value of the escaping gas.
   - *Spares & Procurement:*\
     Assets at risk and on watch, the shortest remaining life, remaining life
     by asset, and exactly what to procure (part / repair / service) where the lead time beats the
     remaining life.

7. **Commercial & Finance** (the dollar-and-chart icon):\
   Opening it from the menu first shows the **AI briefing**, a box over the blurred tabs:
   - *Top 3 commercial actions:* one action per commercial lever (recover the biggest deferment cause,
     approve the best-paying capital project, close the supply gap on short contracts, re-price an
     under-market contract at renewal, cover the Safeguard shortfall, repair methane leaks), ranked by the
     money at stake per year (hover the green figure for how it is worked out). Each has the evidence and an
     **Open …** link that opens its report with the cause highlighted, as the Command Centre does.
   - *Genie's summary:* Genie writes it in the background (the conversation API) and Lakebase caches it,
     so it is ready before you open the menu; Genie is asked again only when the actions change. Until its
     summary is in, a short summary worked out from the same actions is shown.
   - *Don't show this again* / *Show it again in a week* (ticking one clears the other), then **Got it**,
     ✕, Esc or a click outside. The choice is saved for you in Lakebase.
   - *Bring it back:* the **✦ Top 3 actions** button at the top right of the tabs opens it any time, with
     your choice ticked; clear the tick to have it show on entry again.

   The money reports as folder tabs:
   - *Deferment & Losses:*\
     Total deferment in dollars and gas, by coded cause and by day; deferment by
     cause and asset.
   - *Cost & Economics:*\
     Revenue, deferment and carbon cost, revenue by day, and economics by asset.
   - *Commercial & Prices:*\
     Last month's sale revenue, cost of generation, profit, carbon and water
     disposal cost, the sales gas price and delivery vs nomination; monthly revenue, cost and profit;
     the price trend; the daily commercial ledger; and the contract book (price, MDQ, take-or-pay,
     nominated vs delivered, short days and contract end for every buyer).
   - *Molecule-to-Market Margin:*\
     Netback (dollars and per GJ), take-or-pay shortfall days, their
     trends and the daily margin and commitment ledger.
   - *Capital & Decline-Aware Investment:*\
     Projects, capex and expected uplift; capex by area; the
     development and capital schedule with payback and maturity (proposed, FEL-2, approved).

8. **Recommendation Inbox:**\
   Prescriptive recommendations with their cross-domain evidence and the
   asset's open work orders (linked). **Accept / Reject / Annotate** is written back and audited to
   Lakebase; once decided, the card shows an **Undo** instead.

9. **Asset Explorer:**\
   The semantic backbone, made tangible. On the **left**, type a tag, asset, area or signal and it resolves end to
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
  It also keeps what the app writes: recommendation decisions, each user's preferences (gas_twin.settings:
  the briefing choice, the report last opened in each menu, the folded groups in the 3D site list) and
  Genie's cached commercial briefing (gas_twin.ai_briefings).

- **Asset Intelligence Genie** (in Databricks One):\
  Ask questions in plain language over the gold
  marts, including the customer, supply-network and commercial-actions marts. "What are the top 3
  commercial actions we should take now, and why?" gives the same actions as the app's briefing.
- **AI/BI dashboard "Gas Upstream Digital Twin"** (Dashboards):\
  Five pages, Field Command Centre,
  Wells & Lift, Compression Health, Deferment & Emissions, and Custody & UAF.
