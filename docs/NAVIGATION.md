# Navigating the app

A guided tour of the Gas Upstream Digital Twin. The left menu is a flat list; open screens from top
to bottom for the natural story, or jump straight to what you need.

## Two ways to read it

- **Executive / Command Centre view**: start at the Command Centre and the Field Map for the state of
  the field, where production is at risk and what the single biggest constraint is right now.
- **Engineer / value-chain view**: walk the Gas flow chain stop by stop, then the cross-landscape
  reports, drilling into any asset's 360 as you go.

## Global interactions (everywhere)

- **Tiles** (the KPI cards at the top of a screen) are clickable: some navigate to a related screen,
  some open a full list in a drawer.
- **Table rows** open a context-aware **detail drawer**: a work-order row opens the work order, any
  other row opens that asset's 360 (telemetry sparklines, work orders, notifications, failures,
  procurement, lab assays, deferment, live tags). Click a sparkline to zoom it.
- **Charts** open a larger zoom modal when clicked.
- **Header links** (top right): "Ask Genie" everywhere; "Facility view" on value-chain pages.

## Screen by screen

1. **Command Centre** - the executive home. Field actual vs nominated production, production at risk,
   the current field constraint and why, open work orders, recorded failures, emissions and carbon
   intensity. Tiles and the constraint spine are clickable and drill down.

2. **Field Map** - the field laid out across the Australian gas basins. Each marker is a facility,
   sized by asset count and coloured by risk (green / amber / red). Click a marker to open an asset,
   or jump into the 3D navigator.

3. **Gas flow chain** - the five value-chain stops as horizontal tabs:
   - *Reservoirs & Wells* - well scorecard, decline, reservoir pressure, a 3D view.
   - *Gathering & Flow Assurance* - line conditions, hydrate margin (subcooling vs the hydrate curve).
   - *Processing Facility / CPF* - separation, dehydration (TEG), amine, dewpoint and spec margins.
   - *Compression Health* - vibration, efficiency, the flagship predictive story on CPF002.
   - *Custody Transfer & Export* - metered vs allocated volumes, unaccounted-for gas.

4. **Cross Landscape Reports** - cross-cutting reports as tabs:
   - *Integrity & RBI* - inspection due, corrosion, risk-based intervals.
   - *Safety & Alarms* - ESD trips, F&G and alarm events (per asset, clickable).
   - *Emissions & Carbon* - CH4/CO2/NOx by source, field carbon intensity.
   - *Deferment & Losses* - coded causes and dollar impact.
   - *Cost & Economics* - revenue, cost of generation and profit by asset.
   - *Planning & Maintenance* - the merged work-order and reliability view (EAM work orders,
     backlog, failures by mode). Click a work order to open it; click its asset cell to open the asset.
   - *Recommendation Inbox* - prescriptive recommendations with cross-domain evidence. **Accept /
     reject / annotate** is written back and audited to Lakebase. Once a decision is made the
     annotation box is replaced by an Undo.

5. **Lab & Assay** - gas-composition samples per well and at value-chain points (inlet / treated /
   sales gas), with CO2 by sample point.

6. **Commercial & Prices** - sales-gas and carbon prices, delivery vs nomination, and monthly
   revenue / cost of generation / profit.

7. **Weather & Environment** - per-location weather across the continent (tropical north to cool
   south): a map coloured by average temperature; click a site for its history.

8. **Produced Water Management** - produced-water volumes and disposal by well.

9. **Fuel-Gas & Energy Efficiency** - fuel-gas and flare consumption, efficiency trends.

10. **Carbon / Safeguard Compliance** - CO2 vs the Safeguard baseline by facility and year, with ACCU
    cost exposure.

11. **Methane LDAR** - measured plus emission-factor fugitive methane at the asset level, and the lost
    gas value.

12. **Molecule-to-Market Margin** - netback margin from the molecule through custody and contracts.

13. **Spares & Procurement Risk** - where lead time beats remaining useful life, and exactly what to
    procure (part / repair / service) with unit cost.

14. **Capital & Decline-Aware Investment** - the development and capex schedule by area, with expected
    uplift and payback.

15. **Asset & Document Explorer** - the semantic backbone, made tangible. On the **left**, type a tag,
    asset, area or signal and it resolves end to end: the decoded tag key, the asset (a link to its
    360), the functional location, the latest sensor reading with its normal operating range, and its
    ids in every other system (PI historian, EAM, OSDU, P&ID). On the **right**, browse the
    physical-location tree: field to area to asset to tag; click an asset to open its 360, click a tag
    to resolve it on the left.

16. **Ask me anything** - the Field Copilot. Ask a question in plain language and it answers over the
    gold marts via the Genie space, or use the link to jump straight into Genie.
