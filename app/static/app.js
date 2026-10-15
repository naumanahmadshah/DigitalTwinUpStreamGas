/* Upstream Gas Digital Twin v2 — SPA. Vanilla JS; inline-SVG charts; Leaflet map; Three.js 3D. */
const $ = (s) => document.querySelector(s);
const el = (h) => { const d = document.createElement("div"); d.innerHTML = h; return d.firstElementChild; };
async function getJSON(u) { const r = await fetch(u); if (!r.ok) throw new Error(u + " " + r.status); return r.json(); }
async function postJSON(u, b) { const r = await fetch(u, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(b) }); return r.json(); }
const SERIES = ["#4b8fe6", "#7b6cf0", "#37b98a", "#e0a53a", "#e0533a"];
const DEV = { red: "#e0533a", amber: "#e0a53a", green: "#37b98a" };
let NAV = [], CUR = null, _leaf = null;

function fmt(v) {
  if (v === null || v === undefined || v === "") return "–";
  if (typeof v === "number") {
    const a = Math.abs(v);
    if (a >= 1e9) return (v / 1e9).toFixed(2) + "B";
    if (a >= 1e6) return (v / 1e6).toFixed(2) + "M";
    if (a >= 1e3) return (v / 1e3).toFixed(1) + "k";
    return (Number.isInteger(v) ? v : v.toFixed(2)).toString();
  }
  return String(v);
}
function fmtK(v) { const a = Math.abs(v); if (a >= 1e6) return (v / 1e6).toFixed(1) + "M"; if (a >= 1e3) return (v / 1e3).toFixed(1) + "k"; return (Math.round(v * 100) / 100).toString(); }
function fmtDate(s) { const m = String(s).match(/^(\d{4})-(\d{2})-(\d{2})/); return m ? m[3] + "/" + m[2] : String(s); }
const _MON = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"];
const _MONS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
function fmtMD(s) { const m = String(s).match(/^(\d{4})-(\d{2})-(\d{2})/); return m ? (+m[3]) + " " + _MON[+m[2] - 1] : String(s); }
function fmtMon(s) { const m = String(s).match(/^(\d{4})-(\d{2})/); return m ? _MONS[+m[2] - 1] + " " + m[1].slice(2) : String(s); }
// Full human timestamp: "6 Oct 2026, 14:30" (date + time when present) — no ambiguous "06/10". (item 51)
function fmtWhen(s) { const m = String(s).match(/^(\d{4})-(\d{2})-(\d{2})(?:[T ](\d{2}):(\d{2}))?/); if (!m) return String(s); const d = (+m[3]) + " " + _MONS[+m[2] - 1] + " " + m[1]; return m[4] != null ? d + ", " + m[4] + ":" + m[5] : d; }
// Humanise a coded/underscored display value (gas_lift -> Gas lift). Leaves ids/hyphenated codes alone.
function humanVal(v) { return (typeof v === "string" && v.includes("_")) ? v.replace(/_/g, " ").replace(/^\w/, c => c.toUpperCase()) : v; }
// Pretty-print a coded engineering unit for display — never show "e3m3" or raw tokens on the UI.
function prettyUnit(u) { if (u === null || u === undefined || u === "") return ""; const m = { "e3m3/d": "10³ m³/d", "e3m3": "10³ m³", "degC": "°C", "pctLEL": "% LEL", "molpct": "mol %", "pct": "%", "kmh": "km/h" }; return m[u] || u; }
// Clean up any label/title/blurb so no caret unit ("10^3 m^3") or "e3m3" token reaches the UI.
function prettyLabel(s) { return String(s == null ? "" : s).replace(/10\^3\s*m\^3/g, "10³ m³").replace(/m\^3/g, "m³").replace(/\be3m3d\b/g, "10³ m³/d").replace(/\be3m3\b/g, "10³ m³"); }
function niceNum(x, round) { const e = Math.floor(Math.log10(x || 1)), f = (x || 1) / Math.pow(10, e); let nf; if (round) nf = f < 1.5 ? 1 : f < 3 ? 2 : f < 7 ? 5 : 10; else nf = f <= 1 ? 1 : f <= 2 ? 2 : f <= 5 ? 5 : 10; return nf * Math.pow(10, e); }

/* ---------- SVG charts ---------- */
function svgLine(rows, xk, series, opts) {
  opts = opts || {}; rows = rows || [];
  series = series.map((s, i) => ({ color: SERIES[i % SERIES.length], ...s }));
  const n = rows.length; let mn = Infinity, mx = -Infinity;
  const num = v => (v === null || v === undefined || v === "") ? NaN : Number(v);
  series.forEach(s => rows.forEach(r => { const x = num(r[s.key]); if (x === x) { mn = Math.min(mn, x); mx = Math.max(mx, x); } }));
  if (!(mn <= mx) || !n) return '<div class="muted">no data</div>';
  if (mn === mx) { mn -= 1; mx += 1; }
  const step = niceNum(((mx - mn) || 1) / 3, true); mn = Math.floor(mn / step) * step; mx = Math.ceil(mx / step) * step;
  const W = 640, H = 200, pL = 54, pR = 12, pT = 12, pB = 26;
  const sx = i => pL + (n <= 1 ? 0 : i * (W - pL - pR) / (n - 1));
  const sy = v => pT + (mx - v) * (H - pT - pB) / (mx - mn);
  const ticks = []; for (let t = mn; t <= mx + step * 1e-6; t += step) ticks.push(t);
  const grid = ticks.map(t => { const y = sy(t).toFixed(1); return `<line x1="${pL}" y1="${y}" x2="${W - pR}" y2="${y}" class="gl"/><text x="${pL - 5}" y="${(+y + 3)}" class="ax" text-anchor="end">${fmtK(t)}</text>`; }).join("");
  const xs = rows.map(r => r[xk]); const isDate = /^\d{4}-\d{2}-\d{2}/.test(String(xs[0] || ""));
  const spanDays = isDate && n > 1 ? (new Date(xs[n - 1]) - new Date(xs[0])) / 864e5 : 0;
  const xfmt = !isDate ? (v => String(v ?? "")) : (spanDays > 150 ? fmtMon : fmtDate);
  const xi = n <= 1 ? [0] : [0, Math.floor((n - 1) / 3), Math.floor(2 * (n - 1) / 3), n - 1];
  const xax = xi.map(i => `<text x="${sx(i).toFixed(1)}" y="${H - 7}" class="axb" text-anchor="${i === 0 ? 'start' : i === n - 1 ? 'end' : 'middle'}">${xfmt(xs[i])}</text>`).join("");
  const zero = (mn < 0 && mx > 0) ? `<line x1="${pL}" y1="${sy(0).toFixed(1)}" x2="${W - pR}" y2="${sy(0).toFixed(1)}" class="axis0"/>` : "";
  const paths = series.map(s => {
    const pts = rows.map((r, i) => ({ i, y: num(r[s.key]) })).filter(p => p.y === p.y);
    if (!pts.length) return "";
    const d = pts.map((p, k) => (k ? "L" : "M") + sx(p.i).toFixed(1) + " " + sy(p.y).toFixed(1)).join(" ");
    return `<path d="${d}" class="line" style="stroke:${s.color}"${s.dash ? ' stroke-dasharray="5 4"' : ''}/>`;
  }).join("");
  const legend = series.length > 1 ? `<div class="chart-legend">${series.map(s => `<span class="lg"><span class="lg-s" style="background:${s.color}"></span>${s.label || s.key}</span>`).join("")}</div>` : "";
  return `${legend}<svg viewBox="0 0 ${W} ${H}" class="chart">${grid}${zero}${xax}${paths}</svg>`;
}
function svgBar(rows, xk, yk, opts) {
  opts = opts || {}; const pts = rows.map(r => ({ x: String(r[xk]), y: Number(r[yk]) })).filter(p => p.y === p.y);
  if (!pts.length) return '<div class="muted">no data</div>';
  const n = pts.length, pL = 48, pR = 12, pT = 12, pB = 84, BARW = 26, H = 268;
  const minPlot = 680 - pL - pR, plotW = Math.max(minPlot, n * BARW), W = pL + pR + plotW, wide = plotW > minPlot + 1;
  const dataMax = Math.max(...pts.map(p => p.y), 0) || 1, step = niceNum(dataMax / 3, true), top = Math.ceil(dataMax / step) * step;
  const plotH = H - pT - pB, baseY = pT + plotH, bw = plotW / n, yOf = v => pT + (1 - v / top) * plotH;
  const grid = []; for (let t = 0; t <= top + step * 1e-6; t += step) { const y = yOf(t).toFixed(1); grid.push(`<line x1="${pL}" y1="${y}" x2="${W - pR}" y2="${y}" class="gl"/><text x="${pL - 6}" y="${+y + 3}" class="ax" text-anchor="end">${fmtK(t)}</text>`); }
  const mean = pts.reduce((a, p) => a + p.y, 0) / n, dev = opts.color === "lowbad";
  const fill = y => y < mean * 0.75 ? DEV.red : y < mean * 0.93 ? DEV.amber : DEV.green;
  const bars = pts.map((p, i) => { const h = Math.max(1, (p.y / top) * plotH), x = pL + i * bw, y = baseY - h; return `<rect x="${(x + 1.5).toFixed(1)}" y="${y.toFixed(1)}" width="${Math.max(1, bw - 3).toFixed(1)}" height="${h.toFixed(1)}" class="bar"${dev ? ` style="fill:${fill(p.y)}"` : ''}/>`; }).join("");
  // keep labels ACCURATE (never rewrite CPF001->CPF1); just truncate the long ones, full text on hover
  const short = s => { const x = String(s); return x.length > 18 ? x.slice(0, 17) + "…" : x; };
  const ly = (baseY + 20).toFixed(1);
  const labels = pts.map((p, i) => { const cx = (pL + i * bw + bw / 2).toFixed(1); return `<text x="${cx}" y="${ly}" class="axb" text-anchor="end" transform="rotate(-45 ${cx} ${ly})"><title>${escAttr(p.x)}</title>${short(p.x)}</text>`; }).join("");
  const svg = `<svg viewBox="0 0 ${W} ${H}"${wide ? ` style="width:${W}px;max-width:none"` : ' class="chart"'}>${grid.join("")}<line x1="${pL}" y1="${baseY}" x2="${W - pR}" y2="${baseY}" class="gl"/>${bars}${labels}</svg>`;
  return wide ? `<div style="overflow-x:auto;max-width:100%">${svg}</div>` : svg;
}

/* ---------- shared render helpers ---------- */
const escAttr = (s) => String(s).replace(/"/g, "&quot;").replace(/</g, "&lt;");
function infoI(text) { return `<span class="infoi" title="${escAttr(text)}">i</span>`; }
// Resolve a short explanation for a metric from its label (keyword match), with a sensible default.
function kpiDesc(label) {
  const L = String(label).toLowerCase();
  const map = [
    ["producing wells", "Wells producing on the latest gas day."],
    ["csg wells", "Coal-seam-gas wells (lower-rate, higher-count completions) in the field."],
    ["carbon cost", "Cost of CO₂-equivalent emissions over the period, priced at the carbon price."],
    ["carbon price", "Market / ACCU carbon price for the day (cost to offset a tonne of CO₂-e), not a price the operator receives."],
    ["sales gas price", "The daily sales-gas price the operator SELLS at (the gas-sales-agreement reference price), not a buying or spot-only price."],
    ["gas price", "The daily sales-gas price the operator SELLS at (the gas-sales-agreement reference price), not a buying or spot-only price."],
    ["delivery vs nomination", "Gas delivered as a share of the day's contractual nomination (not of production); can exceed 100% when the buyer nominates up and the field delivers above the base nomination."],
    ["nomination delivery", "Gas delivered as a share of the day's contractual nomination (not of production); can exceed 100% when the buyer takes above the base nomination."],
    ["sale revenue", "Produced gas sold, valued at the sales-gas price, for the latest month."],
    ["cost of generation", "Lifting cost to produce the gas (operating cost) for the latest month."],
    ["profit", "Sale revenue minus cost of generation and carbon cost for the latest month."],
    ["carbon cost", "Cost of CO₂-e emissions at the carbon price for the latest month."],
    ["water disposal", "Produced-water reinjection and treatment cost for the latest month."],
    ["samples", "Laboratory assay samples analysed (LIMS)."],
    ["avg c1", "Methane (C1) mole fraction in the sampled gas."],
    ["h2s", "Hydrogen sulphide in the sampled gas (ppm); a sweetening and spec driver."],
    ["ambient", "Ambient air temperature; drives compression energy, dewpoint margin and flaring."],
    ["wind", "Wind speed; an HSE and flaring-dispersion driver."],
    ["flood", "Days with elevated flood risk at the field location."],
    ["wo cost", "Total work-order cost (labour and parts) over the period."],
    ["assets with failures", "Assets with one or more recorded failures in the reliability history."],
    ["p1 open", "Priority-1 (safety or production-critical) work orders currently open."],
    ["open notifications", "EAM notifications currently open (anomaly, inspection-due, corrosion, operator flags)."],
    ["recorded failures", "Failure events in the ISO 14224 reliability history."],
    ["overdue", "Open work orders past their target completion date."],
    ["produced water", "Produced-water rate, derived from gas rate × the well's PVT water-gas ratio."],
    ["reinjected", "Share of produced water routed to reinjection."],
    ["disposal cost", "Cost of produced-water reinjection and treatment."],
    ["own-use", "Fuel gas burned for own use as a share of production."],
    ["flaring", "Flared gas as a share of production."],
    ["fuel gas", "Own-use fuel gas burned for compression and power."],
    ["scope-1", "Scope-1 CO₂ emitted this reporting year."],
    ["position vs baseline", "Scope-1 minus the Safeguard baseline; positive means over baseline."],
    ["accu cost", "Cost to cover a Safeguard shortfall with carbon credits at the carbon price."],
    ["ch₄", "Methane emitted (measured plus factor-based fugitive)."],
    ["co₂-e", "Methane expressed as CO₂-equivalent (GWP 28)."],
    ["components to investigate", "Components whose LDAR triage flags them to investigate."],
    ["netback", "Netback margin: revenue minus lifting and carbon cost."],
    ["take-or-pay", "Days production fell below the contracted take-or-pay floor."],
    ["assets at risk", "Assets whose spare's lead time beats their remaining life, with none in stock."],
    ["on watch", "Assets whose spare's lead time beats their remaining life."],
    ["projects", "Development / capital projects in the schedule."],
    ["capex", "Total capital cost of the planned projects."],
    ["expected uplift", "Production uplift the planned projects are expected to deliver."],
    ["line pressure", "Average gathering-line pressure; high backpressure chokes well deliverability."],
    ["metering station", "Fiscal custody metering stations the twin reconciles against allocation."],
    ["equipment due", "Equipment whose risk-based inspection falls due within the window."],
    ["high-risk equipment", "Equipment the RBI model flags as high risk (short remaining life or overdue inspection)."],
    ["critical event", "Safety events classed critical (ESD trips and confirmed F&G) over the period."],
    ["distinct causes", "Distinct ISO-14224-style coded deferment causes seen in the ledger."],
    ["functional location", "Nodes in the functional-location hierarchy (area to system to equipment)."],
    ["assets", "Distinct physical assets in the semantic backbone."],
    ["liquid-loading", "Wells flagged at risk of liquid loading: gas velocity drops too low to lift liquids, which accumulate and choke flow. Advisory watchlist from the gold well model."],
    ["sssv", "Wells whose sub-surface safety valve last function-test did not pass. Status only; the twin never actuates safety systems."],
    ["utilization", "Average well production as a share of its recent peak deliverability (a nodal-performance proxy)."],
    ["nodal", "Well performance against its inflow/outflow potential."],
    ["rul", "Model estimate of remaining useful life before likely failure. Advisory, from the compression health model."],
    ["health", "Composite asset health score (0 to 100) built from vibration, efficiency and condition features."],
    ["anti-surge", "Minimum anti-surge margin across the compressor fleet; a low margin risks a surge trip."],
    ["production at risk", "Production that would be lost if the constraint is not relieved (10³m³/d)."],
    ["deferment", "Lost-production value over the period, priced at the gas price."],
    ["deferred", "Lost-production volume with its coded cause."],
    ["carbon", "CO₂-equivalent emitted per 10³m³ of gas produced (carbon intensity)."],
    ["ch4", "Methane mass emitted over the period."],
    ["co2", "CO₂ mass emitted over the period."],
    ["dewpoint", "Processing streams at, or close to, their water-dewpoint specification."],
    ["marginal", "Streams within 20% of a contractual spec limit (dewpoint, H2S or CO2)."],
    ["ngl", "NGL recovery against the target recovery."],
    ["uaf", "Unaccounted-for gas: the gap between metered and allocated volumes."],
    ["hydrate", "Hydrate approach margin: how far gathering-line temperature sits above the gas-hydrate formation temperature. A small margin means a segment is close to forming hydrates (watch < 8 °C, at risk < 3 °C)."],
    ["pig", "Gathering segments with a pigging run due soon."],
    ["inspection", "Equipment approaching its risk-based-inspection due date."],
    ["remaining life", "Estimated remaining service life before an integrity limit is reached."],
    ["esd", "Emergency-shutdown trips recorded. Status only; the twin never actuates safety systems."],
    ["alarm", "Alarm activity used for rationalization and flood analysis (not annunciation)."],
    ["work order", "Open maintenance work orders (EAM)."],
    ["revenue", "Produced gas valued at the period gas price."],
    ["cost", "Operating cost over the period."],
    ["tags in registry", "Signals in the tag registry, each resolving to its asset via the AREA-SYSTEM-CLASS-NNNN key."],
    ["stale", "Cross-system links that are stale or missing, which the contextualization layer flags."],
  ];
  for (const [k, d] of map) if (L.includes(k)) return d;
  return "Field metric derived from the governed gold data for this value-chain stop.";
}
function kpiRow(kpis) {
  if (!kpis || !kpis.length) return "";
  return `<div class="kpis">${kpis.map(k => `<div class="kpi ${(k.link || k.list) ? "click" : ""}"${k.link ? ` data-link="${k.link}"` : ""}${k.list ? ` data-list="${k.list}"` : ""}><div class="kpi-v">${fmt(k.value)}</div><div class="kpi-l">${prettyLabel(k.label)} ${infoI(k.desc || kpiDesc(k.label))}</div></div>`).join("")}</div>`;
}
function pill(v) { const c = { red: "red", amber: "amber", green: "green", watch: "watch", not_passed: "red", high: "red", due: "amber", ok: "green", pass: "green", at_risk: "red", investigate: "red", monitor: "amber", critical: "red", warning: "amber", advisory: "green", major: "amber" }[String(v)]; const t = humanVal(v); return c ? `<span class="pill ${c}">${t}</span>` : (t ?? "–"); }
let _tblSeq = 0;
function tableHTML(cols, rows, opts) {
  opts = opts || {};
  if (!rows || !rows.length) return '<div class="muted">no rows</div>';
  const key = c => (c && c.key) ? c.key : c, lab = c => (c && c.label) ? c.label : c, dsc = c => (c && c.desc) ? c.desc : "";
  const isNum = k => /score|risk|dollars|e3m3|eff|vibration|rul|health|pct|days|kg|_c$|temp|pressure|count|wos|life|failures|assets|utiliz|price|flow|cost|downtime|lat$|lon$|molpct|ppm|water_m3|scope1|baseline|position|co2e|netback|capex|uplift|payback|lead_time|on_hand|margin/i.test(k);
  const PILL = ["pin_colour", "rbi_status", "liquid_loading_watch", "sssv_status", "status", "priority", "exposure", "survey_status", "hydrate_status", "severity"];
  const cell = (k, v) => (typeof v === "boolean") ? (v ? '<span class="pill red">yes</span>' : '<span class="pill green">no</span>') : (PILL.includes(k) ? pill(v) : (isNum(k) && typeof v === "number" ? fmt(v) : humanVal(v ?? "–")));
  const sv = (k, v) => (v === null || v === undefined) ? "" : String(v);  // raw sort value (data-v)
  const keys = cols.map(key);
  // the row's primary entity: a work order if it has wo_id, otherwise the asset/well. Clicking the row
  // opens that entity's detail; in a work-order row the asset-id cell still opens the ASSET (item 29).
  const woCol = keys.indexOf("wo_id") >= 0 ? "wo_id" : null;
  const assetCol = keys.indexOf("asset_id") >= 0 ? "asset_id" : (keys.indexOf("well_id") >= 0 ? "well_id" : null);
  const entity = woCol ? "workorder" : (assetCol ? "asset" : null);
  const primaryCol = woCol || assetCol;
  const clickable = !!(opts.rowClick && entity);
  const tid = "t" + (++_tblSeq);
  const head = cols.map((c, i) => `<th class="sortable ${isNum(key(c)) ? "num" : ""}" data-ci="${i}" data-num="${isNum(key(c)) ? 1 : 0}">${lab(c)}${dsc(c) ? " " + infoI(dsc(c)) : ""}<span class="sarrow"></span></th>`).join("");
  const body = rows.map(r => {
    const pid = primaryCol ? escAttr(r[primaryCol] ?? "") : "";
    const tr = clickable && pid ? `<tr class="rowclick" data-entity="${entity}" data-id="${pid}">` : "<tr>";
    return tr + cols.map(c => {
      const k = key(c), v = r[k];
      const assetCell = (entity === "workorder" && k === "asset_id" && v != null && v !== "") ? ` data-asset-id="${escAttr(v)}"` : "";
      return `<td class="${isNum(k) ? "num" : ""}${assetCell ? " cell-link" : ""}"${assetCell} data-v="${escAttr(sv(k, v))}">${cell(k, v)}</td>`;
    }).join("") + "</tr>";
  }).join("");
  return `<div style="overflow:auto;max-height:520px"><table id="${tid}" class="dtbl"><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table></div>`;
}
// click a sortable header: sort the table's rows by that column (numeric-aware), toggling asc/desc.
function sortByTh(th) {
  const table = th.closest("table"); if (!table || !table.tBodies.length) return;
  const ci = +th.getAttribute("data-ci"), num = th.getAttribute("data-num") === "1";
  const prevCi = table.getAttribute("data-sci"), prevDir = +table.getAttribute("data-sdir") || 0;
  const dir = (String(ci) === prevCi && prevDir === 1) ? -1 : 1;
  const tb = table.tBodies[0], trs = Array.from(tb.rows);
  trs.sort((a, b) => {
    let x = a.cells[ci].getAttribute("data-v"), y = b.cells[ci].getAttribute("data-v");
    if (num) { x = parseFloat(x); y = parseFloat(y); if (isNaN(x)) x = -Infinity; if (isNaN(y)) y = -Infinity; return dir * (x - y); }
    return dir * String(x).localeCompare(String(y), undefined, { numeric: true });
  });
  trs.forEach(r => tb.appendChild(r));
  table.setAttribute("data-sci", ci); table.setAttribute("data-sdir", dir);
  table.querySelectorAll("th .sarrow").forEach(s => s.textContent = "");
  const ar = th.querySelector(".sarrow"); if (ar) ar.textContent = dir === 1 ? " ▲" : " ▼";
}
function chartsHTML(charts) {
  if (!charts || !charts.length) return "";
  return `<div class="grid">${charts.map((c, i) => {
    const series = (c.opts && c.opts.series) ? c.opts.series
      : [{ key: c.y, label: c.title }].concat((c.title.includes("nomination") || (c.rows[0] && c.rows[0].nominated_e3m3 !== undefined)) ? [{ key: "nominated_e3m3", label: "nominated" }] : []);
    const body = c.type === "bar" ? svgBar(c.rows, c.x, c.y, c.opts) : svgLine(c.rows, c.x, series, c.opts);
    return `<div class="card"><div class="card-h">${prettyLabel(c.title)}</div>${body}</div>`;
  }).join("")}</div>`;
}

/* ---------- asset detail drawer + telemetry sparklines + chart zoom (shared across value-chain stops) ---------- */
function valueChainMods() { return (NAV || []).filter(n => n.group === "Value chain"); }
function crossMods() { return (NAV || []).filter(n => n.group === "Cross-cutting"); }
let GFC_CUR = null, GFC_CROSS = null, PENDING_SCENE = null;
// navigate to any module; value-chain stops open inside the Gas-flow-chain tab bar, and cross-cutting
// reports open inside its "Cross Landscape Reports" sub-tabs (item 28).
function goTo(key) {
  if (valueChainMods().some(m => m.key === key)) { GFC_CUR = key; select("gas_flow_chain"); }
  else if (crossMods().some(m => m.key === key)) { GFC_CROSS = key; select("cross_landscape"); }
  else select(key);
}
// tiny inline sparkline (no axes) for a telemetry series; click enlarges it
function sparkline(points, color) {
  const pts = (points || []).map(p => Number(p.value)).filter(y => y === y);
  if (pts.length < 2) return '<div class="muted" style="font-size:11px">no series</div>';
  const W = 240, H = 54, pad = 4;
  let mn = Math.min(...pts), mx = Math.max(...pts); if (mn === mx) { mn -= 1; mx += 1; }
  const sx = i => pad + i * (W - 2 * pad) / (pts.length - 1);
  const sy = v => pad + (mx - v) * (H - 2 * pad) / (mx - mn);
  const d = pts.map((y, i) => (i ? "L" : "M") + sx(i).toFixed(1) + " " + sy(y).toFixed(1)).join(" ");
  return `<svg viewBox="0 0 ${W} ${H}" class="spark" preserveAspectRatio="none"><path d="${d}" style="fill:none;stroke:${color || '#2f7dc4'};stroke-width:1.6"/></svg>`;
}
// enlarge a telemetry series into a full chart with the rest of the UI dimmed; ✕ or Esc closes it
function openChartModal(title, points, unit) {
  let m = $("#chartmodal");
  if (!m) {
    m = el(`<div id="chartmodal"><div class="cm-box"><div class="cm-head"><span class="cm-t"></span><button class="cm-x" title="Close">✕</button></div><div class="cm-body"></div></div></div>`);
    document.body.appendChild(m);
    m.addEventListener("click", e => { if (e.target === m || e.target.closest(".cm-x")) m.classList.remove("open"); });
  }
  m.querySelector(".cm-t").textContent = title + (unit ? ` (${unit})` : "");
  m.querySelector(".cm-body").innerHTML = svgLine(points, "gas_day", [{ key: "value", label: title }]);
  m.classList.add("open");
}
function closeDrawer() { const w = $("#drawerwrap"); if (w) w.classList.remove("open"); }
function ensureDrawer() {
  if ($("#drawerwrap")) return;
  const w = el(`<div id="drawerwrap"><div id="drawerback"></div><aside id="assetdrawer"></aside></div>`);
  document.body.appendChild(w);
  $("#drawerback").onclick = closeDrawer;
  document.addEventListener("keydown", e => {
    if (e.key !== "Escape") return;
    const cm = $("#chartmodal");
    if (cm && cm.classList.contains("open")) cm.classList.remove("open"); else closeDrawer();
  });
}
// Open the right-side detail tile for any asset (facility icon, a table row, or the 3D scene).
async function openAssetDrawer(id, sceneId) {
  if (!id) return;
  ensureDrawer();
  const wrap = $("#drawerwrap"), dr = $("#assetdrawer");
  wrap.classList.add("open"); dr.classList.remove("wide");
  dr.innerHTML = `<div class="dr-head"><span class="dr-title">Loading ${id}…</span><button class="dr-x" title="Close">✕</button></div>`;
  dr.querySelector(".dr-x").onclick = closeDrawer;
  let d, s, ctx = {};
  try { [d, s, ctx] = await Promise.all([getJSON("/api/asset/" + encodeURIComponent(id)), getJSON("/api/asset_series/" + encodeURIComponent(id)), getJSON("/api/asset_context/" + encodeURIComponent(id))]); }
  catch (e) { const t = dr.querySelector(".dr-title"); if (t) t.textContent = id + " — unavailable"; return; }
  const st = d.state || {}, name = st.asset_id || id, reco = (d.recommendations || [])[0], sers = s.series || [];
  const kv = (k, val) => `<div class="kv"><span>${k}</span><span>${val}</span></div>`;
  const sparks = sers.length
    ? `<div class="dr-sec">Telemetry over time <span class="muted" style="font-weight:400">· click to enlarge</span></div><div class="sparkgrid">${sers.map((se, i) => `<div class="sparkcard" data-si="${i}"><div class="sparkt">${humanVal(se.signal)}${se.unit ? ` <span class="muted">${prettyUnit(se.unit)}</span>` : ""}</div>${sparkline(se.points, SERIES[i % SERIES.length])}</div>`).join("")}</div>`
    : "";
  const tagRows = (d.tags || []).slice(0, 20);
  const defers = (d.deferment || []).slice(0, 8);
  const wos = (d.workorders || []).slice(0, 12);
  dr.innerHTML = `<div class="dr-head"><span class="dr-title">${name} ${pill(st.pin_colour)}</span><button class="dr-x" title="Close">✕</button></div>
    <div class="dr-scroll">
      ${kv("Type", humanVal(st.equipment_type || "–"))}
      ${kv("Area", `<a href="#" class="drlink" data-area="${escAttr(st.area_code || "")}">${st.area_code || "–"}</a>`)}
      ${st.criticality ? kv("Criticality", st.criticality) : ""}
      ${st.manufacturer ? kv("Manufacturer", st.manufacturer) : ""}
      ${kv("Health score", fmt(st.health_score))}
      ${kv("Production at risk", fmt(st.production_at_risk_e3m3d) + " ×10³m³/d")}
      ${kv("Open work orders", fmt(st.open_wos))}
      ${reco ? `<div class="banner" style="margin:10px 0">${reco.recommendation}${reco.evidence ? `<div class="ev">${reco.evidence}</div>` : ""}</div>` : ""}
      ${sparks}
      ${wos.length ? `<div class="dr-sec">Work orders for this asset</div><table class="dtbl"><tbody>${wos.map(w => `<tr><td>${w.wo_id}</td><td>${humanVal(w.wo_type)} ${pill(w.priority)}</td><td>${humanVal(w.status)}</td></tr>`).join("")}</tbody></table>` : ""}
      ${(ctx.notifications || []).length ? `<div class="dr-sec">Notifications</div><table class="dtbl"><tbody>${ctx.notifications.map(n => `<tr><td>${n.notif_id}</td><td>${humanVal(n.notif_type)} ${pill(n.priority)}</td><td>${humanVal(n.status)}</td></tr>`).join("")}</tbody></table>` : ""}
      ${(ctx.failures || []).length ? `<div class="dr-sec">Failure history (ISO 14224)</div><table class="dtbl"><tbody>${ctx.failures.map(f => `<tr><td>${fmtMD(f.failure_date)}</td><td>${f.iso14224_code} · ${humanVal(f.failure_mode)}</td><td class="num">${fmt(f.downtime_hours)} h</td></tr>`).join("")}</tbody></table>` : ""}
      ${(ctx.spares || []).length ? `<div class="dr-sec">Procurement — what to order</div><table class="dtbl"><tbody>${ctx.spares.map(sp => `<tr><td>${humanVal(sp.procurement_type)}</td><td>${humanVal(sp.part_name)}</td><td class="num">lead ${fmt(sp.lead_time_days)}d · ${fmt(sp.unit_cost)} · ${pill(sp.exposure)}</td></tr>`).join("")}</tbody></table>` : ""}
      ${(ctx.lab || []).length ? `<div class="dr-sec">Lab assays</div><table class="dtbl"><tbody>${ctx.lab.map(l => `<tr><td>${fmtMD(l.sample_date)}</td><td>${humanVal(l.sample_point)}</td><td class="num">C₁ ${fmt(l.c1_molpct)} · CO₂ ${fmt(l.co2_molpct)} · H₂S ${fmt(l.h2s_ppm)}</td></tr>`).join("")}</tbody></table>` : ""}
      ${defers.length ? `<div class="dr-sec">Deferment timeline</div><table class="dtbl"><tbody>${defers.map(x => `<tr><td>${fmtMD(x.gas_day)}</td><td>${humanVal(x.cause_desc)}</td><td class="num">${fmt(x.deferred_dollars)}</td></tr>`).join("")}</tbody></table>` : ""}
      ${tagRows.length ? `<div class="dr-sec">Live tags</div><table class="dtbl"><tbody>${tagRows.map(t => `<tr><td>${humanVal(t.signal)}</td><td class="num">${fmt(t.latest_value)} ${prettyUnit(t.unit)}</td></tr>`).join("")}</tbody></table>` : ""}
      <div style="margin-top:14px"><button class="btn" id="dr3d">View in 3D</button></div>
    </div>`;
  dr.querySelector(".dr-x").onclick = closeDrawer;
  dr.querySelectorAll(".sparkcard").forEach(c => c.onclick = () => { const se = sers[+c.getAttribute("data-si")]; openChartModal(humanVal(se.signal), se.points, se.unit); });
  // click the asset's area to jump to the Work Orders tab (its work orders in context) — item 26
  const al = dr.querySelector(".drlink[data-area]"); if (al) al.onclick = (e) => { e.preventDefault(); closeDrawer(); goTo("maintenance"); };
  const b3d = dr.querySelector("#dr3d"); if (b3d) b3d.onclick = () => { PENDING_SCENE = sceneId || st.scene_id || null; closeDrawer(); select("navigator"); };
}

// Work-order detail (item 29): the WO itself, with a link to its asset and a notifications sublink.
async function openWoDrawer(woId) {
  if (!woId) return;
  ensureDrawer();
  const wrap = $("#drawerwrap"), dr = $("#assetdrawer");
  wrap.classList.add("open"); dr.classList.remove("wide");
  dr.innerHTML = `<div class="dr-head"><span class="dr-title">Loading ${woId}…</span><button class="dr-x" title="Close">✕</button></div>`;
  dr.querySelector(".dr-x").onclick = closeDrawer;
  let d; try { d = await getJSON("/api/workorder/" + encodeURIComponent(woId)); } catch (e) { const t = dr.querySelector(".dr-title"); if (t) t.textContent = woId + " — unavailable"; return; }
  const w = d.wo || {}, notifs = d.notifications || [];
  const kv = (k, val) => `<div class="kv"><span>${k}</span><span>${val}</span></div>`;
  dr.innerHTML = `<div class="dr-head"><span class="dr-title">${w.wo_id || woId} ${pill(w.priority)}</span><button class="dr-x" title="Close">✕</button></div>
    <div class="dr-scroll">
      ${kv("Type", humanVal(w.wo_type || "–"))}
      ${kv("Status", humanVal(w.status || "–"))}
      ${kv("Assigned to", w.assigned_to || "–")}
      ${kv("Target completion", w.target_completion || "–")}
      ${kv("Created", fmtMD(w.created_date))}
      ${kv("Cost", fmt(w.cost))}
      ${w.asset_id ? kv("Asset", `<a href="#" class="drlink" data-asset-id="${escAttr(w.asset_id)}">${w.asset_id}</a>`) : ""}
      ${w.description ? `<div class="banner" style="margin:10px 0">${w.description}</div>` : ""}
      ${notifs.length ? `<div class="dr-sec">Notifications on this asset</div><table class="dtbl"><tbody>${notifs.map(n => `<tr><td>${n.notif_id}</td><td>${humanVal(n.notif_type)} ${pill(n.priority)}</td><td>${humanVal(n.status)}</td></tr>`).join("")}</tbody></table>` : `<div class="muted" style="margin-top:12px">No notifications on this asset.</div>`}
    </div>`;
  dr.querySelector(".dr-x").onclick = closeDrawer;
  const al = dr.querySelector(".drlink[data-asset-id]"); if (al) al.onclick = (e) => { e.preventDefault(); openAssetDrawer(al.getAttribute("data-asset-id")); };
}
// Full notification / failure lists, opened from the KPI tiles (item 29). Rows link to their asset.
async function openListDrawer(kind) {
  ensureDrawer();
  const wrap = $("#drawerwrap"), dr = $("#assetdrawer");
  wrap.classList.add("open"); dr.classList.add("wide");
  dr.innerHTML = `<div class="dr-head"><span class="dr-title">Loading…</span><button class="dr-x" title="Close">✕</button></div>`;
  dr.querySelector(".dr-x").onclick = closeDrawer;
  let d; try { d = await getJSON("/api/list/" + encodeURIComponent(kind)); } catch (e) { return; }
  dr.innerHTML = `<div class="dr-head"><span class="dr-title">${d.title || "List"} <span class="muted" style="font-weight:400">${(d.rows || []).length}</span></span><button class="dr-x" title="Close">✕</button></div>
    <div class="dr-scroll"><div class="muted rowhint">Click a row to open its asset.</div>${tableHTML(d.columns, d.rows, { rowClick: true })}</div>`;
  dr.querySelector(".dr-x").onclick = closeDrawer;
}

/* ---------- screens ---------- */
// Bold Command Centre: the field read as a left-to-right flow system; the binding constraint is the villain.
function _kv(kpis) { const m = {}; (kpis || []).forEach(k => { m[k.label] = k.value; }); return m; }

function ccSpine(cons, bindingTop) {
  const stopOf = { "compression": "Processing", "liquid loading": "Wells", "custody UAF": "Custody" };
  const stops = ["Reservoir", "Wells", "Gathering", "Processing", "Custody"];
  const risk = {};
  (cons || []).forEach(c => { const s = stopOf[c.constraint_type] || "Processing"; risk[s] = (risk[s] || 0) + (Number(c.production_at_risk_e3m3d) || 0); });
  const ranked = Object.keys(risk).sort((a, b) => risk[b] - risk[a]);
  const binding = ranked[0] || "Processing";              // highest-risk stop (red)
  const secondary = ranked[1] || null;                    // data-backed: 2nd-highest-risk stop binds next (amber)
  const stopGoto = { "Reservoir": "reservoir_wells", "Wells": "reservoir_wells", "Gathering": "gathering", "Processing": "processing", "Custody": "custody" };
  return `<div class="cc2-spine">${stops.map((s, i) => {
    const isBind = s === binding;
    const isNext = !isBind && s === secondary;
    const state = isBind ? "bind" : (isNext ? "watch" : "ok");
    const sub = isBind ? fmt(risk[s] || 0) + " ×10³m³/d at risk" : (isNext ? "next" : "nominal");
    return `<div class="cc2-stop ccgo ${state}" data-goto="${stopGoto[s]}">
        <span class="cc2-node"></span>
        <span class="cc2-stop-name">${s}</span>
        <span class="cc2-stop-sub">${sub}</span>
      </div>${i < stops.length - 1 ? '<span class="cc2-pipe"></span>' : ""}`;
  }).join("")}</div>`;
}

function ccArea(rows) {
  rows = rows || []; const n = rows.length; if (!n) return '<div class="muted">no data</div>';
  const num = v => Number(v);
  let mn = Infinity, mx = -Infinity;
  rows.forEach(r => [num(r.actual_e3m3), num(r.nominated_e3m3)].forEach(x => { if (x === x) { mn = Math.min(mn, x); mx = Math.max(mx, x); } }));
  if (!(mn <= mx)) return '<div class="muted">no data</div>';
  const step = niceNum(((mx - mn) || 1) / 3, true); mn = Math.floor(mn / step) * step; mx = Math.ceil(mx / step) * step;
  const W = 1000, H = 236, pL = 96, pR = 30, pT = 16, pB = 40;
  const sx = i => pL + (n <= 1 ? 0 : i * (W - pL - pR) / (n - 1));
  const sy = v => pT + (mx - v) * (H - pT - pB) / (mx - mn);
  const ticks = []; for (let t = mn; t <= mx + step * 1e-6; t += step) ticks.push(t);
  const grid = ticks.map(t => `<line x1="${pL}" y1="${sy(t).toFixed(1)}" x2="${W - pR}" y2="${sy(t).toFixed(1)}" class="gl"/><text x="${pL - 6}" y="${(sy(t) + 3).toFixed(1)}" class="ax" text-anchor="end">${fmtK(t)}</text>`).join("");
  const xs = rows.map(r => r.gas_day); const xi = [0, Math.floor((n - 1) / 3), Math.floor(2 * (n - 1) / 3), n - 1];
  const ly = H - pB + 24;  // horizontal labels in the bottom margin; edge labels anchored in so nothing clips
  const xax = xi.map((i, k) => { const cx = sx(i).toFixed(1); const anchor = k === 0 ? "start" : (k === xi.length - 1 ? "end" : "middle"); return `<text x="${cx}" y="${ly}" class="axb" text-anchor="${anchor}">${fmtMD(xs[i])}</text>`; }).join("");
  // shortfall band: fill between nominated (top) and actual (bottom) where actual < nominated
  const seg = [];
  rows.forEach((r, i) => { const a = num(r.actual_e3m3), nm = num(r.nominated_e3m3); if (a === a && nm === nm && a < nm) seg.push(i); });
  let band = "";
  if (seg.length > 1) {
    const topPts = seg.map(i => `${sx(i).toFixed(1)},${sy(num(rows[i].nominated_e3m3)).toFixed(1)}`);
    const botPts = seg.slice().reverse().map(i => `${sx(i).toFixed(1)},${sy(num(rows[i].actual_e3m3)).toFixed(1)}`);
    band = `<polygon points="${topPts.concat(botPts).join(" ")}" fill="#DD4B1A" opacity="0.14"/>`;
  }
  const line = (key, cls, dash) => { const pts = rows.map((r, i) => ({ i, y: num(r[key]) })).filter(p => p.y === p.y); const dd = pts.map((p, k) => (k ? "L" : "M") + sx(p.i).toFixed(1) + " " + sy(p.y).toFixed(1)).join(" "); return `<path d="${dd}" class="${cls}"${dash ? ' stroke-dasharray="6 5"' : ""}/>`; };
  return `<svg viewBox="0 0 ${W} ${H}" class="cc2-chartsvg">${grid}${band}${line("nominated_e3m3", "cc2-ln-nom", true)}${line("actual_e3m3", "cc2-ln-act")}${xax}</svg>`;
}

async function renderCommand() {
  const v = $("#view"); v.innerHTML = '<div class="placeholder">Loading Command Centre…</div>';
  let d, cons;
  try { d = await getJSON("/api/overview"); cons = await getJSON("/api/constraints"); }
  catch (e) { v.innerHTML = `<div class="placeholder">Command Centre unavailable. ${e}</div>`; return; }
  const kv = _kv(d.kpis), series = d.series || [], top = (cons && cons[0]) || null;
  const rate = Number(kv["Production (10^3 m^3/d)"]) || 0;
  const gap = Number(kv["Gap to nomination"]) || 0;
  const down = gap < 0;
  const led = [
    ["Deferment", fmt(kv["Deferment ($)"]), "$ lost production", "Lost-production value over the period, priced at the gas price.", "deferment"],
    ["Carbon", fmt(kv["Carbon intensity (kg/10^3 m^3)"]), "kg CO₂ / 10³m³", "CO₂-equivalent emitted per 10³m³ of gas produced (carbon intensity).", "emissions"],
    ["Red assets", fmt(kv["Red assets"]), "in alarm", "Assets currently red (in alarm) on the 3D asset-state model.", "navigator"],
    ["ESD trips", fmt(kv["ESD trips"]), "shutdowns", "Emergency-shutdown trips recorded. Status only; the twin never actuates safety systems.", "safety"],
    ["Work orders", fmt(kv["Open work orders"]), "open", "Open maintenance work orders across the field (EAM).", "maintenance"],
  ];
  v.innerHTML = `<div class="cc2">
    <div class="cc2-head"><h1>Command Centre</h1></div>

    <div class="cc2-hero">
      <div class="cc2-rate ccgo" data-goto="reservoir_wells">
        <div class="cc2-rate-val">${fmt(rate)}</div>
        <div class="cc2-rate-lab">field rate · 10³m³/d ${infoI("Current total field gas production rate across all producing wells (10³m³/d).")}</div>
      </div>
      <div class="cc2-gap ccgo ${down ? "down" : "up"}" data-goto="commercial">
        <div class="cc2-gap-lab">Difference from commitment ${infoI("Actual field production minus the contractual nomination. Shown red when the field is below commitment.")}</div>
        <div class="cc2-gap-val">${down ? "▼" : "▲"} ${fmt(Math.abs(gap))}</div>
      </div>
      ${top ? `<div class="cc2-cx ccgo" data-goto="compression">
        <div class="cc2-cx-lab">The bottleneck ${infoI("The single asset putting the most production at risk right now, from the ranked field constraints.")}</div>
        <div class="cc2-cx-asset">${top.asset_id}</div>
        <div class="cc2-cx-meta">${top.constraint_type}, ${fmt(top.production_at_risk_e3m3d)} ×10³m³/d at risk</div>
      </div>` : ""}
    </div>

    ${ccSpine(cons, top)}

    <div class="cc2-panel" style="margin-top:16px">
      <div class="cc2-h"><span>Production against commitment</span><span class="cc2-key"><i class="k-act"></i>actual <i class="k-nom"></i>nominated <i class="k-short"></i>shortfall</span></div>
      ${ccArea(series.slice(-62))}
    </div>

    <div class="cc2-ledger">${led.map(x => `<div class="cc2-led ccgo" data-goto="${x[4]}"><div class="cc2-led-v">${x[1]}</div><div class="cc2-led-n">${x[0]} ${infoI(x[3])}</div><div class="cc2-led-s">${x[2]}</div></div>`).join("")}</div>
  </div>`;
}

async function renderModule(key) { return renderModuleInto("#view", key); }
async function renderModuleInto(sel, key) {
  const v = $(sel); if (!v) return; v.innerHTML = '<div class="placeholder">Loading…</div>';
  let d; try { d = await getJSON("/api/module/" + key); } catch (e) { v.innerHTML = `<div class="placeholder">Module unavailable. ${e}</div>`; return; }
  // row-click opens context-sensitive detail wherever a row has an asset/well/work-order (items 29/45)
  const tbls = d.tables ? d.tables : (d.table ? [d.table] : []);
  const colKey = c => (c && c.key) ? c.key : c;
  const anyClick = tbls.some(t => (t.columns || []).some(c => ["asset_id", "well_id", "wo_id"].includes(colKey(c))));
  const hint = key === "workorders"
    ? "Click a work order for its detail; click an asset id to open the asset."
    : "Click a row to see its details.";
  v.innerHTML = `
    <h2>${d.title}</h2>
    ${d.period ? `<div class="muted periodline">Period: ${d.period}</div>` : ""}
    <div class="blurb">${prettyLabel(d.blurb || "")}</div>
    ${kpiRow(d.kpis)}
    ${chartsHTML(d.charts)}
    ${d.map ? '<div class="card"><div class="card-h">Field map (assets by state)</div><div id="mapbox" class="leafmap"></div></div>' : ""}
    ${tbls.length ? `${anyClick ? `<div class="muted rowhint">${hint}</div>` : ""}<div class="tablegrid">${tbls.map(t => `<div class="card"><div class="card-h">${t.title || "Detail"}</div>${tableHTML(t.columns, t.rows, { rowClick: true })}</div>`).join("")}</div>` : ""}`;
  if (d.map) renderMapInto("#mapbox");
}

// Item 9: left-nav "Gas flow chain" opens the five value-chain stops as horizontal tabs on top.
async function renderGasFlowChain(sub) {
  const stops = valueChainMods(), v = $("#view");
  if (!stops.length) { v.innerHTML = '<div class="placeholder">No value-chain stops.</div>'; return; }
  const active = (sub && stops.some(m => m.key === sub)) ? sub : ((GFC_CUR && stops.some(m => m.key === GFC_CUR)) ? GFC_CUR : stops[0].key);
  GFC_CUR = active;
  v.innerHTML = `<div class="gfc-tabs">${stops.map(m => `<button class="gfc-tab ${m.key === active ? "active" : ""}" data-gfc="${m.key}">${m.label}</button>`).join("")}</div><div id="gfc-body"><div class="placeholder">Loading…</div></div>`;
  v.querySelectorAll("[data-gfc]").forEach(b => b.onclick = () => renderGasFlowChain(b.getAttribute("data-gfc")));
  renderModuleInto("#gfc-body", active);
}

// Item 28: left-nav "Cross Landscape Reports" opens the cross-cutting reports as horizontal tabs on top.
async function renderCrossLandscape(sub) {
  const cross = crossMods(), v = $("#view");
  if (!cross.length) { v.innerHTML = '<div class="placeholder">No reports.</div>'; return; }
  const active = (sub && cross.some(m => m.key === sub)) ? sub : ((GFC_CROSS && cross.some(m => m.key === GFC_CROSS)) ? GFC_CROSS : cross[0].key);
  GFC_CROSS = active;
  v.innerHTML = `<div class="gfc-tabs">${cross.map(m => `<button class="gfc-tab ${m.key === active ? "active" : ""}" data-gfc="${m.key}">${m.label}</button>`).join("")}</div><div id="gfc-body"><div class="placeholder">Loading…</div></div>`;
  v.querySelectorAll("[data-gfc]").forEach(b => b.onclick = () => renderCrossLandscape(b.getAttribute("data-gfc")));
  if (active === "recommendations") renderRecommendations("#gfc-body");   // custom renderer (not a /api/module)
  else renderModuleInto("#gfc-body", active);
}
function roundsHTML() {
  return `<div class="card"><div class="card-h">Digital operator round (writes back to Lakebase)</div>
    <div style="display:flex;gap:8px;flex-wrap:wrap;align-items:center">
      <input id="r_asset" placeholder="asset_id" style="width:180px"/>
      <input id="r_obs" placeholder="observation" style="flex:1;min-width:220px"/>
      <select id="r_flag"><option>normal</option><option>watch</option><option>defect</option></select>
      <button class="btn" id="r_save">Log round</button><span id="r_msg" class="muted"></span>
    </div></div>`;
}
function wireRounds() {
  $("#r_save").onclick = async () => {
    const r = await postJSON("/api/round", { asset_id: $("#r_asset").value, observation: $("#r_obs").value, flag: $("#r_flag").value });
    $("#r_msg").textContent = r.ok ? "logged by " + r.observed_by : ("error: " + (r.error || "")); if (r.ok) { $("#r_obs").value = ""; }
  };
}

async function renderRecommendations(sel) {
  const v = $(sel || "#view"); v.innerHTML = '<div class="placeholder">Loading inbox…</div>';
  let recs; try { recs = await getJSON("/api/recommendations"); } catch (e) { v.innerHTML = `<div class="placeholder">Inbox unavailable. ${e}</div>`; return; }
  v.innerHTML = `<h2>Recommendation Inbox</h2>
    <div class="banner">Accept / reject / annotate is written back and audited to Lakebase.</div>
    <div id="recos">${recs.map(recoCard).join("") || '<div class="muted">No recommendations.</div>'}</div>`;
  // delegated so the swapped-in Undo button is handled too (item 27)
  $("#recos").addEventListener("click", async (e) => {
    const card = e.target.closest(".reco"); if (!card) return;
    const act = e.target.closest(".r-act"), undo = e.target.closest(".r-undo");
    const id = card.getAttribute("data-id"), msg = card.querySelector(".r-msg");
    if (act) {
      const status = act.getAttribute("data-act"), note = (card.querySelector(".r-note") || {}).value || "";
      msg.textContent = "saving…";
      const r = await postJSON("/api/recommendation/" + encodeURIComponent(id), { status, note });
      if (r.ok) { card.querySelector(".r-status").innerHTML = recoPill(status); card.querySelector(".r-actions").innerHTML = recoActions(status); msg.textContent = `${status} by ${r.decided_by}`; }
      else msg.textContent = "error: " + (r.error || "");
    } else if (undo) {
      msg.textContent = "undoing…";
      const r = await postJSON("/api/recommendation/" + encodeURIComponent(id), { status: "proposed", note: "" });
      if (r.ok) { card.querySelector(".r-status").innerHTML = ""; card.querySelector(".r-actions").innerHTML = recoActions(null); msg.textContent = ""; }
      else msg.textContent = "error: " + (r.error || "");
    }
  });
}
function recoPill(status) { return pill(status === "accepted" ? "green" : status === "rejected" ? "red" : "amber") + " " + status; }
function recoActions(status) {
  // item 57: once accepted / rejected / annotated the annotation box is gone — only Undo remains
  if (status && status !== "proposed")
    return `<button class="btn ghost r-undo">↶ Undo</button>`;
  return `<input class="r-note" placeholder="annotation (optional)" style="flex:1;min-width:220px"/>
    <button class="btn r-act" data-act="accepted">Accept</button>
    <button class="btn danger r-act" data-act="rejected">Reject</button>
    <button class="btn ghost r-act" data-act="annotated">Annotate</button>`;
}
function recoCard(r) {
  const decided = r.status && r.status !== "proposed";
  return `<div class="reco" data-id="${r.rec_id}"><h4>${r.asset_id} — <span class="muted">${r.constraint_type}</span> <span class="r-status">${decided ? recoPill(r.status) : ""}</span></h4>
    <div>${r.recommendation}</div>
    <div class="ev">Maturity: ${r.maturity_level || "—"} · Production at risk: ${fmt(r.production_at_risk_e3m3d)} ×10³m³/d</div>
    ${r.evidence ? `<div class="ev">Evidence: ${r.evidence}</div>` : ""}
    <div class="row r-actions">${recoActions(decided ? r.status : null)}</div>
    <span class="r-msg muted"></span></div>`;
}

async function renderCopilot() {
  const v = $("#view");
  v.innerHTML = `<h2>Ask me anything</h2>
    <div class="copilot-box"><input id="cop_q" placeholder="e.g. Which compressor is the current field constraint and why?"/><button class="btn" id="cop_go">Ask</button></div>
    <div class="grid" style="grid-template-columns:1fr 1fr;margin-bottom:14px">${["Which five wells lost the most gas to deferment, and the coded cause and dollar impact?", "Show every compressor whose vibration is rising and that is the current field constraint.", "Which streams are closest to breaching dewpoint or H2S/CO2 spec?", "Reconcile allocated vs metered volumes and flag the largest UAF by area."].map(q => `<div class="card" style="cursor:pointer" data-q="${q.replace(/"/g, "&quot;")}">${q}</div>`).join("")}</div>
    <div id="cop_a"></div>`;
  const ask = async (qtext) => {
    $("#cop_a").innerHTML = '<div class="answer muted">Thinking…</div>';
    const r = await postJSON("/api/copilot", { question: qtext });
    $("#cop_a").innerHTML = `<div class="answer">${(r.answer || "").replace(/</g, "&lt;")}</div>`;
  };
  $("#cop_go").onclick = () => ask($("#cop_q").value);
  $("#cop_q").addEventListener("keydown", e => { if (e.key === "Enter") ask($("#cop_q").value); });
  v.querySelectorAll("[data-q]").forEach(c => c.onclick = () => { $("#cop_q").value = c.getAttribute("data-q"); ask(c.getAttribute("data-q")); });
}

/* ---------- 3D navigator ---------- */
async function renderNavigator() {
  const v = $("#view"); v.innerHTML = '<div class="placeholder">Loading navigator…</div>';
  let scenes; try { scenes = await getJSON("/api/scenes"); } catch (e) { v.innerHTML = `<div class="placeholder">Navigator unavailable. ${e}</div>`; return; }
  v.innerHTML = `<h2>3D Asset Navigator</h2>
    <div class="nav3">
      <div class="n3-left"><div class="card-h">Value-chain stops</div>
        <div class="scene-head"><span>Stop</span><span>Assets</span></div>
        <div id="scenelist">${scenes.map(s => `<div class="scene-item" data-scene="${s.scene_id}"><span>${sceneLabel(s.scene_id)}</span><span>${s.red ? '<span class="dot red"></span>' : s.amber ? '<span class="dot amber"></span>' : '<span class="dot green"></span>'} ${s.assets}</span></div>`).join("")}</div></div>
      <div class="n3-stage"><div id="canvas"></div></div>
      <div class="n3-right" id="detail"><div class="muted">Select a stop, then click an asset in the scene.</div></div>
    </div>`;
  v.querySelectorAll("[data-scene]").forEach(b => b.onclick = () => { v.querySelectorAll(".scene-item").forEach(x => x.classList.remove("active")); b.classList.add("active"); loadScene(b.getAttribute("data-scene")); });
  if (scenes.length) {
    const want = PENDING_SCENE && scenes.some(s => s.scene_id === PENDING_SCENE) ? PENDING_SCENE : scenes[0].scene_id;
    PENDING_SCENE = null;
    const btn = Array.from(v.querySelectorAll(".scene-item")).find(x => x.getAttribute("data-scene") === want) || v.querySelector(".scene-item");
    btn.classList.add("active"); loadScene(want);
  }
}
function sceneLabel(s) { return s.replace("scene_", "").replace("_wh", " wellhead").replace("CPF001", "CPF Train 1").replace("CPF002", "CPF Train 2").replace("GTH001", "Gathering").replace("MET", "Custody metering"); }
async function loadScene(sceneId) {
  const assets = await getJSON("/api/scene/" + encodeURIComponent(sceneId));
  const canvas = $("#canvas"); const detail = $("#detail");
  detail.innerHTML = `<div class="card-h">${sceneLabel(sceneId)}</div><div class="kv"><span>Assets at this stop</span><span>${assets.length}</span></div><div class="kv"><span>Alarms</span><span>${assets.filter(a => a.pin_colour === "red").length}</span></div><div class="muted" style="margin-top:8px">Click an asset to inspect its live tags, health and advisory.</div>`;
  if (window.Twin3D && assets.length) {
    canvas.classList.add("three");
    try { window.Twin3D.load("#canvas", assets, loadAsset, {}); return; } catch (e) { canvas.classList.remove("three"); }
  }
  // 2D hotspot fallback
  canvas.classList.remove("three"); canvas.innerHTML = "";
  const n = assets.length, cols = Math.max(1, Math.ceil(Math.sqrt(n))), rows = Math.max(1, Math.ceil(n / cols));
  assets.forEach((a, i) => { const c = i % cols, r = Math.floor(i / cols); const h = el(`<div class="hotspot ${a.pin_colour || "green"}" style="left:${((c + 0.5) / cols) * 100}%;top:${((r + 0.5) / rows) * 100}%"><span class="label">${a.asset_id}</span></div>`); h.onclick = () => loadAsset(a.asset_id); canvas.appendChild(h); });
}
async function loadAsset(asset_id) {
  const d = await getJSON("/api/asset/" + encodeURIComponent(asset_id));
  const s = d.state || {};
  const tagRows = (d.tags || []).slice(0, 24);
  $("#detail").innerHTML = `<div class="card-h">${asset_id} ${pill(s.pin_colour)}</div>
    <div class="kv"><span>Type</span><span>${humanVal(s.equipment_type || "–")}</span></div>
    <div class="kv"><span>Area</span><span>${s.area_code || "–"}</span></div>
    <div class="kv"><span>Health</span><span>${fmt(s.health_score)}</span></div>
    <div class="kv"><span>Production at risk</span><span>${fmt(s.production_at_risk_e3m3d)} ×10³m³/d</span></div>
    <div class="kv"><span>Open work orders</span><span>${fmt(s.open_wos)}</span></div>
    ${d.recommendations && d.recommendations.length ? `<div class="banner" style="margin-top:10px">${d.recommendations[0].recommendation}<div class="ev">${d.recommendations[0].evidence || ""}</div></div>` : ""}
    <div class="card-h" style="margin-top:12px">Live tags</div>
    <table><tbody>${tagRows.map(t => `<tr><td>${humanVal(t.signal)}</td><td class="num">${fmt(t.latest_value)} ${prettyUnit(t.unit)}</td><td>${pill(t.latest_quality === "good" ? "green" : "amber")}</td></tr>`).join("")}</tbody></table>`;
}

/* ---------- Leaflet map ---------- */
function renderMapScreen() {
  const v = $("#view");
  v.innerHTML = `<h2>Field Map</h2>
    <div class="blurb">Every value-chain stop on the map, coloured by worst-case severity and sized by production-at-risk. Zoom in to resolve assets. Click a facility to open its 3D scene.</div>
    <div id="mapbox" class="leafmap"></div>`;
  renderMapInto("#mapbox");
}

// Item 5: Facility view — the whole field on the Australia map, a typed icon per facility; click a
// facility to open its detail drawer and jump into the 3D visualization.
const FAC_CAT = {
  wellhead: ["Well", "#2f7dc4", "W"],
  gathering_line: ["Gathering", "#2e9c6a", "G"], gathering_segment: ["Gathering", "#2e9c6a", "G"],
  compressor: ["Compression", "#e0533a", "C"],
  metering: ["Custody", "#6a5cf0", "M"], custody_meter: ["Custody", "#6a5cf0", "M"],
};
function facCat(t) { return FAC_CAT[t] || ["Processing", "#c98a1e", "P"]; }
async function renderFacilityView() {
  const v = $("#view");
  const cats = [["Well", "#2f7dc4", "W"], ["Gathering", "#2e9c6a", "G"], ["Processing", "#c98a1e", "P"], ["Compression", "#e0533a", "C"], ["Custody", "#6a5cf0", "M"]];
  v.innerHTML = `<h2>Facility view</h2>
    <div class="blurb">Every facility across the field on the Australia map, by type. Click a facility to open its detail and jump into the 3D visualization.</div>
    <div class="faclegend">${cats.map(c => `<span class="fl-item"><span class="fl-ic" style="background:${c[1]}">${c[2]}</span>${c[0]}</span>`).join("")}</div>
    <div id="facmap" class="leafmap"></div>`;
  const host = $("#facmap"); if (!host || typeof L === "undefined") { if (host) host.innerHTML = '<div class="muted" style="padding:16px">map unavailable</div>'; return; }
  if (_leaf) { try { _leaf.remove(); } catch (e) {} _leaf = null; }
  const map = L.map(host, { minZoom: 3, maxZoom: 16, zoomControl: true });
  map.fitBounds([[-43.5, 113.0], [-10.5, 154.0]]); _leaf = map;
  L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", { subdomains: "abc", maxZoom: 19, attribution: "© OpenStreetMap" }).addTo(map);
  let facs; try { facs = await getJSON("/api/assets_bbox"); } catch (e) { return; }
  // Item 15: a cluster aggregates facilities of different types, so show it as a magnifying-glass +
  // count ("drill in"), not a plain number bubble.
  const magSvg = '<svg viewBox="0 0 20 20" width="13" height="13" fill="none" stroke="currentColor" stroke-width="2.4" stroke-linecap="round"><circle cx="8.5" cy="8.5" r="5.5"></circle><line x1="12.6" y1="12.6" x2="18" y2="18"></line></svg>';
  const clusterIcon = c => L.divIcon({ className: "faccluster", html: `<span class="facmag">${magSvg}<b>${c.getChildCount()}</b></span>`, iconSize: [48, 30], iconAnchor: [24, 15] });
  const layer = (window.L.markerClusterGroup ? L.markerClusterGroup({ maxClusterRadius: 44, iconCreateFunction: clusterIcon }) : L.layerGroup());
  const SKIP = new Set(["esp", "gas_lift"]);   // artificial-lift kit is co-located with its wellhead; show one marker per well
  facs.forEach(f => {
    if (f.lat == null || f.lon == null || SKIP.has(f.equipment_type)) return;
    const [cat, col, letter] = facCat(f.equipment_type);
    const ic = L.divIcon({ className: "facdiv", html: `<span class="facpin" style="background:${col}">${letter}</span>`, iconSize: [22, 22], iconAnchor: [11, 11] });
    const mk = L.marker([f.lat, f.lon], { icon: ic });
    mk.bindTooltip(`${f.asset_id} · ${cat}`);
    mk.on("click", () => openAssetDrawer(f.asset_id, f.scene_id));
    layer.addLayer(mk);
  });
  map.addLayer(layer);
  setTimeout(() => map.invalidateSize(), 200);
}
async function renderMapInto(sel) {
  const host = $(sel); if (!host || typeof L === "undefined") { if (host) host.innerHTML = '<div class="muted" style="padding:16px">map unavailable</div>'; return; }
  if (_leaf) { try { _leaf.remove(); } catch (e) {} _leaf = null; }
  const map = L.map(host, { minZoom: 3, maxZoom: 16, zoomControl: true, preferCanvas: true });
  // frame the Australian continent exactly — "eye level", not the whole Asia-Pacific
  map.fitBounds([[-43.5, 113.0], [-10.5, 154.0]]);
  _leaf = map;
  L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", { subdomains: "abc", maxZoom: 19, attribution: "© OpenStreetMap" }).addTo(map);
  let data; try { data = await getJSON("/api/map"); } catch (e) { return; }
  const facs = data.facilities || []; const maxR = Math.max(1, ...facs.map(f => f.risk_dollars || 0));
  const col = { red: "#e0533a", amber: "#e0a53a", green: "#37b98a" };
  const layer = (window.L.markerClusterGroup ? L.markerClusterGroup() : L.layerGroup());
  facs.forEach(f => {
    // sized so every facility is easy to see: a visible base + growth with asset count + risk emphasis
    const rad = 9 + Math.sqrt(f.assets || 1) * 2.4 + Math.sqrt((f.risk_dollars || 0) / maxR) * 14;
    const mk = L.circleMarker([f.lat, f.lon], { radius: rad, color: col[f.pin_colour] || "#37b98a", fillColor: col[f.pin_colour] || "#37b98a", fillOpacity: .7, weight: 1 });
    mk.bindPopup(`<b>${f.area_code}</b><br>${f.assets} assets · ${f.red} red / ${f.amber} amber<br>PAR: ${fmt(f.risk_dollars)} ×10³m³/d`);
    mk.on("click", () => { setTimeout(() => { select("navigator"); }, 400); });
    layer.addLayer(mk);
  });
  map.addLayer(layer);
  setTimeout(() => map.invalidateSize(), 200);
}

/* ---------- Weather & Environment: per-location map (item 39) ---------- */
async function renderWeather() {
  const v = $("#view");
  v.innerHTML = `<h2>Weather & Environment</h2>
    <div class="blurb">Per-location weather across the field. Australia spans the tropical north to the cool south, so each site has its own temperature, wind and risk profile — there is no single field average. Click a site for its history.</div>
    <div id="wxtiles" class="kpis"></div>
    <div class="faclegend"><span class="fl-item">Marker colour = average temperature (cool blue → hot red)</span></div>
    <div id="wxmap" class="leafmap"></div>`;
  let sites; try { sites = await getJSON("/api/weather_map"); } catch (e) { $("#wxmap").innerHTML = '<div class="muted" style="padding:16px">weather unavailable</div>'; return; }
  if (!sites.length) return;
  const hottest = sites.reduce((a, b) => b.tmax > a.tmax ? b : a);
  const windiest = sites.reduce((a, b) => b.wind_max > a.wind_max ? b : a);
  const flood = sites.filter(s => s.fld_max > 0.3).length, ltn = sites.filter(s => s.ltn_max > 0.5).length;
  $("#wxtiles").innerHTML = [
    [`${fmt(hottest.tmax)}°C`, "Hottest site", `${hottest.area_code} peak`],
    [`${fmt(windiest.wind_max)} km/h`, "Windiest site", `${windiest.area_code} peak gust`],
    [flood, "Sites at flood risk", "flood-risk index above 0.3 at some point"],
    [ltn, "Sites at lightning risk", "lightning-risk index above 0.5 at some point"],
  ].map(t => `<div class="kpi"><div class="kpi-v">${t[0]}</div><div class="kpi-l">${t[1]} ${infoI(t[2])}</div></div>`).join("");
  const host = $("#wxmap"); if (!host || typeof L === "undefined") return;
  if (_leaf) { try { _leaf.remove(); } catch (e) {} _leaf = null; }
  const map = L.map(host, { minZoom: 3, maxZoom: 16, zoomControl: true });
  map.fitBounds([[-43.5, 113.0], [-10.5, 154.0]]); _leaf = map;
  L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", { subdomains: "abc", maxZoom: 19, attribution: "© OpenStreetMap" }).addTo(map);
  const tavgs = sites.map(s => s.tavg).filter(x => x != null), lo = Math.min(...tavgs), hi = Math.max(...tavgs);
  const col = t => { const f = (hi > lo) ? (t - lo) / (hi - lo) : 0.5; return `rgb(${Math.round(55 + f * 180)},90,${Math.round(215 - f * 170)})`; };
  sites.forEach(s => {
    if (s.lat == null) return;
    const mk = L.circleMarker([s.lat, s.lon], { radius: 9, color: "#fff", weight: 1, fillColor: col(s.tavg), fillOpacity: .85 });
    mk.bindTooltip(`${s.area_code}: ${fmt(s.tmin)}–${fmt(s.tmax)}°C · wind ${fmt(s.wind_max)} km/h`);
    mk.on("click", () => openWeatherSite(s.area_code));
    mk.addTo(map);
  });
  setTimeout(() => map.invalidateSize(), 200);
}
async function openWeatherSite(area) {
  let rows; try { rows = await getJSON("/api/weather_site/" + encodeURIComponent(area)); } catch (e) { return; }
  let m = $("#chartmodal");
  if (!m) { m = el(`<div id="chartmodal"><div class="cm-box"><div class="cm-head"><span class="cm-t"></span><button class="cm-x" title="Close">✕</button></div><div class="cm-body"></div></div></div>`); document.body.appendChild(m); m.addEventListener("click", e => { if (e.target === m || e.target.closest(".cm-x")) m.classList.remove("open"); }); }
  m.querySelector(".cm-t").textContent = area + " — weather history";
  m.querySelector(".cm-body").innerHTML = `<div class="wx-site-charts">
    <div class="card"><div class="card-h">Ambient temperature (°C)</div>${svgLine(rows, "obs_date", [{ key: "ambient_c", label: "°C" }])}</div>
    <div class="card"><div class="card-h">Wind (km/h)</div>${svgLine(rows, "obs_date", [{ key: "wind_kmh", label: "km/h" }])}</div>
    <div class="card"><div class="card-h">Lightning &amp; flood risk</div>${svgLine(rows, "obs_date", [{ key: "lightning_risk", label: "lightning" }, { key: "flood_risk", label: "flood" }])}</div>
  </div>`;
  m.classList.add("open");
}

/* ---------- Asset & Document Explorer: the tag contract, resolved live ---------- */
function tagCard(t) {
  const u = prettyUnit(t.unit);
  const lv = (t.latest_value === null || t.latest_value === undefined) ? "–" : `${fmt(t.latest_value)} ${u}`;
  const rng = (t.eng_range_min != null && t.eng_range_max != null) ? `${fmt(t.eng_range_min)} to ${fmt(t.eng_range_max)} ${u}` : "–";
  const qp = t.latest_quality ? pill(t.latest_quality === "good" ? "green" : "amber") : "";
  // item 53: the asset is a link to its 360 drawer
  const assetBits = [t.equipment_type ? humanVal(t.equipment_type) : "", t.criticality ? `criticality ${t.criticality}` : "", t.manufacturer || ""].filter(Boolean).join(" · ");
  const assetVal = t.asset_id
    ? `<a href="#" class="cell-link" data-asset-id="${escAttr(t.asset_id)}">${t.asset_id}</a>${assetBits ? ` · <span class="muted">${assetBits}</span>` : ""}`
    : "–";
  // item 51: each cross-system id on its own labelled row, not one run-on concatenation
  const xids = [["PI historian tag", t.historian_tag], ["EAM functional location", t.eam_fl], ["OSDU record", t.osdu_ref], ["P&ID drawing", t.doc_entity]].filter(x => x[1]);
  const stale = String(t.is_stale) === "true";
  return `<div class="reco">
    <h4>${t.tag_id} <span class="muted" style="font-weight:400">${humanVal(t.signal)} · ${u}${t.measured_or_calculated ? " · " + humanVal(t.measured_or_calculated) : ""}</span> ${qp}</h4>
    ${t.description ? `<div class="ev">${t.description}</div>` : ""}
    <div class="kv"><span>Tag key decodes to</span><span>area ${t.area_code || "–"} · system ${t.system_code || "–"} · class ${t.class_code || "–"}</span></div>
    <div class="kv"><span>Asset</span><span>${assetVal}</span></div>
    <div class="kv"><span>Functional location</span><span>${t.fl_name || t.fl_id || "–"}${t.fl_level != null ? ` (${t.fl_level})` : ""}</span></div>
    <div class="kv"><span>Latest sensor reading</span><span>${lv}${t.latest_ts ? ` <span class="muted">(taken ${fmtWhen(t.latest_ts)})</span>` : ""}</span></div>
    <div class="kv"><span>Normal operating range</span><span>${rng}</span></div>
    ${xids.map(x => `<div class="kv"><span>${x[0]} id</span><span><code>${x[1]}</code></span></div>`).join("")}
    ${stale ? `<div class="kv"><span>Link status</span><span>${pill("amber")} stale cross-system link</span></div>` : ""}
  </div>`;
}
async function renderAssetExplorer() {
  const v = $("#view"); v.innerHTML = '<div class="placeholder">Loading…</div>';
  let d; try { d = await getJSON("/api/module/asset_model"); } catch (e) { v.innerHTML = `<div class="placeholder">Explorer unavailable. ${e}</div>`; return; }
  const chips = ["CPF002-CMP", "vibration", "BAR003", "dewpoint", "meter"];
  // item 54: search on the left half, physical-location tree on the right half
  v.innerHTML = `
    <h2>${d.title}</h2>
    <div class="blurb">${d.blurb || ""}</div>
    ${kpiRow(d.kpis)}
    <div class="explorer-split">
      <div class="card explorer-col">
        <div class="card-h">Resolve a tag — the contract in action</div>
        <div class="blurb" style="margin-bottom:10px">Type a tag, asset, area or signal. Each hit resolves the contextualized tag to its asset, functional location, latest sensor reading and its ids in every other system (PI historian, EAM, OSDU, P&ID): the one join key that ties OT to IT. Every asset here links to its 360.</div>
        <div class="copilot-box"><input id="tl_q" placeholder="e.g. CPF002-CMP · vibration · BAR003-WB · dewpoint"/><button class="btn" id="tl_go">Resolve</button></div>
        <div style="display:flex;gap:7px;flex-wrap:wrap;margin:2px 0 12px">${chips.map(x => `<span data-chip="${x}" style="cursor:pointer;font-size:11.5px;background:var(--panel2);border:1px solid var(--line);border-radius:12px;padding:3px 11px;color:var(--muted)">${x}</span>`).join("")}</div>
        <div id="tl_res"></div>
      </div>
      <div class="card explorer-col">
        <div class="card-h">Browse by physical location</div>
        <div class="blurb" style="margin-bottom:10px">The functional-location hierarchy: field → area → asset → tag. Click an asset to open its 360; click a tag to resolve it on the left.</div>
        <div id="loc_tree"><div class="muted">Loading locations…</div></div>
      </div>
    </div>`;
  const run = async (term) => {
    if (!term || !term.trim()) { $("#tl_res").innerHTML = '<div class="muted">Type a tag, asset, area or signal to resolve the contract — or pick one from the location tree on the right.</div>'; return; }
    $("#tl_res").innerHTML = '<div class="muted">Resolving…</div>';
    let r; try { r = await getJSON("/api/tag-lookup?term=" + encodeURIComponent(term.trim())); } catch (e) { $("#tl_res").innerHTML = '<div class="muted">lookup error</div>'; return; }
    const rows = r.rows || [];
    $("#tl_res").innerHTML = rows.length
      ? `<div class="muted" style="margin-bottom:8px">${rows.length} tag${rows.length > 1 ? "s" : ""} resolved</div>${rows.map(tagCard).join("")}`
      : '<div class="muted">No tags match. Try CPF002-CMP, vibration, or a well like BAR003.</div>';
  };
  $("#tl_go").onclick = () => run($("#tl_q").value);
  $("#tl_q").addEventListener("keydown", e => { if (e.key === "Enter") run($("#tl_q").value); });
  v.querySelectorAll("[data-chip]").forEach(c => c.onclick = () => { $("#tl_q").value = c.getAttribute("data-chip"); run(c.getAttribute("data-chip")); });
  // item 53: asset links inside results open the 360 drawer
  $("#tl_res").addEventListener("click", e => { const al = e.target.closest("[data-asset-id]"); if (al) { e.preventDefault(); openAssetDrawer(al.getAttribute("data-asset-id")); } });
  // item 52: no pre-loaded results — start empty with a prompt
  run("");
  wireLocationTree(run);
}

// item 54: lazy physical-location tree (area -> asset -> tag). A tag click resolves on the left.
async function wireLocationTree(run) {
  const host = $("#loc_tree");
  let d; try { d = await getJSON("/api/tree"); } catch (e) { host.innerHTML = '<div class="muted">location tree unavailable</div>'; return; }
  const areas = d.areas || [];
  host.innerHTML = areas.map(a =>
    `<div class="tnode" data-area="${escAttr(a.area_code)}">
      <div class="trow area"><span class="tcaret">▸</span><span class="tlabel">${a.name || a.area_code}</span><span class="tmeta">${a.n_assets} assets · ${a.n_tags} tags</span></div>
      <div class="tkids" style="display:none"></div>
    </div>`).join("") || '<div class="muted">No locations.</div>';
  host.addEventListener("click", async (e) => {
    const open360 = e.target.closest(".topen[data-asset-id]");
    if (open360) { e.preventDefault(); e.stopPropagation(); openAssetDrawer(open360.getAttribute("data-asset-id")); return; }
    const trow = e.target.closest(".trow"); if (!trow) return;
    if (trow.classList.contains("tag")) { const tg = trow.getAttribute("data-tag"); $("#tl_q").value = tg; run(tg); const lc = document.querySelector(".explorer-col"); if (lc) lc.scrollIntoView({ behavior: "smooth", block: "nearest" }); return; }
    const node = trow.parentElement, kids = node.querySelector(".tkids"), caret = trow.querySelector(".tcaret");
    if (node.classList.contains("open")) { node.classList.remove("open"); kids.style.display = "none"; if (caret) caret.textContent = "▸"; return; }
    node.classList.add("open"); if (caret) caret.textContent = "▾"; kids.style.display = "block";
    if (kids.getAttribute("data-loaded")) return;
    kids.setAttribute("data-loaded", "1"); kids.innerHTML = '<div class="muted" style="padding:4px 0 4px 24px">Loading…</div>';
    if (trow.classList.contains("area")) {
      let r; try { r = await getJSON("/api/tree/" + encodeURIComponent(node.getAttribute("data-area"))); } catch (err) { kids.innerHTML = '<div class="muted" style="padding-left:24px">load error</div>'; return; }
      kids.innerHTML = (r.assets || []).map(a =>
        `<div class="tnode" data-asset="${escAttr(a.asset_id)}">
          <div class="trow asset"><span class="tcaret">▸</span><span class="dot ${a.pin_colour || "green"}"></span><span class="tlabel">${a.asset_id}</span><span class="tmeta">${humanVal(a.equipment_type || "")} · crit ${a.criticality || "–"} · ${a.n_tags} tags</span><a href="#" class="topen" data-asset-id="${escAttr(a.asset_id)}">360 ↗</a></div>
          <div class="tkids" style="display:none"></div>
        </div>`).join("") || '<div class="muted" style="padding-left:24px">No assets.</div>';
    } else if (trow.classList.contains("asset")) {
      let r; try { r = await getJSON("/api/tree-asset/" + encodeURIComponent(node.getAttribute("data-asset"))); } catch (err) { kids.innerHTML = '<div class="muted" style="padding-left:24px">load error</div>'; return; }
      kids.innerHTML = (r.tags || []).map(t =>
        `<div class="trow tag" data-tag="${escAttr(t.tag_id)}"><span class="tcaret-sp"></span><span class="tlabel">${t.tag_id}</span><span class="tmeta">${humanVal(t.signal)}${t.latest_value != null ? ` · ${fmt(t.latest_value)} ${prettyUnit(t.unit)}` : ""}</span></div>`).join("") || '<div class="muted" style="padding-left:24px">No tags.</div>';
    }
  });
}

/* ---------- nav + routing ---------- */
function buildNav() {
  const groups = {};
  NAV.forEach(n => { (groups[n.group] = groups[n.group] || []).push(n); });
  // Item 49: a flat menu — no group headings. Value-chain stops collapse into one "Gas flow chain"
  // entry (item 9) and the cross-cutting reports into one "Cross Landscape Reports" entry (item 28).
  $("#nav").innerHTML = Object.entries(groups).map(([g, items]) => {
    if (g === "Value chain") return `<button class="nav-item" data-key="gas_flow_chain"><span>Gas flow chain</span></button>`;
    if (g === "Cross-cutting") return `<button class="nav-item" data-key="cross_landscape"><span>Cross Landscape Reports</span></button>`;
    return items.map(n => `<button class="nav-item" data-key="${n.key}"><span>${n.label}</span></button>`).join("");
  }).join("");
  $("#nav").querySelectorAll("[data-key]").forEach(b => b.onclick = () => select(b.getAttribute("data-key")));
}
let GENIE_URL;
async function genieUrl() { if (GENIE_URL === undefined) { try { GENIE_URL = (await getJSON("/api/genie-link")).url; } catch (e) { GENIE_URL = null; } } return GENIE_URL; }
// Utility links live top-right of the header: Ask Genie always, "Facility view" on value-chain stops.
async function setHeadLinks(key) {
  const host = $("#headlinks"); if (!host) return;
  const mod = (NAV || []).find(n => n.key === key);
  const isVC = key === "gas_flow_chain" || key === "facility_view" || key === "navigator" || (mod && mod.group === "Value chain");
  const links = [];
  const gu = await genieUrl();
  if (gu) links.push(`<a class="hl" href="${gu}" target="_blank" rel="noopener">Ask Genie</a>`);
  if (isVC) links.push(`<a class="hl" href="#" data-facility="1">Facility view</a>`);
  host.innerHTML = links.join("");
  const s = host.querySelector("[data-facility]"); if (s) s.onclick = (e) => { e.preventDefault(); select("facility_view"); };
}
function select(key) {
  CUR = key;
  $("#nav").querySelectorAll(".nav-item").forEach(b => b.classList.toggle("active", b.getAttribute("data-key") === key));
  setHeadLinks(key);
  route(key);
}
function route(key) {
  if (key === "command_centre") return renderCommand();
  if (key === "map") return renderMapScreen();
  if (key === "gas_flow_chain") return renderGasFlowChain();
  if (key === "cross_landscape") return renderCrossLandscape();
  if (key === "facility_view") return renderFacilityView();
  if (key === "weather") return renderWeather();
  if (key === "navigator") return renderNavigator();
  if (key === "copilot") return renderCopilot();
  if (key === "recommendations") return renderRecommendations();
  if (key === "asset_model") return renderAssetExplorer();
  return renderModule(key);
}
async function loadFreshness() { const f = $("#freshness"); if (!f) return; try { const h = await getJSON("/health"); f.textContent = h.ok ? `${fmt(h.assets)} assets live` : "warming up…"; } catch (e) { f.textContent = ""; } }

async function boot() {
  document.addEventListener("click", e => {
    const th = e.target.closest("th.sortable"); if (th) { sortByTh(th); return; }
    const ac = e.target.closest("td[data-asset-id]"); if (ac) { openAssetDrawer(ac.getAttribute("data-asset-id")); return; }  // asset-id cell wins (item 29)
    const kl = e.target.closest(".kpi[data-list]"); if (kl) { openListDrawer(kl.getAttribute("data-list")); return; }       // tile opens a full list
    const g = e.target.closest("[data-goto]"); if (g) { goTo(g.getAttribute("data-goto")); return; }
    const row = e.target.closest("tr.rowclick"); if (row) { const ent = row.getAttribute("data-entity"), id = row.getAttribute("data-id"); if (ent === "workorder") openWoDrawer(id); else openAssetDrawer(id); return; }  // context-sensitive
    const k = e.target.closest(".kpi[data-link]"); if (k) { goTo(k.getAttribute("data-link")); return; }
  });
  loadFreshness();
  try { NAV = await getJSON("/api/modules"); } catch (e) { $("#view").innerHTML = '<div class="placeholder">Could not load modules.</div>'; return; }
  buildNav();
  select("command_centre");
}
boot();
