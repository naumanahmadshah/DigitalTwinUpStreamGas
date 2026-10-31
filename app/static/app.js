/* Upstream Gas Digital Twin v2: SPA. Vanilla JS; inline-SVG charts; Leaflet map; Three.js 3D. */
const $ = (s) => document.querySelector(s);
const el = (h) => { const d = document.createElement("div"); d.innerHTML = h; return d.firstElementChild; };
// Full-screen loader: any data fetch shows the animated loader over a blurred, darkened app.
// Shown only if a load takes >150 ms (no flash on instant cached loads), and kept >=400 ms once shown.
let _busy = 0, _ldShow = null, _ldAt = 0, _ldHide = null;
function loaderStart() {
  _busy++; clearTimeout(_ldHide);
  if (_busy === 1 && !_ldShow) _ldShow = setTimeout(() => { _ldShow = null; if (_busy > 0) { _ldAt = Date.now(); $("#loader").classList.add("on"); } }, 150);
}
function loaderEnd() {
  _busy = Math.max(0, _busy - 1); if (_busy) return;
  if (_ldShow) { clearTimeout(_ldShow); _ldShow = null; }
  const el = $("#loader"); if (!el || !el.classList.contains("on")) return;
  _ldHide = setTimeout(() => { if (!_busy) el.classList.remove("on"); }, Math.max(0, 400 - (Date.now() - _ldAt)));
}
async function getJSON(u) {
  loaderStart();
  try { const r = await fetch(u); if (!r.ok) throw new Error(u + " " + r.status); return await r.json(); }
  finally { loaderEnd(); }
}
async function postJSON(u, b) { loaderStart(); try { const r = await fetch(u, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(b) }); return await r.json(); } finally { loaderEnd(); } }
const SERIES = ["#4b8fe6", "#7b6cf0", "#37b98a", "#e0a53a", "#e0533a"];
const DEV = { red: "#e0533a", amber: "#e0a53a", green: "#37b98a" };
// Bar-chart palette (deep teal -> mint -> cream). Status bars use the darker shades for worse values.
const BARPAL = { deep: "#1e5068", teal: "#50949e", mint: "#96d3c4", pale: "#d6f2dc", cream: "#fffed8" };
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
// Full human timestamp: "6 Oct 2026, 14:30" (date + time when present), no ambiguous "06/10". (item 51)
function fmtWhen(s) { const m = String(s).match(/^(\d{4})-(\d{2})-(\d{2})(?:[T ](\d{2}):(\d{2}))?/); if (!m) return String(s); const d = (+m[3]) + " " + _MONS[+m[2] - 1] + " " + m[1]; return m[4] != null ? d + ", " + m[4] + ":" + m[5] : d; }
// Humanise a coded/underscored display value (gas_lift -> Gas lift). Leaves ids/hyphenated codes alone.
function humanVal(v) { return (typeof v === "string" && v.includes("_")) ? v.replace(/_/g, " ").replace(/^\w/, c => c.toUpperCase()) : v; }
// Pretty-print a coded engineering unit for display, never show "e3m3" or raw tokens on the UI.
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
    const fo = opts && opts.focus, hit = fo && (fo.ids.has(s.key) || fo.ids.has(s.label)), anyHit = fo && series.some(q => fo.ids.has(q.key) || fo.ids.has(q.label));
    return `<path d="${d}" class="line${hit ? " fhi" : (anyHit ? " fdim" : "")}" style="stroke:${s.color}"${s.dash ? ' stroke-dasharray="5 4"' : ''}/>`;
  }).join("");
  const legend = series.length > 1 ? `<div class="chart-legend">${series.map(s => `<span class="lg"><span class="lg-s" style="background:${s.color}"></span>${s.label || s.key}</span>`).join("")}</div>` : "";
  return `${legend}<svg viewBox="0 0 ${W} ${H}" class="chart">${grid}${zero}${xax}${paths}</svg>`;
}
// Bar charts are drawn at their card's real pixel width (not a stretched viewBox), so text is always the
// same size and few-bar charts don't turn into slabs. svgBar returns a placeholder; drawBars() renders it
// once it is in the page (a MutationObserver catches every insertion) and again on window resize.
const BAR_REG = new Map(); let _barSeq = 0;
function svgBar(rows, xk, yk, opts) {
  opts = opts || {}; const pts = rows.map(r => ({ x: String(r[xk]), y: Number(r[yk]) })).filter(p => p.y === p.y);
  if (!pts.length) return '<div class="muted">no data</div>';
  const id = "b" + (++_barSeq); BAR_REG.set(id, { pts, opts });
  const dev = opts.color === "lowbad";
  const key = dev ? `<div class="barkey"><i style="background:${BARPAL.deep}"></i>well below average <i style="background:${BARPAL.teal}"></i>below average <i style="background:${BARPAL.mint}"></i>at or above average</div>` : "";
  return key + `<div class="barchart" data-bid="${id}"></div>`;
}
function drawBar(host) {
  const spec = BAR_REG.get(host.getAttribute("data-bid")); if (!spec) return;
  const avail = Math.floor(host.clientWidth); if (avail < 50) return;
  if (host._w === avail) return; host._w = avail;
  const { pts, opts } = spec, n = pts.length, pL = 46, pR = 10, pT = 12, plotH = 200;   // same plot height in every chart
  const MIN_SLOT = 22, MAX_BAR = 56;
  const fitSlot = (avail - pL - pR) / n, wide = fitSlot < MIN_SLOT;
  const slot = wide ? MIN_SLOT : fitSlot, plotW = slot * n, W = pL + pR + plotW;
  const longest = Math.min(18, Math.max(...pts.map(p => p.x.length)));
  const rotate = slot < 70 || longest * 6.3 > slot - 6;   // tilt the labels whenever any would not fit flat
  const short = s => { const lim = rotate ? 18 : Math.max(6, Math.floor(slot / 6.2)); return s.length > lim ? s.slice(0, lim - 1) + "…" : s; };
  const pB = rotate ? 18 + longest * 4.6 : 30, H = Math.round(pT + plotH + pB), baseY = pT + plotH;
  const dataMax = Math.max(...pts.map(p => p.y), 0) || 1, step = niceNum(dataMax / 3, true), top = Math.ceil(dataMax / step) * step;
  const yOf = v => pT + (1 - v / top) * plotH;
  const grid = []; for (let t = 0; t <= top + step * 1e-6; t += step) { const y = yOf(t).toFixed(1); grid.push(`<line x1="${pL}" y1="${y}" x2="${W - pR}" y2="${y}" class="gl"/><text x="${pL - 6}" y="${+y + 3}" class="ax" text-anchor="end">${fmtK(t)}</text>`); }
  const mean = pts.reduce((a, p) => a + p.y, 0) / n, dev = opts.color === "lowbad";
  const fill = y => y < mean * 0.75 ? BARPAL.deep : y < mean * 0.93 ? BARPAL.teal : BARPAL.mint;
  const bwid = Math.max(1, Math.min(MAX_BAR, slot - 4));
  const fo = opts.focus, FC = { red: "#dd4b1a", amber: "#e0a53a" };
  const bars = pts.map((p, i) => { const h = Math.max(1, (p.y / top) * plotH), x = pL + i * slot + (slot - bwid) / 2;
    const f0 = dev ? fill(p.y) : "", hit = fo && fo.ids.has(p.x);
    const st = hit ? ` style="fill:${FC[fo.level]};stroke:#7a2410;stroke-width:1"` : (f0 ? ` style="fill:${f0}"` : "");
    return `<rect x="${x.toFixed(1)}" y="${(baseY - h).toFixed(1)}" width="${bwid.toFixed(1)}" height="${h.toFixed(1)}" class="bar${hit ? " fbar" : ""}" data-f0="${f0}"${st}><title>${escAttr(p.x)}: ${fmt(p.y)}</title></rect>`; }).join("");
  const ly = baseY + (rotate ? 14 : 17);
  const labels = pts.map((p, i) => { const cx = (pL + i * slot + slot / 2).toFixed(1);
    return rotate ? `<text x="${cx}" y="${ly}" class="axb" text-anchor="end" transform="rotate(-45 ${cx} ${ly})"><title>${escAttr(p.x)}</title>${short(p.x)}</text>`
                  : `<text x="${cx}" y="${ly}" class="axb" text-anchor="middle"><title>${escAttr(p.x)}</title>${short(p.x)}</text>`; }).join("");
  const svg = `<svg width="${W}" height="${H}" viewBox="0 0 ${W} ${H}" class="barsvg">${grid.join("")}<line x1="${pL}" y1="${baseY}" x2="${W - pR}" y2="${baseY}" class="gl"/>${bars}${labels}</svg>`;
  host.innerHTML = wide ? `<div class="barscroll">${svg}</div>` : svg;
  const fb = host.querySelector(".bar.fbar"), sc = host.querySelector(".barscroll");   // a long chart scrolls to the first highlighted bar
  if (fb && sc) sc.scrollLeft = Math.max(0, +fb.getAttribute("x") - 60);
}
function drawBars(root) { (root || document).querySelectorAll(".barchart").forEach(drawBar); }
new MutationObserver(() => drawBars()).observe(document.documentElement, { childList: true, subtree: true });
window.addEventListener("resize", () => drawBars());

/* ---------- shared render helpers ---------- */
const escAttr = (s) => String(s).replace(/"/g, "&quot;").replace(/</g, "&lt;");
const escH = (s) => String(s == null ? "" : s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
// Every work-order id in the app is a link to its detail (handled by the global click handler).
const woLink = id => `<a href="#" class="cell-link wolink" data-wo-id="${escAttr(id)}">${id}</a>`;
const linkWOs = html => String(html).replace(/\bWO-\d{4,}\b/g, m => woLink(m));
// Recommendation evidence says "open P1 WO": point it at the asset's actual open work order(s).
function evidenceWithWOs(text, wos) {
  const open = (wos || []).filter(w => !w.status || ["open", "in_progress"].includes(w.status));
  const p1 = open.find(w => w.priority === "P1") || open[0];
  let t = linkWOs(text || "");
  if (p1) t = t.replace(/open P1 WO/, `open ${p1.priority || "P1"} work order ${woLink(p1.wo_id)}`);
  else t = t.replace(/open P1 WO \+ /, "");   // the stock evidence claims an open WO; drop it when the asset has none
  return t;
}
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
  // a row whose status is amber or red (watch, at risk, high, not passed...) is tinted that colour
  const ROWST = { red: "red", watch: "amber", amber: "amber", not_passed: "red", high: "red", due: "amber", at_risk: "red", investigate: "red", monitor: "amber", critical: "red", warning: "amber", major: "amber" };
  const rowSt = r => { let st = ""; PILL.filter(k => keys.includes(k) && k !== "priority" && k !== "severity").forEach(k => { const c = ROWST[String(r[k])]; if (c === "red" || (c === "amber" && !st)) st = c; }); return st; };
  const body = rows.map(r => {
    const pid = primaryCol ? escAttr(r[primaryCol] ?? "") : "";
    const st = rowSt(r), sc = st ? ` row-${st}` : "";
    const tr = clickable && pid ? `<tr class="rowclick${sc}" data-entity="${entity}" data-id="${pid}">` : (sc ? `<tr class="${sc.trim()}">` : "<tr>");
    return tr + cols.map(c => {
      const k = key(c), v = r[k];
      const assetCell = (entity === "workorder" && k === "asset_id" && v != null && v !== "") ? ` data-asset-id="${escAttr(v)}"` : "";
      const body = (k === "wo_id" && v != null && v !== "") ? woLink(v) : cell(k, v);
      return `<td class="${isNum(k) ? "num" : ""}${assetCell ? " cell-link" : ""}"${assetCell} data-v="${escAttr(sv(k, v))}">${body}</td>`;
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
function chartsHTML(charts, fx) {
  if (!charts || !charts.length) return "";
  return `<div class="grid">${charts.map((c, i) => {
    const series = (c.opts && c.opts.series) ? c.opts.series
      : [{ key: c.y, label: c.title }].concat((c.title.includes("nomination") || (c.rows[0] && c.rows[0].nominated_e3m3 !== undefined)) ? [{ key: "nominated_e3m3", label: "nominated" }] : []);
    const fo = fx ? { ...(c.opts || {}), focus: fx } : c.opts;
    const body = c.type === "bar" ? svgBar(c.rows, c.x, c.y, fo) : svgLine(c.rows, c.x, series, fo);
    return `<div class="card"><div class="card-h">${prettyLabel(c.title)}</div>${body}</div>`;
  }).join("")}</div>`;
}

/* ---------- asset detail drawer + telemetry sparklines + chart zoom (shared across value-chain stops) ---------- */
function valueChainMods() { return (NAV || []).filter(n => n.group === "Value chain"); }
function crossMods() { return (NAV || []).filter(n => n.group === "Cross-cutting"); }
function commMods() { return (NAV || []).filter(n => n.group === "Commercial"); }
let GFC_CUR = null, GFC_CROSS = null, GFC_COMM = null, PENDING_SCENE = null;
// navigate to any module; value-chain stops open inside the Gas-flow-chain tab bar, and cross-cutting
// reports open inside its "Analytics" sub-tabs (item 28).
function goTo(key) {
  if (valueChainMods().some(m => m.key === key)) { GFC_CUR = key; select("gas_flow_chain"); }
  else if (crossMods().some(m => m.key === key)) { GFC_CROSS = key; select("cross_landscape"); }
  else if (commMods().some(m => m.key === key)) { GFC_COMM = key; select("commercial_landscape"); }
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
// One shared modal for enlarged charts and site weather history. Its body scrolls on its own; the
// page behind never scrolls while it is open (wheel/touch outside the body is swallowed).
function chartModal() {
  let m = $("#chartmodal"); if (m) return m;
  m = el(`<div id="chartmodal"><div class="cm-box"><div class="cm-head"><span class="cm-t"></span><button class="cm-x" title="Close">✕</button></div><div class="cm-body"></div></div></div>`);
  document.body.appendChild(m);
  m.addEventListener("click", e => { if (e.target === m || e.target.closest(".cm-x")) m.classList.remove("open"); });
  const block = e => { const b = e.target.closest(".cm-body"); if (!b || b.scrollHeight <= b.clientHeight) e.preventDefault(); };
  m.addEventListener("wheel", block, { passive: false }); m.addEventListener("touchmove", block, { passive: false });
  document.addEventListener("keydown", e => { if (e.key === "Escape") m.classList.remove("open"); });
  return m;
}
function openChartModal(title, points, unit) {
  const m = chartModal();
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
  renderAssetDetail(dr, id, { sceneId, onClose: closeDrawer });
}

// The rich asset detail (state, recommendation, telemetry sparklines, work orders, notifications,
// failures, procurement, lab, deferment, live tags). Shared by the asset drawer and the 3D view's
// side panel so both always show the same thing. opts.in3d hides the "View site" button.
async function renderAssetDetail(root, id, opts) {
  opts = opts || {};
  const close = opts.onClose || (() => {});
  root.innerHTML = `<div class="dr-head"><span class="dr-title">Loading ${id}…</span><button class="dr-x" title="Close">✕</button></div>`;
  root.querySelector(".dr-x").onclick = close;
  let d, s, ctx = {};
  try { [d, s, ctx] = await Promise.all([getJSON("/api/asset/" + encodeURIComponent(id)), getJSON("/api/asset_series/" + encodeURIComponent(id)), getJSON("/api/asset_context/" + encodeURIComponent(id))]); }
  catch (e) { const t = root.querySelector(".dr-title"); if (t) t.textContent = id + " (unavailable)"; return; }
  if (root.getAttribute("data-asset") && root.getAttribute("data-asset") !== id) return;  // a newer click won
  const st = d.state || {}, name = st.asset_id || id, reco = (d.recommendations || [])[0], sers = s.series || [];
  const kv = (k, val) => `<div class="kv"><span>${k}</span><span>${val}</span></div>`;
  const sparks = sers.length
    ? `<div class="dr-sec">Telemetry over time <span class="muted" style="font-weight:400">· click to enlarge</span></div><div class="sparkgrid">${sers.map((se, i) => `<div class="sparkcard" data-si="${i}"><div class="sparkt">${humanVal(se.signal)}${se.unit ? ` <span class="muted">${prettyUnit(se.unit)}</span>` : ""}</div>${sparkline(se.points, SERIES[i % SERIES.length])}</div>`).join("")}</div>`
    : "";
  const tagRows = (d.tags || []).slice(0, 20);
  const defers = (d.deferment || []).slice(0, 8);
  const wos = (d.workorders || []).slice(0, 12);
  root.innerHTML = `<div class="dr-head"><span class="dr-title">${name} ${pill(st.pin_colour)}</span><span class="dr-acts">${opts.in3d ? "" : `<button class="btn dr-site" id="dr3d">View site</button>`}<button class="dr-x" title="Close">✕</button></span></div>
    <div class="dr-scroll">
      ${kv("Type", humanVal(st.equipment_type || "–"))}
      ${kv("Area", `<a href="#" class="drlink" data-area="${escAttr(st.area_code || "")}">${st.area_code || "–"}</a>`)}
      ${st.criticality ? kv("Criticality", st.criticality) : ""}
      ${st.manufacturer ? kv("Manufacturer", st.manufacturer) : ""}
      ${kv("Health score", fmt(st.health_score))}
      ${kv("Production at risk", fmt(st.production_at_risk_e3m3d) + " ×10³m³/d")}
      ${kv("Open work orders", fmt(st.open_wos))}
      ${reco ? `<div class="banner" style="margin:10px 0">${reco.recommendation}${reco.evidence ? `<div class="ev">${evidenceWithWOs(reco.evidence, d.workorders)}</div>` : ""}</div>` : ""}
      ${sparks}
      ${wos.length ? `<div class="dr-sec">Work orders for this asset</div><table class="dtbl"><tbody>${wos.map(w => `<tr class="worow" data-wo-id="${escAttr(w.wo_id)}" data-back="${escAttr(name)}"><td>${woLink(w.wo_id)}</td><td>${humanVal(w.wo_type)} ${pill(w.priority)}</td><td>${humanVal(w.status)}</td></tr>`).join("")}</tbody></table>` : ""}
      ${(ctx.notifications || []).length ? `<div class="dr-sec">Notifications</div><table class="dtbl"><tbody>${ctx.notifications.map(n => `<tr><td>${n.notif_id}</td><td>${humanVal(n.notif_type)} ${pill(n.priority)}</td><td>${humanVal(n.status)}</td></tr>`).join("")}</tbody></table>` : ""}
      ${(ctx.failures || []).length ? `<div class="dr-sec">Failure history (ISO 14224)</div><table class="dtbl"><tbody>${ctx.failures.map(f => `<tr><td>${fmtMD(f.failure_date)}</td><td>${f.iso14224_code} · ${humanVal(f.failure_mode)}</td><td class="num">${fmt(f.downtime_hours)} h</td></tr>`).join("")}</tbody></table>` : ""}
      ${(ctx.spares || []).length ? `<div class="dr-sec">Procurement: what to order</div><table class="dtbl"><tbody>${ctx.spares.map(sp => `<tr><td>${humanVal(sp.procurement_type)}</td><td>${humanVal(sp.part_name)}</td><td class="num">lead ${fmt(sp.lead_time_days)}d · ${fmt(sp.unit_cost)} · ${pill(sp.exposure)}</td></tr>`).join("")}</tbody></table>` : ""}
      ${(ctx.lab || []).length ? `<div class="dr-sec">Lab assays</div><table class="dtbl"><tbody>${ctx.lab.map(l => `<tr><td>${fmtMD(l.sample_date)}</td><td>${humanVal(l.sample_point)}</td><td class="num">C₁ ${fmt(l.c1_molpct)} · CO₂ ${fmt(l.co2_molpct)} · H₂S ${fmt(l.h2s_ppm)}</td></tr>`).join("")}</tbody></table>` : ""}
      ${defers.length ? `<div class="dr-sec">Deferment timeline</div><table class="dtbl"><tbody>${defers.map(x => `<tr><td>${fmtMD(x.gas_day)}</td><td>${humanVal(x.cause_desc)}</td><td class="num">${fmt(x.deferred_dollars)}</td></tr>`).join("")}</tbody></table>` : ""}
      ${tagRows.length ? `<div class="dr-sec">Live tags</div><table class="dtbl"><tbody>${tagRows.map(t => `<tr><td>${humanVal(t.signal)}</td><td class="num">${fmt(t.latest_value)} ${prettyUnit(t.unit)}</td><td>${pill(t.latest_quality === "good" ? "green" : "amber")}</td></tr>`).join("")}</tbody></table>` : ""}
    </div>`;
  root.querySelector(".dr-x").onclick = close;
  root.querySelectorAll(".sparkcard").forEach(c => c.onclick = () => { const se = sers[+c.getAttribute("data-si")]; openChartModal(humanVal(se.signal), se.points, se.unit); });
  // click the asset's area to jump to its work orders in context (item 26)
  const al = root.querySelector(".drlink[data-area]"); if (al) al.onclick = (e) => { e.preventDefault(); close(); goTo("maintenance"); };
  const b3d = root.querySelector("#dr3d"); if (b3d) b3d.onclick = () => { PENDING_SCENE = opts.sceneId || st.scene_id || null; close(); select("navigator"); };
}

// Work-order detail (item 29): the WO itself, with a link to its asset and a notifications sublink.
// showAsset (optional) decides where "back" and the asset link go: opened from the 3D site view's side
// panel, they close this drawer and show the asset in that panel, so only one asset detail is ever open.
async function openWoDrawer(woId, backAsset, showAsset) {
  const toAsset = showAsset || openAssetDrawer;
  if (!woId) return;
  ensureDrawer();
  const wrap = $("#drawerwrap"), dr = $("#assetdrawer");
  wrap.classList.add("open"); dr.classList.remove("wide");
  dr.innerHTML = `<div class="dr-head"><span class="dr-title">Loading ${woId}…</span><button class="dr-x" title="Close">✕</button></div>`;
  dr.querySelector(".dr-x").onclick = closeDrawer;
  let d; try { d = await getJSON("/api/workorder/" + encodeURIComponent(woId)); } catch (e) { const t = dr.querySelector(".dr-title"); if (t) t.textContent = woId + ", unavailable"; return; }
  const w = d.wo || {}, notifs = d.notifications || [];
  const kv = (k, val) => `<div class="kv"><span>${k}</span><span>${val}</span></div>`;
  dr.innerHTML = `<div class="dr-head"><span class="dr-title">${backAsset ? `<a href="#" class="drlink dr-back" title="Back to ${escAttr(backAsset)}">‹</a> ` : ""}${w.wo_id || woId} ${pill(w.priority)}</span><button class="dr-x" title="Close">✕</button></div>
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
  const al = dr.querySelector(".drlink[data-asset-id]"); if (al) al.onclick = (e) => { e.preventDefault(); toAsset(al.getAttribute("data-asset-id")); };
  const bk = dr.querySelector(".dr-back"); if (bk) bk.onclick = (e) => { e.preventDefault(); toAsset(backAsset); };
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

// What made a Command Centre element red or amber, carried to the screen it opens so that screen can colour
// the same rows, bars, lines and tiles (set on click, used once by the next module render).
let FOCUS = null, CC_FOCUS = {};
const CTYPE_TAB = { "compression": "compression", "liquid loading": "reservoir_wells", "custody UAF": "custody" };
const basinOfName = n => ((n || "").match(/([A-Za-z-]+) Basin\)\s*$/) || [])[1];
function focusFrom(list, level, lead) {
  const live = list.filter(c => Number(c.production_at_risk_e3m3d) > 0); if (live.length) list = live;   // only what actually puts gas at risk
  const ids = new Set(), sites = new Map(), basins = new Set(), types = new Set();
  let risk = 0;
  list.forEach(c => { [c.asset_id, c.well_id].forEach(x => x && ids.add(x)); types.add(c.constraint_type);
    risk += Number(c.production_at_risk_e3m3d) || 0;
    const nm = (c.site_name || c.area_code || "").replace(/ \([^)]*\)\s*$/, ""); sites.set(nm, (sites.get(nm) || 0) + 1);
    const b = basinOfName(c.site_name); if (b) { basins.add(b); ids.add(b === "Cooper-Eromanga" ? "Cooper" : b); ids.add(b); } });
  const t = [...types][0] || "", n = list.length;
  const what = t === "compression" ? `${n} compressor${n > 1 ? "s" : ""} in poor health` : t === "liquid loading" ? `${n} well${n > 1 ? "s" : ""} loading up with liquid`
    : t === "custody UAF" ? `metered and allocated gas diverging at ${n} meter${n > 1 ? "s" : ""}` : `${n} constraint${n > 1 ? "s" : ""}`;
  const where = [...sites.keys()].filter(Boolean).slice(0, 3).join(", ");
  const KPI = { "compression": ["at risk", "health"], "liquid loading": ["liquid-loading"], "custody UAF": ["uaf"] }[t] || [];
  return { key: CTYPE_TAB[t] || "processing", level, ids, kpis: KPI,
           text: `${lead}: ${what}${where ? ` (${where})` : ""}, ${fmt(risk)} ×10³m³/d at risk. Highlighted below.` };
}

// A comic-style exclamation mark (tilted bar and dot over a black offset shadow) that hops to draw the eye.
function alertMark(tone, label) {
  const shape = '<polygon points="10,3 38,0 27,64 16,65"/><ellipse cx="19" cy="83" rx="11" ry="8.5"/>';
  return `<span class="cc2-alert ${tone}" role="img" aria-label="${label}"><svg viewBox="0 0 50 96">
    <g transform="rotate(12 25 48)"><g class="sh" transform="translate(4 2.5)">${shape}</g><g class="fg">${shape}</g></g></svg></span>`;
}

// Sit a stop's exclamation mark just off the top-right corner of its picture (the drawn picture, not the
// image box, since each picture has its own proportions).
function placeAlert(img) {
  const m = img.parentElement.querySelector(".cc2-alert"); if (!m || !img.naturalWidth) return;
  const w = Math.min(img.clientWidth, img.clientHeight * img.naturalWidth / img.naturalHeight);
  m.style.left = `calc(50% + ${(w / 2 - 6).toFixed(0)}px)`;
}

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
    // the binding stop (red) puts the most gas at risk; the runner-up (amber) is the one to watch, the stop most likely to bind next
    // whichever stops carry the most production at risk get an exclamation badge: red for the bottleneck,
    // amber for the one to watch (any of the five, from the live constraint ranking); the figure is in the tooltip
    const mark = isBind ? alertMark("red", "bottleneck") : (isNext ? alertMark("amber", "watch") : "");
    const tip = isBind ? `The field's binding constraint: this stop puts the most production at risk right now (${fmt(risk[s] || 0)} ×10³m³/d).`
      : isNext ? `Second-highest production at risk (${fmt(risk[s] || 0)} ×10³m³/d): the stop most likely to become the constraint next.` : "No significant production at risk at this stop.";
    let goto = stopGoto[s], fattr = "";
    if (isBind || isNext) {   // open the tab where the cause lives, with the cause highlighted there
      const mine = (cons || []).filter(c => (stopOf[c.constraint_type] || "Processing") === s);
      CC_FOCUS[s] = focusFrom(mine, isBind ? "red" : "amber", isBind ? `Why ${s} is red, the field's bottleneck` : `Why ${s} is amber, the stop to watch`);
      goto = CC_FOCUS[s].key; fattr = ` data-focus="${s}"`;
    }
    return `<div class="cc2-stop ccgo ${state}" data-goto="${goto}"${fattr} title="${tip}">
        <span class="cc2-imgwrap"><img class="cc2-stop-img" src="img/stop_${s.toLowerCase()}.png?v=4" alt="${s}" onload="placeAlert(this)">${mark}</span>
        <span class="cc2-stop-name">${s}</span>
      </div>${i < stops.length - 1 ? '<span class="cc2-pipe"></span>' : ""}`;
  }).join("")}</div>`;
}

function ccArea(rows, W, H) {
  rows = rows || []; const n = rows.length; if (!n) return '<div class="muted">no data</div>';
  const num = v => Number(v);
  let mn = Infinity, mx = -Infinity;
  rows.forEach(r => [num(r.actual_e3m3), num(r.nominated_e3m3)].forEach(x => { if (x === x) { mn = Math.min(mn, x); mx = Math.max(mx, x); } }));
  if (!(mn <= mx)) return '<div class="muted">no data</div>';
  const step = niceNum(((mx - mn) || 1) / 3, true); mn = Math.floor(mn / step) * step; mx = Math.ceil(mx / step) * step;
  W = Math.max(W || 1000, 320); H = Math.max(H || 236, 160);
  const pL = 52, pR = 16, pT = 10, pB = 30;
  const sx = i => pL + (n <= 1 ? 0 : i * (W - pL - pR) / (n - 1));
  const sy = v => pT + (mx - v) * (H - pT - pB) / (mx - mn);
  const ticks = []; for (let t = mn; t <= mx + step * 1e-6; t += step) ticks.push(t);
  const grid = ticks.map(t => `<line x1="${pL}" y1="${sy(t).toFixed(1)}" x2="${W - pR}" y2="${sy(t).toFixed(1)}" class="gl"/><text x="${pL - 6}" y="${(sy(t) + 3).toFixed(1)}" class="ax" text-anchor="end">${fmtK(t)}</text>`).join("");
  const xs = rows.map(r => r.gas_day); const xi = [0, Math.floor((n - 1) / 3), Math.floor(2 * (n - 1) / 3), n - 1];
  const ly = H - pB + 22;  // horizontal labels in the bottom margin; edge labels anchored in so nothing clips
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
  return `<svg viewBox="0 0 ${W} ${H}" width="${W}" height="${H}" class="cc2-chartsvg">${grid}${band}${line("nominated_e3m3", "cc2-ln-nom", true)}${line("actual_e3m3", "cc2-ln-act")}${xax}</svg>`;
}

async function renderCommand() {
  const v = $("#view"); v.innerHTML = '<div class="placeholder">Loading Command Centre…</div>';
  let d, cons;
  try { d = await getJSON("/api/overview"); cons = await getJSON("/api/constraints"); }
  catch (e) { if (CUR === "command_centre") v.innerHTML = `<div class="placeholder">Command Centre unavailable. ${e}</div>`; return; }
  if (CUR !== "command_centre") return;   // the user moved on while this was loading; don't overwrite their screen
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
    <div class="cc2-hero">
      <div class="cc2-rate ccgo" data-goto="reservoir_wells">
        <div class="cc2-rate-val">${fmt(rate)}</div>
        <div class="cc2-rate-lab">field rate · 10³m³/d ${infoI("Current total field gas production rate across all producing wells (10³m³/d).")}</div>
      </div>
      <div class="cc2-gap ccgo ${down ? "down" : "up"}" data-goto="commercial"${down ? ' data-focus="gap"' : ""}>
        <div class="cc2-gap-lab">Difference from commitment ${infoI("Actual field production minus the contractual nomination. Shown red when the field is below commitment.")}</div>
        <div class="cc2-gap-val">${down ? "▼" : "▲"} ${fmt(Math.abs(gap))}</div>
      </div>
      ${top ? `<div class="cc2-cx ccgo" data-goto="${CTYPE_TAB[top.constraint_type] || "compression"}" data-focus="bottleneck">
        <div class="cc2-cx-lab">The bottleneck ${infoI("The single asset putting the most production at risk right now, from the ranked field constraints.")}</div>
        <div class="cc2-cx-asset">${top.asset_id}</div>
        <div class="cc2-cx-meta">${top.constraint_type}, ${fmt(top.production_at_risk_e3m3d)} ×10³m³/d at risk</div>
      </div>` : "<div></div>"}
      <a class="cc2-maplink" href="#" title="Open the Field Map" aria-label="Open the Field Map"><img src="img/fieldmap.png?v=2" alt=""><span>Field Map</span></a>
      <a class="cc2-maplink cc2-impactlink" href="#" title="Trace what each stop feeds, down to the customers" aria-label="Impact analysis">${IMPACT_ICON}<span>Impact analysis</span></a>
    </div>

    ${ccSpine(cons, top)}

    <div class="cc2-panel cc2-chartpanel">
      <div class="cc2-h"><span>Production against commitment</span><span class="cc2-key"><i class="k-act"></i>actual <i class="k-nom"></i>nominated <i class="k-short"></i>shortfall</span></div>
      <div class="cc2-chartbox"></div>
    </div>

    <div class="cc2-ledger">${led.map(x => `<div class="cc2-led ccgo" data-goto="${x[4]}"><div class="cc2-led-v">${x[1]}</div><div class="cc2-led-n">${x[0]} ${infoI(x[3])}</div><div class="cc2-led-s">${x[2]}</div></div>`).join("")}</div>
  </div>`;
  if (top) CC_FOCUS.bottleneck = focusFrom([top], "red", "The bottleneck");
  if (down) {   // the shortfall days behind the red difference from commitment
    const short = series.slice(-62).filter(r => Number(r.actual_e3m3) < Number(r.nominated_e3m3));
    CC_FOCUS.gap = { key: "commercial", level: "red", kpis: ["delivery vs nomination"], ids: new Set(short.map(r => String(r.gas_day).slice(0, 10))),
      text: `Why the field is red against commitment: ${fmt(Math.abs(gap))} ×10³m³/d below nomination on the latest gas day, and below nomination on ${short.length} of the last 62 gas days. Those days are highlighted below.` };
  }
  v.querySelector(".cc2-maplink").onclick = e => { e.preventDefault(); e.stopPropagation(); select("map"); };
  v.querySelector(".cc2-impactlink").onclick = e => { e.preventDefault(); e.stopPropagation(); select("impact"); };
  const rows = series.slice(-62), box = v.querySelector(".cc2-chartbox");
  const draw = () => { if (!box.isConnected) { window.removeEventListener("resize", draw); return; }
    box.innerHTML = ccArea(rows, box.clientWidth, box.clientHeight); };
  draw(); window.addEventListener("resize", draw);
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
  const fx = FOCUS && FOCUS.key === key ? FOCUS : null; FOCUS = null;   // a cause carried from the Command Centre
  v.innerHTML = `
    ${fx ? `<div class="focus-banner ${fx.level}"><span class="fb-dot"></span><span class="fb-t">${fx.text}</span><button class="fb-x" title="Clear the highlight" aria-label="Clear the highlight">✕</button></div>` : ""}
    ${d.period ? `<div class="muted periodline">Period: ${d.period}</div>` : ""}
    ${kpiRow(d.kpis)}
    ${chartsHTML(d.charts, fx)}
    ${d.map ? '<div class="card"><div class="card-h">Field map (assets by state)</div><div id="mapbox" class="leafmap"></div></div>' : ""}
    ${tbls.length ? `${anyClick ? `<div class="muted rowhint">${hint}</div>` : ""}<div class="tablegrid">${tbls.map(t => `<div class="card"><div class="card-h">${t.title || "Detail"}</div>${tableHTML(t.columns, t.rows, { rowClick: true })}</div>`).join("")}</div>` : ""}`;
  if (d.map) renderMapInto("#mapbox");
  if (fx) applyFocus(v, fx);
}

// Colour what caused the red / amber: matching table rows (moved to the top of their table), KPI tiles and,
// through chartsHTML, bars and line series. The banner's ✕ clears it all.
function applyFocus(v, fx) {
  const cls = "focus-" + fx.level;
  v.querySelectorAll(".kpi").forEach(k => { const l = (k.querySelector(".kpi-l") || k).textContent.toLowerCase(); if ((fx.kpis || []).some(x => l.includes(x))) k.classList.add(cls); });
  v.querySelectorAll("table.dtbl tbody").forEach(tb => {
    const hit = Array.from(tb.rows).filter(tr => Array.from(tr.cells).some(td => fx.ids.has(td.getAttribute("data-v")) || fx.ids.has((td.getAttribute("data-v") || "").slice(0, 10))));
    hit.reverse().forEach(tr => { tr.classList.add(cls); tb.prepend(tr); });
  });
  const x = v.querySelector(".fb-x");
  if (x) x.onclick = () => { v.querySelectorAll(".focus-red,.focus-amber").forEach(e => e.classList.remove("focus-red", "focus-amber")); v.querySelectorAll(".bar.fbar").forEach(b => { b.classList.remove("fbar"); b.style.fill = b.getAttribute("data-f0") || ""; }); v.querySelectorAll(".line.fdim,.line.fhi").forEach(l => l.classList.remove("fdim", "fhi")); x.parentElement.remove(); };
}

// Item 9: left-nav "Gas flow chain" opens the five value-chain stops as horizontal tabs on top.
async function renderGasFlowChain(sub) {
  const stops = valueChainMods(), v = $("#view");
  if (!stops.length) { v.innerHTML = '<div class="placeholder">No value-chain stops.</div>'; return; }
  const active = (sub && stops.some(m => m.key === sub)) ? sub : ((GFC_CUR && stops.some(m => m.key === GFC_CUR)) ? GFC_CUR : stops[0].key);
  GFC_CUR = active; rememberTab("gas_flow_chain", active);
  v.innerHTML = `<div class="gfc-tabs">${stops.map(m => `<button class="gfc-tab ${m.key === active ? "active" : ""}" data-gfc="${m.key}">${m.label}</button>`).join("")}</div><div id="gfc-body"><div class="placeholder">Loading…</div></div>`;
  raiseActiveRow(v.querySelector(".gfc-tabs"));
  v.querySelectorAll("[data-gfc]").forEach(b => b.onclick = () => renderGasFlowChain(b.getAttribute("data-gfc")));
  renderModuleInto("#gfc-body", active);
}

// Folder tabs: the selected tab joins the raised page below it, so its row must be the bottom row.
// Like a classic multi-row tab dialog, the selected tab's row is moved to sit directly above the page.
function raiseActiveRow(bar) {
  if (!bar) return;
  const tabs = [...bar.querySelectorAll(".gfc-tab")], act = bar.querySelector(".gfc-tab.active"); if (!act) return;
  // measure the natural rows (in menu order), then freeze each row so moving one never reflows the others
  // rows are bottom-aligned and the selected tab is taller, so group tabs by their bottom edge
  const rows = []; tabs.forEach(t => { const y = t.offsetTop + t.offsetHeight, row = rows.find(r => Math.abs(r.y - y) < 4); row ? row.items.push(t) : rows.push({ y, items: [t] }); });
  if (rows.length < 2) return;
  const mine = rows.indexOf(rows.find(r => r.items.includes(act)));
  const order = rows.filter((_, i) => i !== mine).concat([rows[mine]]);
  bar.classList.add("rows");
  bar.innerHTML = "";
  order.forEach(r => { const d = document.createElement("div"); d.className = "gfc-row"; r.items.forEach(t => d.appendChild(t)); bar.appendChild(d); });
}
// Item 28: left-nav "Analytics" opens the cross-cutting reports as horizontal tabs on top; "Commercial &
// Finance" does the same for the money reports (deferment, cost, prices, margin, capital plan).
async function renderCrossLandscape(sub) { return renderLandscape(sub, "cross"); }
async function renderCommercialLandscape(sub) { return renderLandscape(sub, "comm"); }
async function renderLandscape(sub, which) {
  const comm = which === "comm", cross = comm ? commMods() : crossMods(), v = $("#view");
  if (!cross.length) { v.innerHTML = '<div class="placeholder">No reports.</div>'; return; }
  const cur = comm ? GFC_COMM : GFC_CROSS;
  const active = (sub && cross.some(m => m.key === sub)) ? sub : ((cur && cross.some(m => m.key === cur)) ? cur : cross[0].key);
  if (comm) GFC_COMM = active; else GFC_CROSS = active;
  rememberTab(comm ? "commercial" : "analytics", active);
  // Commercial & Finance carries the button that brings the briefing back, whatever was chosen in it
  const bb = comm ? `<button class="brief-btn" type="button" title="The top 3 commercial actions, summarised by Genie"><span class="bb-spark">✦</span> Top 3 actions</button>` : "";
  v.innerHTML = `<div class="gfc-wrap"><div class="gfc-tabs">${cross.map(m => `<button class="gfc-tab ${m.key === active ? "active" : ""}" data-gfc="${m.key}">${m.label}</button>`).join("")}</div>${bb}</div><div id="gfc-body"><div class="placeholder">Loading…</div></div>`;
  raiseActiveRow(v.querySelector(".gfc-tabs"));
  v.querySelectorAll("[data-gfc]").forEach(b => b.onclick = () => renderLandscape(b.getAttribute("data-gfc"), which));
  if (comm) {
    v.querySelector(".brief-btn").onclick = () => openBriefing(true);
    if (BRIEF_ON_ENTRY) { BRIEF_ON_ENTRY = false; maybeShowBriefing(); }   // opened from the menu: show the briefing unless snoozed
  }
  if (active === "recommendations") renderRecommendations("#gfc-body");   // custom renderers (not a /api/module)
  else if (active === "weather") renderWeather("#gfc-body");
  else renderModuleInto("#gfc-body", active);
}
/* ---------- user preferences, kept per user in Lakebase (gas_twin.settings) ---------- */
// The briefing choice (don't show again / show again in a week), the last report opened in each menu and the
// folded groups in the 3D site list. Loaded once at start-up; each change is written back (debounced).
let PREFS = {}, PREFS_READY = null;
const _prefT = {};
function loadPrefs() {
  PREFS_READY = fetch("/api/settings").then(r => r.ok ? r.json() : null).then(d => {
    PREFS = (d && d.settings) || {};
    const lt = PREFS.last_tabs || {};   // reopen each menu on the report the user last had open
    if (!GFC_CUR && lt.gas_flow_chain) GFC_CUR = lt.gas_flow_chain;
    if (!GFC_CROSS && lt.analytics) GFC_CROSS = lt.analytics;
    if (!GFC_COMM && lt.commercial) GFC_COMM = lt.commercial;
    (PREFS.site_list_folded || []).forEach(k => SCENE_CLOSED.add(k));
  }).catch(() => {});
  return PREFS_READY;
}
function savePref(key, value) {
  if (value == null) delete PREFS[key]; else PREFS[key] = value;
  clearTimeout(_prefT[key]);
  _prefT[key] = setTimeout(() => fetch("/api/settings/" + key, { method: "PUT", headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ value: value == null ? null : value }) }).catch(() => {}), 400);
}
function rememberTab(menu, key) {
  const lt = { ...(PREFS.last_tabs || {}) }; if (lt[menu] === key) return;
  lt[menu] = key; savePref("last_tabs", lt);
}

/* ---------- Commercial & Finance briefing: the top 3 commercial actions ---------- */
// Fetched quietly at start-up (no loader), so it opens instantly. The actions come from the
// gld_commercial_actions mart; the summary is Genie's, prepared in the background by the server.
let BRIEF = null, BRIEF_ON_ENTRY = false, _briefPoll = 0;
function loadBrief() {
  return fetch("/api/briefing/commercial").then(r => r.ok ? r.json() : null).then(d => {
    if (d && d.actions && d.actions.length) BRIEF = d;
    // Genie is still writing the summary: look again shortly (and refresh an open briefing in place)
    if (BRIEF && BRIEF.pending && _briefPoll++ < 12) setTimeout(() => loadBrief().then(fillBriefSummary), 20000);
    return BRIEF;
  }).catch(() => BRIEF);
}
function briefSuppressed() {
  const p = PREFS.commercial_briefing; if (!p) return false;
  return p.mode === "never" || (p.mode === "snooze" && Date.parse(p.until) > Date.now());
}
async function maybeShowBriefing() { if (PREFS_READY) await PREFS_READY; if (!briefSuppressed()) openBriefing(false); }
function briefSummaryHTML(b) {
  const gen = b.summary_source === "genie";
  return `<img class="bs-ic" src="img/genie.png" alt="" aria-hidden="true"><div><div class="bs-who">${gen ? "Genie's summary" : "In short"}</div>
    <p>${escH(b.summary || "")}</p>${b.pending ? '<div class="bs-note">Genie is reviewing these actions; its summary will appear here.</div>' : ""}</div>`;
}
function fillBriefSummary() { const s = $(".brief .brief-sum"); if (s && BRIEF) s.innerHTML = briefSummaryHTML(BRIEF); }
async function openBriefing(manual) {
  if ($(".brief-veil")) return;
  const b = BRIEF || await loadBrief();
  if (!b || !b.actions || !b.actions.length) { if (manual) toast("The commercial briefing is not available yet."); return; }
  const was = PREFS.commercial_briefing || null, mode = was && was.mode;
  const veil = el(`<div class="brief-veil" role="presentation"><div class="brief" role="dialog" aria-modal="true" aria-labelledby="brief-t">
      <button class="brief-x" type="button" aria-label="Close">✕</button>
      <div class="brief-kick"><span class="bb-spark">✦</span> AI briefing</div>
      <h2 id="brief-t">Top 3 commercial actions</h2>
      <div class="brief-subt">Ranked by the money at stake across the Commercial &amp; Finance reports${b.data_as_of ? `, from data to ${fmtMD(b.data_as_of)} ${String(b.data_as_of).slice(0, 4)}` : ""}</div>
      <div class="brief-sum">${briefSummaryHTML(b)}</div>
      <ol class="brief-list">${b.actions.map((a, i) => `<li class="brief-item">
          <span class="bi-n">${i + 1}</span>
          <div class="bi-body"><div class="bi-h">${escH(a.action)}</div><div class="bi-d">${escH(a.detail)}</div>
            <div class="bi-f"><span class="bi-v" title="${escAttr(a.value_basis || "")}">${escH(a.value_label || "")}</span>
              <button class="bi-go" type="button" data-i="${i}">Open ${escH(a.module_label)} <span aria-hidden="true">→</span></button></div></div></li>`).join("")}</ol>
      <div class="brief-foot">
        <label class="bf-opt"><input type="checkbox" data-opt="never"${mode === "never" ? " checked" : ""}> Don't show this again</label>
        <label class="bf-opt"><input type="checkbox" data-opt="snooze"${mode === "snooze" ? " checked" : ""}> Show it again in a week</label>
        <button class="brief-ok" type="button">Got it</button>
      </div></div></div>`);
  document.body.appendChild(veil);
  requestAnimationFrame(() => veil.classList.add("on"));
  const boxes = [...veil.querySelectorAll("[data-opt]")];
  boxes.forEach(x => x.onchange = () => { if (x.checked) boxes.forEach(y => { if (y !== x) y.checked = false; }); });   // one or the other
  const close = () => {
    const pick = (boxes.find(x => x.checked) || {}).getAttribute?.("data-opt") || null;
    let val = null;
    if (pick === "never") val = mode === "never" ? was : { mode: "never", set_at: new Date().toISOString() };
    if (pick === "snooze") val = mode === "snooze" ? was : { mode: "snooze", until: new Date(Date.now() + 7 * 864e5).toISOString() };
    if (JSON.stringify(val) !== JSON.stringify(was)) {
      savePref("commercial_briefing", val);
      if (val) { toast(val.mode === "never" ? "Saved. Bring the briefing back any time with ✦ Top 3 actions." : "Saved. It will show again in a week, or any time with ✦ Top 3 actions.");
        const bt = $(".brief-btn"); if (bt) { bt.classList.remove("pulse"); void bt.offsetWidth; bt.classList.add("pulse"); } }
    }
    document.removeEventListener("keydown", onKey);
    veil.classList.remove("on"); setTimeout(() => veil.remove(), 180);
  };
  const onKey = e => { if (e.key === "Escape") close(); };
  document.addEventListener("keydown", onKey);
  veil.querySelector(".brief-x").onclick = close;
  veil.querySelector(".brief-ok").onclick = close;
  veil.onclick = e => { if (e.target === veil) close(); };
  veil.querySelectorAll(".bi-go").forEach(g => g.onclick = () => {   // open the report behind the action, its evidence highlighted
    const a = b.actions[+g.getAttribute("data-i")];
    close();
    FOCUS = { key: a.module_key, level: "amber", ids: new Set(String(a.focus_ids || "").split("|").filter(Boolean)),
              kpis: String(a.focus_kpis || "").split("|").filter(Boolean), text: `Briefing action ${a.rank || ""}: ${a.action}. ${a.detail} Highlighted below.` };
    goTo(a.module_key);
  });
  veil.querySelector(".brief-ok").focus();
}
function toast(msg) {
  const old = $(".toast"); if (old) old.remove();
  const tt = el(`<div class="toast" role="status">${escH(msg)}</div>`); document.body.appendChild(tt);
  requestAnimationFrame(() => tt.classList.add("on")); setTimeout(() => { tt.classList.remove("on"); setTimeout(() => tt.remove(), 300); }, 4200);
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
  // item 57: once accepted / rejected / annotated the annotation box is gone, only Undo remains
  if (status && status !== "proposed")
    return `<button class="btn ghost r-undo">↶ Undo</button>`;
  return `<input class="r-note" placeholder="annotation (optional)" style="flex:1;min-width:220px"/>
    <button class="btn r-act" data-act="accepted">Accept</button>
    <button class="btn danger r-act" data-act="rejected">Reject</button>
    <button class="btn ghost r-act" data-act="annotated">Annotate</button>`;
}
function recoCard(r) {
  const decided = r.status && r.status !== "proposed";
  return `<div class="reco" data-id="${r.rec_id}"><h4>${r.asset_id}: <span class="muted">${r.constraint_type}</span> <span class="r-status">${decided ? recoPill(r.status) : ""}</span></h4>
    <div>${r.recommendation}</div>
    <div class="ev">Maturity: ${r.maturity_level || "-"} · Production at risk: ${fmt(r.production_at_risk_e3m3d)} ×10³m³/d</div>
    ${r.evidence ? `<div class="ev">Evidence: ${evidenceWithWOs(r.evidence, r.open_wos)}</div>` : ""}
    ${(r.open_wos || []).length ? `<div class="ev">Open work orders on this asset: ${r.open_wos.map(w => `${woLink(w.wo_id)} ${pill(w.priority)}`).join(" ")}</div>` : ""}
    <div class="row r-actions">${recoActions(decided ? r.status : null)}</div>
    <span class="r-msg muted"></span></div>`;
}

/* ---------- 3D navigator ---------- */
async function renderNavigator() {
  const v = $("#view"); v.innerHTML = '<div class="placeholder">Loading navigator…</div>';
  let scenes; try { scenes = await getJSON("/api/scenes"); } catch (e) { v.innerHTML = `<div class="placeholder">Navigator unavailable. ${e}</div>`; return; }
  v.innerHTML = `<div class="nav3">
      <div class="n3-left">
        <div id="scenelist">${sceneListHTML(scenes)}</div></div>
      <div class="n3-stage"><div id="canvas"></div><button class="n3-close" title="Back to the normal layout" aria-label="Back to the normal layout">✕</button></div>
      <div class="n3-right" id="detail"></div>
    </div>`;
  // the site view goes full screen: the menu slides away and the site list takes its place; ✕ brings the layout back
  setImmersive(true);
  v.querySelector(".n3-close").onclick = () => setImmersive(false);
  v.querySelectorAll(".scene-sec .scene-grp").forEach(h => h.onclick = () => {   // collapse / expand a group
    const sec = h.parentElement, k = sec.getAttribute("data-grp"), shut = !sec.classList.contains("closed");
    sec.classList.toggle("closed", shut); h.setAttribute("aria-expanded", String(!shut)); if (shut) SCENE_CLOSED.add(k); else SCENE_CLOSED.delete(k);
    savePref("site_list_folded", [...SCENE_CLOSED]);
  });
  v.querySelectorAll("[data-scene]").forEach(b => b.onclick = () => { v.querySelectorAll(".scene-item").forEach(x => x.classList.remove("active")); b.classList.add("active"); loadScene(b.getAttribute("data-scene")); });
  if (scenes.length) {
    // PENDING_SCENE may be a scene id or a site's area code (from the Field Map); match either
    const hit = PENDING_SCENE && scenes.find(s => s.scene_id === PENDING_SCENE || s.scene_id.replace(/^scene_/, "").replace(/_wh$/, "") === PENDING_SCENE);
    const want = hit ? hit.scene_id : scenes[0].scene_id;
    PENDING_SCENE = null;
    const btn = Array.from(v.querySelectorAll(".scene-item")).find(x => x.getAttribute("data-scene") === want) || v.querySelector(".scene-item");
    btn.classList.add("active"); loadScene(want);
    // open the site's group if it is folded and bring the site into view in the list
    const sec = btn.closest(".scene-sec");
    if (sec && sec.classList.contains("closed")) { sec.classList.remove("closed"); sec.querySelector(".scene-grp").setAttribute("aria-expanded", "true"); SCENE_CLOSED.delete(sec.getAttribute("data-grp")); savePref("site_list_folded", [...SCENE_CLOSED]); }
    btn.scrollIntoView({ block: "center" });
  }
}
// The site list, grouped along the gas path (well pads, offshore platforms, gathering hubs, offshore manifolds,
// gas plants, custody), each site named with its place underneath, e.g. "Well pad BAR029" over "Surat Basin".
const SCENE_CLOSED = new Set();   // site-list groups the user has collapsed (kept per user in Lakebase)
function sceneListHTML(scenes) {
  const groups = [];
  scenes.forEach(s => { const k = s.kind || "Sites"; let g = groups.find(x => x.k === k); if (!g) groups.push(g = { k, items: [] }); g.items.push(s); });
  return groups.map(g => `<div class="scene-sec${SCENE_CLOSED.has(g.k) ? " closed" : ""}" data-grp="${escAttr(g.k)}">
      <button class="scene-grp" type="button" aria-expanded="${!SCENE_CLOSED.has(g.k)}"><span>${g.k}</span><span class="sg-n">${g.items.length}</span><i class="sg-ar"></i></button>
      <div class="scene-items">${g.items.map(s => {
        const nm = (s.name || sceneLabel(s.scene_id)).replace(/^Sales gas metering, /, ""), m = nm.match(/^(.*?)\s*\(([^)]*)\)\s*$/);
        return `<div class="scene-item" data-scene="${s.scene_id}"><span class="sc-nm">${m ? m[1] : nm}${m ? `<span class="sc-pl">${m[2]}</span>` : ""}</span></div>`;
      }).join("")}</div></div>`).join("");
}
function sceneLabel(s) { return s.replace("scene_", "").replace("_wh", ""); }
async function loadScene(sceneId) {
  if (window.Twin3D && window.Twin3D.clear) window.Twin3D.clear();   // the old site (and the engineer) leave at once
  closeAssetPanel();
  const assets = await getJSON("/api/scene/" + encodeURIComponent(sceneId));
  const canvas = $("#canvas");
  closeAssetPanel();   // the detail panel only slides in once an asset or tag is clicked
  if (window.Twin3D && assets.length) {
    canvas.classList.add("three");
    try { window.Twin3D.load("#canvas", assets, loadAsset, { onEmpty: closeAssetPanel }); return; } catch (e) { canvas.classList.remove("three"); }
  }
  // 2D hotspot fallback
  canvas.classList.remove("three"); canvas.innerHTML = "";
  const n = assets.length, cols = Math.max(1, Math.ceil(Math.sqrt(n))), rows = Math.max(1, Math.ceil(n / cols));
  assets.forEach((a, i) => { const c = i % cols, r = Math.floor(i / cols); const h = el(`<div class="hotspot ${a.pin_colour || "green"}" style="left:${((c + 0.5) / cols) * 100}%;top:${((r + 0.5) / rows) * 100}%"><span class="label">${a.asset_id}</span></div>`); h.onclick = () => loadAsset(a.asset_id); canvas.appendChild(h); });
}
function closeAssetPanel() { const p = $("#detail"); if (p) p.classList.remove("open"); }
function loadAsset(asset_id) {
  const p = $("#detail"); if (!p) return;
  p.setAttribute("data-asset", asset_id);
  p.scrollTop = 0; p.classList.add("open");
  renderAssetDetail(p, asset_id, { in3d: true, onClose: closeAssetPanel });
}

/* ---------- Impact analysis: reservoir-to-customer Sankey ---------- */
// A small flow-bands glyph for the Command Centre link (theme colours, no external image).
const IMPACT_ICON = `<svg viewBox="0 0 80 64" aria-hidden="true">
  <rect x="2" y="6" width="9" height="20" rx="2" fill="#0c5a66"/><rect x="2" y="32" width="9" height="26" rx="2" fill="#2f7dc4"/>
  <rect x="35" y="14" width="9" height="38" rx="2" fill="#c98a1e"/>
  <rect x="69" y="4" width="9" height="16" rx="2" fill="#6a5cf0"/><rect x="69" y="24" width="9" height="14" rx="2" fill="#2e9c6a"/><rect x="69" y="42" width="9" height="18" rx="2" fill="#dd4b1a"/>
  <path d="M11 16 C24 16 22 22 35 22" stroke="#0c5a66" stroke-opacity=".35" stroke-width="20" fill="none"/>
  <path d="M11 45 C24 45 22 40 35 40" stroke="#2f7dc4" stroke-opacity=".35" stroke-width="24" fill="none"/>
  <path d="M44 19 C57 19 56 12 69 12" stroke="#6a5cf0" stroke-opacity=".35" stroke-width="10" fill="none"/>
  <path d="M44 31 C57 31 56 31 69 31" stroke="#2e9c6a" stroke-opacity=".35" stroke-width="12" fill="none"/>
  <path d="M44 45 C57 45 56 51 69 51" stroke="#dd4b1a" stroke-opacity=".35" stroke-width="16" fill="none"/></svg>`;
const IMP_COLS = [["reservoir", "Reservoir"], ["wells", "Pads & platforms"], ["gathering", "Gathering"], ["processing", "Processing"],
                  ["compression", "Compression"], ["custody", "Custody"], ["customer", "Customers"]];
const IMP_STOP_COL = { reservoir: "#0c5a66", wells: "#2f7dc4", gathering: "#2e9c6a", processing: "#c98a1e", compression: "#6a5cf0", custody: "#3d5566" };
const IMP_SEG_COL = { "LNG export": "#0c5a66", "LNG portfolio": "#3a8a96", "Retail": "#2f7dc4", "Distribution": "#6aa0d8", "Power generation": "#c98a1e",
                      "Industrial": "#6a5cf0", "Manufacturing": "#9a8cf5", "Mining": "#8a6d3b", "Spot market": "#8c9aa6" };
let IMP = null;   // { g, sel, res, layout }

async function renderImpact() {
  const v = $("#view");
  v.innerHTML = `<div class="imp">
    <div class="imp-head">
      <div><h2>Impact analysis</h2><div class="muted" id="impSub">Loading the supply network…</div></div>
      <div class="imp-acts"><button class="btn ghost" id="impClear" disabled>Clear</button><button class="btn" id="impPdf" disabled>Download Impact Details (PDF)</button></div>
    </div>
    <div class="imp-body"><div class="imp-chart" id="impChart"></div><div class="imp-side"><div class="imp-hint" id="impHint">Click any stop in the flow to trace what feeds it and what it supplies, down to the customers.</div><div class="imp-bubble" id="impBubble"></div></div></div>
  </div>`;
  let g; try { g = await getJSON("/api/impact/graph"); } catch (e) { $("#impSub").textContent = "Impact analysis unavailable. " + e; return; }
  if (CUR !== "impact") return;
  IMP = { g, sel: null, res: null };
  $("#impSub").textContent = `Gas flow from reservoir to customer: the average gas day over the 30 gas days to ${fmtLong(g.as_of)}. Band widths are flow; click a stop to trace it, click empty space to clear.`;
  $("#impClear").onclick = () => impactSelect(null);
  $("#impPdf").onclick = async () => {
    if (!IMP || !IMP.sel) return;
    loaderStart();
    try {
      const r = await fetch("/api/impact/pdf/" + encodeURIComponent(IMP.sel)); if (!r.ok) throw new Error(r.status);
      const a = document.createElement("a"); a.href = URL.createObjectURL(await r.blob());
      a.download = "impact_" + IMP.sel.replace(/[^A-Za-z0-9_-]/g, "_") + ".pdf"; document.body.appendChild(a); a.click();
      setTimeout(() => { URL.revokeObjectURL(a.href); a.remove(); }, 1000);
    } catch (e) { alert("Could not build the PDF: " + e); }
    finally { loaderEnd(); }
  };
  const chart = $("#impChart");
  const draw = () => { if (!chart.isConnected) { window.removeEventListener("resize", draw); return; } drawSankey(chart); if (IMP.res) applyImpact(); };
  draw(); window.addEventListener("resize", draw);
}
function fmtLong(d) { if (!d) return "the latest gas day"; const x = new Date(d + "T00:00:00"); return isNaN(x) ? d : x.toLocaleDateString("en-AU", { day: "numeric", month: "long", year: "numeric" }); }

// Sankey layout: one column per value-chain stop, node height = throughput, link width = flow.
function drawSankey(host) {
  const g = IMP.g, W = Math.max(720, host.clientWidth), H = Math.max(420, host.clientHeight);
  const top = 30, bot = 8, left = 6, labelR = 170, nodeW = 12, colGap = (W - left - labelR - nodeW) / (IMP_COLS.length - 1);
  const byId = {}, cols = IMP_COLS.map(() => []);
  g.nodes.forEach(n => { const o = { ...n, tp: Number(n.throughput_e3m3d) || 0, ins: [], outs: [] }; byId[n.node_id] = o; cols[(n.stop_order || 1) - 1].push(o); });
  const links = g.links.map(l => ({ ...l, f: Number(l.flow_e3m3d) || 0, s: byId[l.from_id], t: byId[l.to_id] })).filter(l => l.s && l.t && l.f > 0);
  links.forEach(l => { l.s.outs.push(l); l.t.ins.push(l); });
  const gapOf = c => Math.min(6, (H - top - bot) * 0.22 / Math.max(1, c.length - 1));
  // scale so every column fits, leaving room for the minimum bar height of its many small stops (pads)
  const MINH = 1;
  const k = Math.min(...cols.map(c => (H - top - bot - gapOf(c) * (c.length - 1) - MINH * c.length) / Math.max(1, c.reduce((a, n) => a + n.tp, 0))));
  cols.forEach((c, ci) => c.forEach(n => { n.x = left + ci * colGap; n.h = Math.max(MINH, n.tp * k); n.col = ci; }));
  const place = c => { const gap = gapOf(c), tot = c.reduce((a, n) => a + n.h, 0) + gap * (c.length - 1); let y = top + Math.max(0, (H - top - bot - tot) / 2); c.forEach(n => { n.y = y; y += n.h + gap; }); };
  // order: reservoirs by size, then each column by the flow-weighted centre of what feeds it (fewer crossings)
  cols[0].sort((a, b) => b.tp - a.tp); place(cols[0]);
  for (let pass = 0; pass < 3; pass++) {
    for (let ci = 1; ci < cols.length; ci++) {
      cols[ci].forEach(n => { const w = n.ins.reduce((a, l) => a + l.f, 0); n.bc = w ? n.ins.reduce((a, l) => a + l.f * (l.s.y + l.s.h / 2), 0) / w : 0; });
      cols[ci].sort((a, b) => a.bc - b.bc); place(cols[ci]);
    }
    for (let ci = cols.length - 2; ci >= 0; ci--) {
      cols[ci].forEach(n => { const w = n.outs.reduce((a, l) => a + l.f, 0); n.bc = w ? n.outs.reduce((a, l) => a + l.f * (l.t.y + l.t.h / 2), 0) / w : n.y; });
      cols[ci].sort((a, b) => a.bc - b.bc); place(cols[ci]);
    }
  }
  // stack link ends on each node in the order of the node they connect to
  Object.values(byId).forEach(n => {
    let so = 0; n.outs.sort((a, b) => a.t.y - b.t.y).forEach(l => { l.w = Math.max(0.6, l.f * k); l.sy = n.y + so + l.w / 2; so += l.f * k; });
    let to = 0; n.ins.sort((a, b) => a.s.y - b.s.y).forEach(l => { l.ty = n.y + to + Math.max(0.6, l.f * k) / 2; to += l.f * k; });
  });
  const colOf = n => n.stop === "customer" ? (IMP_SEG_COL[(g.customers.find(c => c.customer_id === n.node_id) || {}).segment] || "#8c9aa6") : (IMP_STOP_COL[n.stop] || "#5f7185");
  const esc = s => String(s == null ? "" : s).replace(/&/g, "&amp;").replace(/</g, "&lt;");
  const unitTj = n => `${fmt(n.tp)} 10³ m³/d (${fmt(n.tp * 0.038)} TJ/d)`;
  const linkSvg = links.map(l => { const x0 = l.s.x + nodeW, x1 = l.t.x, xm = (x0 + x1) / 2;
    return `<path class="imp-link" data-k="${esc(l.from_id)}>${esc(l.to_id)}" d="M${x0.toFixed(1)},${l.sy.toFixed(1)} C${xm.toFixed(1)},${l.sy.toFixed(1)} ${xm.toFixed(1)},${l.ty.toFixed(1)} ${x1.toFixed(1)},${l.ty.toFixed(1)}" stroke="${colOf(l.s)}" stroke-width="${l.w.toFixed(2)}"><title>${esc(l.s.node_name)} → ${esc(l.t.node_name)}: ${fmt(l.f)} 10³ m³/d</title></path>`; }).join("");
  // every customer is named: labels sit at their bar's centre, nudged apart where small bars bunch up,
  // with a thin leader line back to the bar when a label had to move
  const cust = cols[6].slice().sort((a, b) => a.y - b.y), LH = 13;
  cust.forEach(n => { n.ly = n.y + n.h / 2 + 3.5; });
  for (let i = 1; i < cust.length; i++) cust[i].ly = Math.max(cust[i].ly, cust[i - 1].ly + LH);
  for (let i = cust.length - 1, lim = H - bot; i >= 0; i--) { cust[i].ly = Math.min(cust[i].ly, lim); lim = cust[i].ly - LH; }
  const nodeSvg = Object.values(byId).map(n => {
    const lab = n.h >= 9 || (n.col === 5 && n.h >= 4) || n.col === 0 || n.col === 3 || n.col === 6;   // small customers: name in the tooltip
    // short labels so every column reads cleanly; the full name is in the tooltip
    const name = n.stop === "wells" || n.stop === "gathering" ? n.node_id
      : n.stop === "compression" ? n.node_id.replace(/^(CPF\d+)-CMP-CP-/, "$1 · ")
      : n.stop === "custody" ? ((n.node_name.match(/\(([^,)]*)/) || [, n.node_id])[1])   // the place; the meter code is in the tooltip
      : n.node_name.replace(/ \([^)]*Basin\)$/, "").replace(/ (CPF|GTH|MNF)\d+$/, "");
    const st = n.stop === "wells" ? "" : n.status === "red" ? ' data-st="red"' : n.status === "amber" ? ' data-st="amber"' : "";
    return `<g class="imp-node" data-id="${esc(n.node_id)}"${st}><rect x="${n.x}" y="${n.y.toFixed(1)}" width="${nodeW}" height="${n.h.toFixed(1)}" rx="2" fill="${colOf(n)}"/>
      <rect class="imp-hit" x="${n.x - 3}" y="${(n.y - 1).toFixed(1)}" width="${nodeW + 6}" height="${(n.h + 2).toFixed(1)}"/>
      ${lab ? (n.col === 6
        ? `${Math.abs(n.ly - (n.y + n.h / 2 + 3.5)) > 2 ? `<path class="imp-lead" d="M${n.x + nodeW + 1},${(n.y + n.h / 2).toFixed(1)} L${n.x + nodeW + 9},${(n.ly - 3.5).toFixed(1)}"/>` : ""}<text x="${n.x + nodeW + 11}" y="${n.ly.toFixed(1)}" class="imp-lab big">${esc(name)}</text>`
        : `<text x="${n.x + nodeW + 5}" y="${(n.y + n.h / 2 + 3.5).toFixed(1)}" class="imp-lab${n.col >= 5 ? " big" : ""}">${esc(name)}</text>`) : ""}
      <title>${esc(n.node_name)}\n${unitTj(n)}${n.status && n.status !== "green" ? `\nHealth: ${n.status}` : ""}\nClick to trace</title></g>`; }).join("");
  const heads = IMP_COLS.map((c, i) => `<text class="imp-colh" x="${left + i * colGap}" y="16">${c[1]}</text>`).join("");
  host.innerHTML = `<svg width="${W}" height="${H}" viewBox="0 0 ${W} ${H}" class="imp-svg"><rect class="imp-bg" x="0" y="0" width="${W}" height="${H}"/>${heads}<g class="imp-links">${linkSvg}</g>${nodeSvg}</svg>`;
  IMP.layout = { byId, H };
  host.querySelector(".imp-bg").onclick = () => impactSelect(null);
  host.querySelectorAll(".imp-node").forEach(el => el.onclick = e => { e.stopPropagation(); impactSelect(el.getAttribute("data-id")); });
}

async function impactSelect(id) {
  if (!IMP) return;
  IMP.sel = id; IMP.res = null;
  $("#impPdf").disabled = !id; $("#impClear").disabled = !id;
  if (!id) { applyImpact(); return; }
  const svg = $("#impChart svg"); if (svg) svg.classList.add("busy");
  let r; try { r = await getJSON("/api/impact/select/" + encodeURIComponent(id)); } catch (e) { r = null; }
  if (!IMP || IMP.sel !== id) return;            // a newer click won
  IMP.res = r; applyImpact();
}

function applyImpact() {
  const svg = $("#impChart svg"), bub = $("#impBubble"), hint = $("#impHint"); if (!svg) return;
  svg.classList.remove("busy");
  const r = IMP.res;
  svg.classList.toggle("has-sel", !!r);
  const on = new Set(r ? r.highlight_nodes : []), lk = new Set(r ? r.highlight_links : []);
  svg.querySelectorAll(".imp-node").forEach(el => { const id = el.getAttribute("data-id"); el.classList.toggle("on", on.has(id)); el.classList.toggle("sel", !!r && id === r.node.node_id); });
  svg.querySelectorAll(".imp-link").forEach(el => el.classList.toggle("on", lk.has(el.getAttribute("data-k"))));
  if (!r) { bub.classList.remove("open"); hint.style.display = ""; return; }
  hint.style.display = "none";
  const n = r.node, t = r.totals, cs = r.customers;
  const isCust = n.stop === "customer";
  bub.innerHTML = `<div class="ib-head">
      <div class="ib-k">${n.stop_label}</div><div class="ib-t">${n.node_name}</div>
      <div class="ib-m">${isCust ? `Supplied through ${t.upstream_stops} upstream stops` : `${t.upstream_stops} stops upstream · ${t.downstream_stops} downstream`} · ${fmt(n.throughput_e3m3d)} 10³ m³/d</div>
    </div>
    <div class="ib-sum"><div><b>${t.customers}</b><span>${t.customers === 1 ? "customer" : "customers"}</span></div><div><b>${fmt(t.at_risk_tj_d)}</b><span>TJ/d ${isCust ? "supplied" : "at risk"}</span></div><div><b>$${fmt(t.revenue_at_risk_per_day)}</b><span>revenue / day</span></div></div>
    <div class="ib-list">${cs.length ? cs.map(c => `<div class="ib-c" data-id="${escAttr(c.customer_id)}">
        <div class="ib-c1"><span class="ib-dot" style="background:${IMP_SEG_COL[c.segment] || "#8c9aa6"}"></span><b>${c.customer_name}</b><span class="ib-v">${fmt(c.at_risk_tj_d)} TJ/d</span></div>
        <div class="ib-c2">${c.segment} · ${c.location} · ${c.delivery_point} · ${c.contract_type}<span class="ib-p">${fmt(c.pct_of_supply)}% of its gas</span></div>
        <div class="ib-bar"><i style="width:${Math.min(100, Number(c.pct_of_supply) || 0)}%"></i></div></div>`).join("") : `<div class="muted" style="padding:10px 2px">No customers downstream of this stop.</div>`}</div>`;
  bub.classList.add("open");
  bub.querySelectorAll(".ib-c").forEach(el => el.onclick = () => impactSelect(el.getAttribute("data-id")));
  // aim the bubble's tail at the customers it lists (their centre on the chart)
  const ys = cs.map(c => IMP.layout.byId[c.customer_id]).filter(Boolean).map(o => o.y + o.h / 2);
  const box = bub.getBoundingClientRect(), sb = svg.getBoundingClientRect();
  const yc = ys.length ? (Math.min(...ys) + Math.max(...ys)) / 2 + sb.top - box.top : 40;
  bub.style.setProperty("--tail", Math.max(22, Math.min(box.height - 22, yc)) + "px");
}

/* ---------- Leaflet map ---------- */
function renderMapScreen() {
  const v = $("#view");
  v.innerHTML = `<div id="mapbox" class="leafmap"></div>`;
  renderMapInto("#mapbox");
}

// Size a map to the space left below its header/legend so the page never scrolls.
function fitMapHeight(host) {
  const fit = () => { if (!host.isConnected) { window.removeEventListener("resize", fit); return; }
    const vb = $("#view").getBoundingClientRect().bottom, inTab = host.closest("#gfc-body") ? 20 : 0;
    host.style.height = Math.max(360, vb - host.getBoundingClientRect().top - 18 - inTab) + "px";
    if (_leaf) _leaf.invalidateSize(); };
  fit(); window.addEventListener("resize", fit);
}
async function renderMapInto(sel) {
  const host = $(sel); if (!host || typeof L === "undefined") { if (host) host.innerHTML = '<div class="muted" style="padding:16px">map unavailable</div>'; return; }
  if (_leaf) { try { _leaf.remove(); } catch (e) {} _leaf = null; }
  fitMapHeight(host);
  const map = L.map(host, { minZoom: 3, maxZoom: 16, zoomControl: true, preferCanvas: true });
  // open zoomed onto the mainland basins where the facilities are, not the whole Asia-Pacific
  map.setView([-26.5, 132.6], 5);
  _leaf = map;
  L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", { subdomains: "abc", maxZoom: 19, attribution: "© OpenStreetMap" }).addTo(map);
  let data; try { data = await getJSON("/api/map"); } catch (e) { return; }
  const facs = data.facilities || []; const maxR = Math.max(1, ...facs.map(f => f.risk_dollars || 0));
  const col = { red: "#e0533a", amber: "#e0a53a", green: "#37b98a" };
  const siteCluster = c => { const n = c.getChildCount();
    return L.divIcon({ className: `marker-cluster sitecl marker-cluster-${n < 10 ? "small" : n < 100 ? "medium" : "large"}`,
      html: `<div><span>${n} sites</span></div>`, iconSize: [72, 40] }); };
  const layer = (window.L.markerClusterGroup ? L.markerClusterGroup({ iconCreateFunction: siteCluster }) : L.layerGroup());
  facs.forEach(f => {
    // sized so every facility is easy to see: a visible base + growth with asset count + risk emphasis
    const rad = 15 + Math.sqrt(f.assets || 1) * 2.4 + Math.sqrt((f.risk_dollars || 0) / maxR) * 14;
    // a single site reads "1 site", matching the "N sites" group bubbles
    const d = Math.round(rad * 2), c = col[f.pin_colour] || "#37b98a";
    const mk = L.marker([f.lat, f.lon], { icon: L.divIcon({ className: "site1", iconSize: [d, d],
      html: `<div style="width:${d}px;height:${d}px;background:${c}b3;border:1px solid ${c}">1 site</div>` }) });
    const healthy = Math.max(0, (f.assets || 0) - (f.red || 0) - (f.amber || 0));
    const par = Number(f.risk_dollars) > 0 ? `${fmt(f.risk_dollars)} ×10³m³/d` : "none";
    mk.bindTooltip(`<div class="sitetip"><b>${f.name || f.area_code}</b>${f.name && !f.name.includes(f.area_code) ? `<span class="muted"> ${f.area_code}</span>` : ""}
      <div>${f.assets} assets: <span class="st-r">${f.red} red</span> · <span class="st-a">${f.amber} amber</span> · <span class="st-g">${healthy} healthy</span></div>
      <div>Production at risk: ${par}</div><div class="muted">Click for site view</div></div>`,
      { direction: "top", offset: [0, -Math.round(rad)], opacity: 1 });
    mk.on("click", () => { mk.closeTooltip(); PENDING_SCENE = f.area_code; select("navigator"); });
    layer.addLayer(mk);
  });
  map.addLayer(layer);
  setTimeout(() => map.invalidateSize(), 200);
}

/* ---------- Weather & Environment: per-location map (item 39) ---------- */
async function renderWeather(sel) {
  const v = $(sel || "#view");
  v.innerHTML = `    <div id="wxtiles" class="kpis"></div>
    <div class="faclegend"><span class="fl-item">Marker colour = average temperature (cool blue → hot red)</span></div>
    <div id="wxmap" class="leafmap"></div>`;
  let sites; try { sites = await getJSON("/api/weather_map"); } catch (e) { $("#wxmap").innerHTML = '<div class="muted" style="padding:16px">weather unavailable</div>'; return; }
  if (!sites.length) return;
  const hottest = sites.reduce((a, b) => b.tmax > a.tmax ? b : a);
  const windiest = sites.reduce((a, b) => b.wind_max > a.wind_max ? b : a);
  const flood = sites.filter(s => s.fld_max > 0.3).length, ltn = sites.filter(s => s.ltn_max > 0.5).length;
  $("#wxtiles").innerHTML = [
    [`${fmt(hottest.tmax)}°C`, "Hottest site", "Highest daily temperature recorded at any site over the period.", hottest],
    [`${fmt(windiest.wind_max)} km/h`, "Windiest site", "Strongest daily wind recorded at any site over the period.", windiest],
    [flood, "Sites at flood risk", "Sites whose flood-risk index went above 0.3 at some point."],
    [ltn, "Sites at lightning risk", "Sites whose lightning-risk index went above 0.5 at some point."],
  ].map(t => `<div class="kpi${t[3] ? " click" : ""}"${t[3] ? ` data-wx="${escAttr(t[3].area_code)}" title="Open this site's weather history"` : ""}><div class="kpi-v">${t[0]}</div><div class="kpi-l">${t[1]} ${infoI(t[2])}</div>${t[3] ? `<div class="kpi-site">${t[3].name || t[3].area_code}</div>` : ""}</div>`).join("");
  $("#wxtiles").querySelectorAll("[data-wx]").forEach(k => k.onclick = () => openWeatherSite(k.getAttribute("data-wx"), (sites.find(s => s.area_code === k.getAttribute("data-wx")) || {}).name));
  const host = $("#wxmap"); if (!host || typeof L === "undefined") return;
  if (_leaf) { try { _leaf.remove(); } catch (e) {} _leaf = null; }
  fitMapHeight(host);
  const map = L.map(host, { minZoom: 3, maxZoom: 16, zoomControl: true, zoomSnap: 0.25 });
  // fit the Australian mainland tightly (fractional zoom), so the sites fill the map
  map.fitBounds([[-39.0, 113.2], [-11.0, 153.8]], { padding: [10, 10] }); _leaf = map;
  L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", { subdomains: "abc", maxZoom: 19, attribution: "© OpenStreetMap" }).addTo(map);
  const tavgs = sites.map(s => s.tavg).filter(x => x != null), lo = Math.min(...tavgs), hi = Math.max(...tavgs);
  const col = t => { const f = (hi > lo) ? (t - lo) / (hi - lo) : 0.5; return `rgb(${Math.round(55 + f * 180)},90,${Math.round(215 - f * 170)})`; };
  sites.forEach(s => {
    if (s.lat == null) return;
    const mk = L.circleMarker([s.lat, s.lon], { radius: 9, color: "#fff", weight: 1, fillColor: col(s.tavg), fillOpacity: .85 });
    mk.bindTooltip(`<b>${s.name || s.area_code}</b><br>${fmt(s.tmin)} to ${fmt(s.tmax)}°C · wind up to ${fmt(s.wind_max)} km/h`);
    mk.on("click", () => openWeatherSite(s.area_code, s.name));
    mk.addTo(map);
  });
  // re-fit once the tab has its final size: frame the weather sites themselves (they span the mainland)
  const pts = sites.filter(s => s.lat != null).map(s => [s.lat, s.lon]);
  const fit = () => { map.invalidateSize(); if (pts.length) map.fitBounds(pts, { padding: [36, 36], animate: false }); };
  setTimeout(fit, 200);
}
async function openWeatherSite(area, name) {
  let rows; try { rows = await getJSON("/api/weather_site/" + encodeURIComponent(area)); } catch (e) { return; }
  const m = chartModal();
  m.querySelector(".cm-t").textContent = (name || area) + ": weather history";
  m.querySelector(".cm-body").innerHTML = `<div class="wx-site-charts">
    <div class="card"><div class="card-h">Ambient temperature (°C)</div>${svgLine(rows, "obs_date", [{ key: "ambient_c", label: "°C" }])}</div>
    <div class="card"><div class="card-h">Wind (km/h)</div>${svgLine(rows, "obs_date", [{ key: "wind_kmh", label: "km/h" }])}</div>
    <div class="card"><div class="card-h">Lightning &amp; flood risk</div>${svgLine(rows, "obs_date", [{ key: "lightning_risk", label: "lightning" }, { key: "flood_risk", label: "flood" }])}</div>
  </div>`;
  m.classList.add("open"); m.querySelector(".cm-body").scrollTop = 0;
}

/* ---------- Asset Explorer: the tag contract, resolved live ---------- */
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
    <div class="explorer-split">
      <div class="card explorer-col">
        <div class="card-h">Resolve a tag: the contract in action</div>
        <div class="blurb" style="margin-bottom:10px">Type a tag, asset, area or signal. Each hit resolves the contextualized tag to its asset, functional location, latest sensor reading and its ids in every other system (PI historian, EAM, OSDU, P&ID): the one join key that ties OT to IT. Every asset here links to its 360.</div>
        <div class="search-box"><input id="tl_q" placeholder="e.g. CPF002-CMP · vibration · BAR003-WB · dewpoint"/><button class="btn" id="tl_go">Resolve</button></div>
        <div style="display:flex;gap:7px;flex-wrap:wrap;margin:2px 0 12px">${chips.map(x => `<span data-chip="${x}" style="cursor:pointer;font-size:11.5px;background:var(--panel2);border:1px solid var(--line);border-radius:12px;padding:3px 11px;color:var(--muted)">${x}</span>`).join("")}</div>
        <div id="tl_res"></div>
      </div>
      <div class="card explorer-col">
        <div class="card-h">Browse by physical location</div>
        <div class="blurb" style="margin-bottom:10px">The functional-location hierarchy: field → basin → site → asset → tag. Click an asset to open its 360; click a tag to resolve it on the left.</div>
        <div id="loc_tree"><div class="muted">Loading locations…</div></div>
      </div>
    </div>`;
  const run = async (term) => {
    if (!term || !term.trim()) { $("#tl_res").innerHTML = '<div class="muted">Type a tag, asset, area or signal to resolve the contract, or pick one from the location tree on the right.</div>'; return; }
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
  // item 52: no pre-loaded results: start empty with a prompt
  run("");
  wireLocationTree(run);
}

// item 54: lazy physical-location tree (area -> asset -> tag). A tag click resolves on the left.
async function wireLocationTree(run) {
  const host = $("#loc_tree");
  let d; try { d = await getJSON("/api/tree"); } catch (e) { host.innerHTML = '<div class="muted">location tree unavailable</div>'; return; }
  const areas = d.areas || [];
  // the location level: every site sits in its basin (named in the site name, e.g. "(Karratha, Carnarvon Basin)");
  // basins with the most sites first, and within a basin the sites along the gas path (pads, platforms, hubs,
  // manifolds, plants, custody)
  const KIND = ["BAR", "PLT", "GTH", "MNF", "CPF", "MET"], basinOf = a => ((a.name || "").match(/([^,(]+) Basin\)\s*$/) || [, "Other"])[1].trim();
  const groups = {};
  areas.forEach(a => (groups[basinOf(a)] = groups[basinOf(a)] || []).push(a));
  const order = Object.keys(groups).sort((x, y) => groups[y].length - groups[x].length || x.localeCompare(y));
  const sum = (l, k) => l.reduce((t, a) => t + (Number(a[k]) || 0), 0);
  host.innerHTML = order.map(b => {
    const sites = groups[b].slice().sort((x, y) => KIND.indexOf(x.area_code.slice(0, 3)) - KIND.indexOf(y.area_code.slice(0, 3)) || x.area_code.localeCompare(y.area_code));
    return `<div class="tnode basin" data-basin="${escAttr(b)}">
      <div class="trow basin"><span class="tcaret">▸</span><span class="tlabel">${b} Basin</span><span class="tmeta">${sites.length} sites · ${fmt(sum(sites, "n_assets"))} assets · ${fmt(sum(sites, "n_tags"))} tags</span></div>
      <div class="tkids" style="display:none" data-loaded="1">${sites.map(a =>
        `<div class="tnode" data-area="${escAttr(a.area_code)}">
          <div class="trow area"><span class="tcaret">▸</span><span class="tlabel">${(a.name || a.area_code).replace(/^Sales gas metering, /, "").replace(/ \(([^)]*)\)\s*$/, (m, p) => p.includes(",") ? ` (${p.split(",")[0].trim()})` : "")}</span><span class="tmeta">${a.n_assets} ${a.n_assets == 1 ? "asset" : "assets"} · ${a.n_tags} tags</span></div>
          <div class="tkids" style="display:none"></div>
        </div>`).join("")}</div>
    </div>`; }).join("") || '<div class="muted">No locations.</div>';
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
// Each menu item carries its hand-drawn icon (img/nav/<key>.png, made with Meshy in the blue-flame colours).
const NAV_ICONS = new Set(["command_centre", "gas_flow_chain", "cross_landscape", "commercial_landscape", "recommendations", "asset_model"]);
const navBtn = (key, label) => `<button class="nav-item${NAV_ICONS.has(key) ? " has-ic" : ""}" data-key="${key}" aria-label="${label}">` +
  (NAV_ICONS.has(key) ? `<img class="nav-ic" src="img/nav/${key}.png?v=4" alt="" aria-hidden="true">` : "") + `<span>${label}</span></button>`;
function buildNav() {
  const groups = {};
  NAV.forEach(n => { (groups[n.group] = groups[n.group] || []).push(n); });
  // Item 49: a flat menu, no group headings. Value-chain stops collapse into one "Gas flow chain"
  // entry (item 9) and the cross-cutting reports into one "Analytics" entry (item 28).
  $("#nav").innerHTML = Object.entries(groups).map(([g, items]) => {
    if (g === "Value chain") return navBtn("gas_flow_chain", "Gas flow chain");
    if (g === "Cross-cutting") return navBtn("cross_landscape", "Analytics");
    if (g === "Commercial") return navBtn("commercial_landscape", "Commercial & Finance");
    return items.map(n => navBtn(n.key, n.label)).join("");
  }).join("");
  $("#nav").querySelectorAll("[data-key]").forEach(b => b.onclick = () => {
    const k = b.getAttribute("data-key");
    if (k === "commercial_landscape" && CUR !== k) BRIEF_ON_ENTRY = true;   // entering Commercial & Finance from the menu
    select(k);
  });
  $("#nav").addEventListener("scroll", placeTail);
}
let GENIE_URL;
async function genieUrl() { if (GENIE_URL === undefined) { try { GENIE_URL = (await getJSON("/api/genie-link")).url; } catch (e) { GENIE_URL = null; } } return GENIE_URL; }
// Utility link top-right of the header: the Genie lamp (opens the Genie space) on every screen.
async function setHeadLinks() {
  const host = $("#headlinks"); if (!host) return;
  const gu = await genieUrl();
  host.innerHTML = gu ? `<a class="hl genie-link" href="${gu}" target="_blank" rel="noopener" title="Ask Genie" aria-label="Ask Genie"><img src="img/genie.png" alt="Ask Genie"></a>` : "";
}
// Full-screen site view: hides the left menu (with a slide) so the 3D landscape gets the whole screen.
function setImmersive(on) {
  document.body.classList.toggle("immersive", !!on);
  if (!on) setTimeout(placeTail, 320);   // once the menu has slid back in
}
function select(key) {
  CUR = key;
  if (key !== "navigator") setImmersive(false);
  // Screens outside the menu (Field Map, 3D site view) keep the item you came from highlighted,
  // so the content bubble always has a menu item to point at.
  const items = [...$("#nav").querySelectorAll(".nav-item")];
  if (items.some(b => b.getAttribute("data-key") === key)) items.forEach(b => b.classList.toggle("active", b.getAttribute("data-key") === key));
  placeTail();
  setHeadLinks();
  route(key);
}
// The content area is drawn as a speech bubble; its tail points at the highlighted menu item.
function placeTail() {
  const w = $(".viewwrap"), a = $("#nav .nav-item.active"); if (!w) return;
  if (!a) { w.style.setProperty("--tail-op", "0"); return; }
  const y = a.getBoundingClientRect().top + a.offsetHeight / 2 - w.getBoundingClientRect().top;
  // keep the tail clear of the bubble's rounded corners; hide it only if the item is scrolled out of view
  w.style.setProperty("--tail-y", Math.round(Math.min(Math.max(y, 26), w.clientHeight - 26)) + "px");
  w.style.setProperty("--tail-op", y > -10 && y < w.clientHeight + 10 ? "1" : "0");
}
window.addEventListener("resize", placeTail);
function route(key) {
  if (key === "command_centre") return renderCommand();
  if (key === "map") return renderMapScreen();
  if (key === "impact") return renderImpact();
  if (key === "gas_flow_chain") return renderGasFlowChain();
  if (key === "cross_landscape") return renderCrossLandscape();
  if (key === "commercial_landscape") return renderCommercialLandscape();
  if (key === "weather") return renderWeather();
  if (key === "navigator") return renderNavigator();
  if (key === "recommendations") return renderRecommendations();
  if (key === "asset_model") return renderAssetExplorer();
  return renderModule(key);
}
async function loadFreshness() { const f = $("#freshness"); if (!f) return; try { const h = await getJSON("/health"); f.textContent = h.ok ? `${fmt(h.assets)} assets live` : "warming up…"; } catch (e) { f.textContent = ""; } }

async function boot() {
  document.addEventListener("click", e => {
    const th = e.target.closest("th.sortable"); if (th) { sortByTh(th); return; }
    const wo = e.target.closest("[data-wo-id]"); if (wo) { e.preventDefault(); const row = wo.closest("[data-back]") || wo;
      const in3d = wo.closest("#detail") ? (id) => { closeDrawer(); if ($("#detail").getAttribute("data-asset") !== id || !$("#detail").classList.contains("open")) loadAsset(id); } : null;
      openWoDrawer(wo.getAttribute("data-wo-id"), row.getAttribute("data-back"), in3d); return; }
    const ac = e.target.closest("td[data-asset-id]"); if (ac) { openAssetDrawer(ac.getAttribute("data-asset-id")); return; }  // asset-id cell wins (item 29)
    const kl = e.target.closest(".kpi[data-list]"); if (kl) { openListDrawer(kl.getAttribute("data-list")); return; }       // tile opens a full list
    const g = e.target.closest("[data-goto]"); if (g) { const fk = g.getAttribute("data-focus"); FOCUS = fk && CC_FOCUS[fk] ? CC_FOCUS[fk] : null; goTo(g.getAttribute("data-goto")); return; }
    const row = e.target.closest("tr.rowclick"); if (row) { const ent = row.getAttribute("data-entity"), id = row.getAttribute("data-id"); if (ent === "workorder") openWoDrawer(id); else openAssetDrawer(id); return; }  // context-sensitive
    const k = e.target.closest(".kpi[data-link]"); if (k) { goTo(k.getAttribute("data-link")); return; }
  });
  loadFreshness();
  loadPrefs();
  setTimeout(loadBrief, 1200);   // ready the briefing before Commercial & Finance is opened
  try { NAV = await getJSON("/api/modules"); } catch (e) { $("#view").innerHTML = '<div class="placeholder">Could not load modules.</div>'; return; }
  buildNav();
  select("command_centre");
}
boot();
