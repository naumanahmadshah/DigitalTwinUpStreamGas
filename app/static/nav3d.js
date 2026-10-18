// Real 3D scene for the Asset Twin Navigator. Each asset becomes a recognisable, procedurally-built
// piece of process equipment (compressor package, horizontal separator, wellhead tree, custody meter
// run, flare, TEG/amine columns, NGL cold box, gathering segment) rendered in a clean matte "clay"
// style with soft shadows, sat on a structural skid. Alarm state shows on a stack-light beacon so the
// equipment itself stays true-to-life. Three.js is vendored locally (single-command portable).
// Exposes window.Twin3D.

import * as THREE from "./vendor/three.module.js";

const COL = { red: 0xe0533a, amber: 0xe0a53a, green: 0x3fb27f };

let renderer, scene, camera, assetGroup, envGroup, raf, container, onPick, tipEl;
let hemiLight, keyLight, fillLight, groundMesh, gridHelp;
let ray = new THREE.Raycaster(), mouse = new THREE.Vector2();
let theta = 0.9, phi = 1.02, radius = 60;
const target = new THREE.Vector3(0, 3, 0);
let drag = false, lx = 0, ly = 0, hovered = null;
// vertical tilt band (polar angle from +Y). Indoor locks to a narrow "eye-level" band so the camera
// walks around the plant floor like a person and never flies over or under the equipment.
let phiMin = 0.2, phiMax = 1.45;
// per-asset ceiling spotlights + light panels; the one nearest the camera becomes the lit "hero".
let assetSpots = [], indoorMode = false, nearestIdx = -1;
// status balls: a glowing sphere that levitates just to the left of each asset's (camera-facing) tag.
let tagBalls = [];
const _camRight = new THREE.Vector3();
// zoom level-of-detail: zooming out past a threshold reveals the facility (interior shell drops away),
// and further out hands off to the map. Callbacks let the app switch to the Leaflet map.
let onZoomOut = null, onStage = null, onEmpty = null, downX = 0, downY = 0, initRadius = 40, facilityStage = false, indoorShell = [];
// zoom-out is hard-clamped at the facility view (maxRadius); a deliberate extra push past that stop
// (overscroll ticks) is required to step up to the map.
let maxRadius = 220, overscroll = 0;

// ---------------- materials (clay render) ----------------
function clay() { return new THREE.MeshStandardMaterial({ color: 0xe7ebef, metalness: 0.05, roughness: 0.82 }); }
function clayDark() { return new THREE.MeshStandardMaterial({ color: 0xccd3da, metalness: 0.08, roughness: 0.8 }); }
function steel() { return new THREE.MeshStandardMaterial({ color: 0x8ea3b8, metalness: 0.3, roughness: 0.6 }); }   // muted blue-grey structural steel
function deck() { return new THREE.MeshStandardMaterial({ color: 0x8092a6, metalness: 0.25, roughness: 0.7 }); }
// tasteful industrial accent colours
function pipe() { return new THREE.MeshStandardMaterial({ color: 0x5f93b8, metalness: 0.2, roughness: 0.6 }); }    // process piping (blue)
function accentR() { return new THREE.MeshStandardMaterial({ color: 0xc2593f, metalness: 0.15, roughness: 0.55 }); } // valves / handwheels (red)
function cabinetM() { return new THREE.MeshStandardMaterial({ color: 0x4f6f92, metalness: 0.25, roughness: 0.6 }); } // instrument cabinets (blue)
function hot() { return new THREE.MeshStandardMaterial({ color: 0xdd8a44, metalness: 0.1, roughness: 0.6 }); }      // flare tip / hot (amber)
function beaconMat(c) { return new THREE.MeshStandardMaterial({ color: c, emissive: c, emissiveIntensity: 0.9, metalness: 0.1, roughness: 0.4 }); }

// ---------------- geometry helpers (all cast/receive shadow) ----------------
function mesh(geo, m) { const o = new THREE.Mesh(geo, m); o.castShadow = true; o.receiveShadow = true; return o; }
function cyl(r1, r2, h, m, seg) { return mesh(new THREE.CylinderGeometry(r1, r2, h, seg || 22), m); }
function tube(r, h, m) { return cyl(r, r, h, m); }
function box(w, h, d, m) { return mesh(new THREE.BoxGeometry(w, h, d), m); }
function cap(r, m) { const o = mesh(new THREE.SphereGeometry(r, 20, 12, 0, Math.PI * 2, 0, Math.PI / 2), m); o.scale.y = 0.5; return o; }
function flange(r, m) { return cyl(r, r, 0.16, m, 20); }
function wheel(r, m) { return mesh(new THREE.TorusGeometry(r, r * 0.14, 8, 18), m); }
function elbow(r, R, m) { return mesh(new THREE.TorusGeometry(R, r, 10, 16, Math.PI / 2), m); }

// ---------------- fine-detail helpers (bolts, gauges, valves, junction boxes) ----------------
function darkSteel() { return new THREE.MeshStandardMaterial({ color: 0x5c6672, metalness: 0.45, roughness: 0.5 }); } // nuts / bolts
function nut(rr, m) { return mesh(new THREE.CylinderGeometry(rr, rr, rr * 1.15, 6), m); } // hex nut
// A ring of hex bolts around a flange face. normal = 'x'|'y'|'z' (the flange's axis); centred at (x,y,z), bolt-circle radius rr.
function bolts(g, x, y, z, rr, n, normal, m) {
  m = m || darkSteel();
  for (let i = 0; i < n; i++) {
    const a = (i / n) * Math.PI * 2, c = Math.cos(a) * rr * 0.84, s = Math.sin(a) * rr * 0.84;
    const b = nut(Math.max(0.028, rr * 0.11), m); b.castShadow = false;
    if (normal === 'y') b.position.set(x + c, y, z + s);
    else if (normal === 'x') { b.rotation.z = Math.PI / 2; b.position.set(x, y + c, z + s); }
    else { b.rotation.x = Math.PI / 2; b.position.set(x + c, y + s, z); }
    g.add(b);
  }
}
// A dial pressure/temperature gauge on a short stem (face in the XZ-normal-to-z plane by default).
function gauge(g, x, y, z, faceM) {
  const s = steel();
  const stem = tube(0.03, 0.26, s); stem.position.set(x, y - 0.13, z); g.add(stem);
  const face = cyl(0.12, 0.12, 0.05, faceM || clayDark(), 16); face.rotation.x = Math.PI / 2; face.position.set(x, y, z); g.add(face);
  const rim = mesh(new THREE.TorusGeometry(0.12, 0.02, 6, 16), s); rim.rotation.x = Math.PI / 2; rim.position.set(x, y, z); rim.castShadow = false; g.add(rim);
}
// An instrument / junction box on the equipment.
function jbox(g, x, y, z, m) { const b = box(0.32, 0.42, 0.2, m || cabinetM()); b.position.set(x, y, z); g.add(b); }
// An inline hand-operated valve with a red handwheel; bonnet points +y.
function valveBW(g, x, y, z, size) {
  const body = mesh(new THREE.SphereGeometry(size, 12, 10), clay()); body.scale.set(1, 1.15, 1); body.position.set(x, y, z); g.add(body);
  const bonnet = tube(size * 0.34, size * 1.7, steel()); bonnet.position.set(x, y + size * 1.1, z); g.add(bonnet);
  const w = wheel(size * 1.15, accentR()); w.position.set(x, y + size * 1.95, z); w.rotation.x = Math.PI / 2; g.add(w);
}

// A structural skid: base deck, corner posts, top rail. Gives the "process module" look.
function skid(g, w, d, h, m) {
  const base = box(w, 0.35, d, m); base.position.y = 0.18; g.add(base);
  const pr = 0.09;
  for (const sx of [-1, 1]) for (const sz of [-1, 1]) {
    const p = tube(pr, h, m); p.position.set(sx * (w / 2 - 0.2), h / 2, sz * (d / 2 - 0.2)); g.add(p);
  }
  // top rails
  for (const sz of [-1, 1]) { const r = box(w - 0.3, 0.08, 0.08, m); r.position.set(0, h, sz * (d / 2 - 0.2)); g.add(r); }
  for (const sx of [-1, 1]) { const r = box(0.08, 0.08, d - 0.3, m); r.position.set(sx * (w / 2 - 0.2), h, 0); g.add(r); }
}

// A caged access ladder on the side of a tall item.
function ladder(g, x, z, h, m) {
  const s = steel();
  for (const dx of [-0.18, 0.18]) { const r = tube(0.03, h, s); r.position.set(x + dx, h / 2, z); g.add(r); }
  for (let y = 0.4; y < h; y += 0.4) { const rung = box(0.4, 0.03, 0.03, s); rung.position.set(x, y, z); g.add(rung); }
}

// A ring platform + rail around a tall column at height y.
function platform(g, r, y, m) {
  const ring = mesh(new THREE.TorusGeometry(r + 0.35, 0.09, 8, 28), m); ring.rotation.x = Math.PI / 2; ring.position.y = y; g.add(ring);
  const rail = mesh(new THREE.TorusGeometry(r + 0.35, 0.04, 6, 28), m); rail.rotation.x = Math.PI / 2; rail.position.y = y + 1.0; g.add(rail);
  for (let a = 0; a < Math.PI * 2; a += Math.PI / 6) {
    const p = tube(0.03, 1.0, m); p.position.set(Math.cos(a) * (r + 0.35), y + 0.5, Math.sin(a) * (r + 0.35)); g.add(p);
  }
}

// ---------------- per-type equipment templates (centred at origin, y=0 = ground) ----------------
function buildCompressor(g) {
  // A reciprocating gas-compressor package: engine driver -> crankcase frame -> two cylinder throws
  // with distance pieces + heads, suction/discharge pulsation bottles, suction scrubbers, discharge
  // piping, exhaust silencer + stack, lube-oil console, fin-fan cooler, gauge board and cabling.
  const m = clay(), s = steel(), pm = pipe(), cd = clayDark();
  skid(g, 6.0, 3.2, 1.4, s);
  for (const sx of [-1, 1]) for (const sz of [-1, 1]) bolts(g, sx * 2.7, 0.36, sz * 1.35, 0.2, 4, 'y'); // hold-down / anchor bolts
  // crankcase frame + pedestal
  const pedestal = box(2.8, 0.5, 2.4, s); pedestal.position.set(-1.0, 0.6, 0); g.add(pedestal);
  const frame = box(1.4, 1.25, 2.3, m); frame.position.set(0.35, 1.02, 0); g.add(frame);
  const sump = box(1.5, 0.35, 2.4, cd); sump.position.set(0.35, 0.55, 0); g.add(sump);         // crankcase oil sump
  // two compressor cylinder throws (front/back), each: distance piece, cylinder, outboard head + bolts
  for (const sz of [-0.66, 0.66]) {
    const dp = cyl(0.34, 0.34, 0.55, s); dp.rotation.z = Math.PI / 2; dp.position.set(-0.28, 1.15, sz); g.add(dp);
    const cylb = cyl(0.62, 0.62, 1.85, m); cylb.rotation.z = Math.PI / 2; cylb.position.set(-1.45, 1.15, sz); g.add(cylb);
    const headO = cap(0.62, m); headO.rotation.z = Math.PI / 2; headO.position.set(-2.38, 1.15, sz); g.add(headO);
    bolts(g, -2.34, 1.15, sz, 0.66, 12, 'x');
    // suction (front) + discharge (rear) pulsation dampener bottles above the cylinder
    for (const [by, br] of [[2.28, 0.34], [2.9, 0.28]]) {
      const bot = cyl(br, br, 1.95, m); bot.rotation.z = Math.PI / 2; bot.position.set(-1.45, by, sz); g.add(bot);
      const c1 = cap(br, m); c1.rotation.z = Math.PI / 2; c1.position.set(-2.43, by, sz); g.add(c1);
      const c2 = cap(br, m); c2.rotation.z = -Math.PI / 2; c2.position.set(-0.47, by, sz); g.add(c2);
    }
    const noz1 = tube(0.1, 0.55, pm); noz1.position.set(-1.0, 1.75, sz); g.add(noz1);   // cylinder -> bottle
    const noz2 = tube(0.1, 0.7, pm); noz2.position.set(-1.9, 1.9, sz); g.add(noz2);
  }
  // gas-engine / motor driver + valve cover + coupling guard
  const driver = box(2.3, 1.55, 2.0, m); driver.position.set(1.75, 1.15, 0); g.add(driver);
  const drtop = box(1.7, 0.4, 1.6, cd); drtop.position.set(1.75, 2.0, 0); g.add(drtop);
  for (const sz of [-0.6, 0, 0.6]) { const ex = tube(0.07, 0.5, s); ex.position.set(0.9, 1.9, sz); ex.rotation.z = 0.5; g.add(ex); } // rocker/exhaust runners
  const guard = cyl(0.44, 0.44, 0.7, s); guard.rotation.z = Math.PI / 2; guard.position.set(0.95, 1.15, 0); g.add(guard);
  // suction scrubbers (knockout bottles) at the far end, with bolted heads + gauges
  for (const sz of [-0.55, 0.55]) {
    const sc = cyl(0.4, 0.4, 1.9, m); sc.position.set(-2.6, 1.35, sz); g.add(sc);
    const cp = cap(0.4, m); cp.position.set(-2.6, 2.3, sz); g.add(cp);
    bolts(g, -2.6, 2.3, sz, 0.44, 8, 'y'); gauge(g, -2.18, 2.0, sz, cd);
  }
  // discharge pipe run over the top + block valve
  const p1 = tube(0.22, 1.5, pm); p1.position.set(-0.5, 3.05, 1.0); g.add(p1);
  const e1 = elbow(0.22, 0.5, pm); e1.position.set(-0.5, 3.8, 1.0); e1.rotation.y = Math.PI; g.add(e1);
  const p2 = tube(0.22, 2.4, pm); p2.rotation.z = Math.PI / 2; p2.position.set(0.7, 4.25, 1.0); g.add(p2);
  valveBW(g, 0.7, 4.35, 1.0, 0.17);
  // exhaust silencer drum + stack + rain cap
  const sil = cyl(0.42, 0.42, 1.2, cd); sil.position.set(2.2, 2.4, -0.7); g.add(sil);
  bolts(g, 2.2, 3.0, -0.7, 0.46, 8, 'y');
  const stack = cyl(0.18, 0.14, 1.8, cd); stack.position.set(2.2, 3.6, -0.7); g.add(stack);
  const rc = cap(0.22, s); rc.position.set(2.2, 4.55, -0.7); g.add(rc);
  // lube-oil / control console beside the driver
  const con = box(1.0, 0.95, 0.75, m); con.position.set(2.9, 0.65, -1.05); g.add(con);
  const pump = cyl(0.18, 0.18, 0.55, cd); pump.rotation.z = Math.PI / 2; pump.position.set(2.9, 1.25, -1.05); g.add(pump);
  // fin-fan cooler on the skid top
  const cooler = box(1.8, 0.16, 1.6, cd); cooler.position.set(0.4, 1.72, 1.05); g.add(cooler);
  for (let fx = -0.75; fx <= 0.7501; fx += 0.2) { const fin = box(0.02, 0.5, 1.5, s); fin.position.set(0.4 + fx, 1.99, 1.05); fin.castShadow = false; g.add(fin); }
  // instrument cabinet + local gauge board + cable tray
  jbox(g, 1.75, 2.05, 1.05);
  const gbrd = box(0.7, 0.55, 0.05, s); gbrd.position.set(-0.2, 1.7, 1.5); gbrd.castShadow = false; g.add(gbrd);
  for (const gx of [-0.42, -0.12, 0.18, 0.42]) gauge(g, gx, 1.72, 1.53, cd);
  const cabtray = box(0.16, 0.08, 2.8, s); cabtray.position.set(0.0, 0.62, 1.3); cabtray.castShadow = false; g.add(cabtray);
  return 4.9;
}

function buildSeparator(g) {
  const m = clay(), s = steel();
  const R = 1.15, L = 5.0, H = 2.3;
  for (const sx of [-1.4, 1.4]) { const sad = box(0.7, H - R, R * 1.8, s); sad.position.set(sx, (H - R) / 2, 0); g.add(sad); } // saddles
  const v = cyl(R, R, L, m); v.rotation.z = Math.PI / 2; v.position.set(0, H, 0); g.add(v); // horizontal vessel
  const cL = cap(R, m); cL.rotation.z = Math.PI / 2; cL.position.set(-L / 2, H, 0); g.add(cL);
  const cR = cap(R, m); cR.rotation.z = -Math.PI / 2; cR.position.set(L / 2, H, 0); g.add(cR);
  const pm = pipe();
  const inlet = tube(0.3, 1.0, pm); inlet.position.set(-L / 2 + 0.8, H + R + 0.4, 0); g.add(inlet); g.add(fl(0.42, pm, -L / 2 + 0.8, H + R + 0.9, 0));
  const gas = tube(0.25, 1.0, pm); gas.position.set(L / 2 - 0.8, H + R + 0.4, 0); g.add(gas); g.add(fl(0.36, pm, L / 2 - 0.8, H + R + 0.9, 0));
  const liq = tube(0.22, 1.0, pm); liq.position.set(L / 2 - 0.5, H - R - 0.4, 0); g.add(liq);
  // level bridle on the front
  const br = tube(0.12, R * 1.6, pm); br.position.set(0.4, H, R + 0.05); g.add(br);
  for (const by of [H - R * 0.6, H, H + R * 0.6]) { const lg = mesh(new THREE.SphereGeometry(0.06, 8, 6), accentR()); lg.position.set(0.4, by, R + 0.14); lg.castShadow = false; g.add(lg); } // sight-glass taps
  ladder(g, -L / 2 - 0.2, R + 0.2, H + R, s);
  // fine detail: bolted heads, nozzle block valves, a relief valve, gauge and nameplate
  bolts(g, -L / 2 + 0.02, H, 0, R * 0.92, 12, 'x');
  bolts(g, L / 2 - 0.02, H, 0, R * 0.92, 12, 'x');
  valveBW(g, -L / 2 + 0.8, H + R + 0.9, 0, 0.16);     // inlet block valve
  valveBW(g, L / 2 - 0.8, H + R + 0.9, 0, 0.14);      // gas outlet valve
  const psv = tube(0.11, 0.6, s); psv.position.set(0.9, H + R + 0.5, 0); g.add(psv);
  const psvt = cyl(0.16, 0.1, 0.42, clayDark()); psvt.position.set(0.9, H + R + 0.85, 0); g.add(psvt); // relief valve
  gauge(g, -0.6, H + R + 0.2, R + 0.02, clayDark());
  const plate = box(0.5, 0.3, 0.02, s); plate.position.set(-1.4, H + 0.2, R + 0.02); plate.castShadow = false; g.add(plate); // nameplate
  return H + R + 1.2;
}
function fl(r, m, x, y, z) { const f = flange(r, m); f.position.set(x, y, z); return f; }

function buildWellhead(g) {
  const m = clay(), s = steel();
  const base = cyl(0.9, 0.9, 0.4, s); base.position.y = 0.2; g.add(base);
  let y = 0.4;
  const stack = [[0.55, 0.7], [0.5, 0.55], [0.6, 0.7], [0.5, 0.5], [0.45, 0.6]]; // valve bodies up the tree
  for (const [r, h] of stack) {
    const fg = flange(r + 0.12, s); fg.position.y = y + 0.06; g.add(fg);
    bolts(g, 0, y + 0.06, 0, r + 0.03, Math.max(6, Math.round(r * 14)), 'y'); // flange bolt-up on every spool
    const b = cyl(r, r, h, m); b.position.y = y + h / 2 + 0.12; g.add(b);
    y += h + 0.12;
  }
  // wing valves with red handwheels
  const rw = accentR();
  for (const [sx, hy] of [[1, 1.1], [-1, 1.7]]) {
    const wing = cyl(0.28, 0.28, 1.0, m); wing.rotation.z = Math.PI / 2; wing.position.set(sx * 0.7, hy, 0); g.add(wing);
    const wf = flange(0.34, s); wf.rotation.z = Math.PI / 2; wf.position.set(sx * 0.55, hy, 0); g.add(wf); bolts(g, sx * 0.55, hy, 0, 0.32, 6, 'x');
    const w = wheel(0.4, rw); w.position.set(sx * 1.25, hy, 0); w.rotation.y = Math.PI / 2; g.add(w);
  }
  // choke assembly + pressure gauge on the low (production) wing
  const choke = cyl(0.2, 0.16, 0.55, clayDark()); choke.position.set(1.15, 1.55, 0); g.add(choke);
  gauge(g, 0.0, 1.7, 0.62, clayDark());
  const cap0 = cap(0.45, m); cap0.position.y = y + 0.1; g.add(cap0);
  bolts(g, 0, y + 0.02, 0, 0.4, 8, 'y');
  const topw = wheel(0.5, rw); topw.position.y = y + 0.1; g.add(topw);
  return y + 1.0;
}

function buildCustodyMeter(g) {
  const m = clay(), s = steel();
  const L = 4.6, y = 1.3;
  const run = cyl(0.42, 0.42, L, m); run.rotation.z = Math.PI / 2; run.position.set(0, y, 0); g.add(run);
  const spool = cyl(0.52, 0.52, 1.0, clayDark()); spool.rotation.z = Math.PI / 2; spool.position.set(0, y, 0); g.add(spool); // meter body
  for (const sx of [-L / 2, -0.6, 0.6, L / 2]) { const f = flange(0.55, m); f.rotation.z = Math.PI / 2; f.position.set(sx, y, 0); g.add(f); }
  for (const sx of [-1.5, 1.5]) { const post = box(0.35, y - 0.4, 0.6, s); post.position.set(sx, (y - 0.4) / 2, 0); g.add(post); }
  const cab = box(0.7, 1.1, 0.5, cabinetM()); cab.position.set(0, 1.0, 1.3); g.add(cab); // flow computer cabinet
  const cabpost = tube(0.06, 1.0, s); cabpost.position.set(0, 0.5, 1.3); g.add(cabpost);
  const cabdoor = box(0.52, 0.9, 0.02, clayDark()); cabdoor.position.set(0, 1.0, 1.56); cabdoor.castShadow = false; g.add(cabdoor); // door face
  // fine detail: bolted meter flanges, pressure + temperature gauges, and signal conduit to the flow computer
  for (const sx of [-L / 2, -0.6, 0.6, L / 2]) bolts(g, sx, y, 0, 0.55, 10, 'x');
  gauge(g, -1.3, y + 0.55, 0, clayDark());   // static pressure
  gauge(g, 1.3, y + 0.55, 0, clayDark());    // temperature
  const conduit = tube(0.035, 1.2, s); conduit.position.set(0.35, 1.05, 0.85); conduit.rotation.x = 0.55; conduit.castShadow = false; g.add(conduit);
  return y + 1.0;
}

function buildFlare(g) {
  const m = clay(), s = steel();
  const H = 11;
  const stack = cyl(0.34, 0.3, H, m); stack.position.y = H / 2 + 0.5; g.add(stack);
  const tip = cyl(0.46, 0.34, 1.1, hot()); tip.position.y = H + 0.4; g.add(tip);
  // support derrick
  for (let a = 0; a < Math.PI * 2; a += Math.PI / 2) {
    const strut = tube(0.06, H * 0.7, s); strut.position.set(Math.cos(a) * 1.4, H * 0.35 + 0.5, Math.sin(a) * 1.4);
    strut.rotation.x = Math.sin(a) * 0.16; strut.rotation.z = -Math.cos(a) * 0.16; g.add(strut);
  }
  // knockout drum at base
  const ko = cyl(0.7, 0.7, 2.4, m); ko.rotation.z = Math.PI / 2; ko.position.set(2.0, 0.9, 0); g.add(ko);
  for (const sx of [1.2, 2.8]) { const sad = box(0.5, 0.5, 1.2, s); sad.position.set(sx, 0.35, 0); g.add(sad); }
  return H + 1.4;
}

function buildColumn(g, tall) {
  // TEG contactor / generic tall column + reboiler
  const m = clay(), s = steel();
  const R = 0.9, H = tall ? 7 : 5.5;
  const skirt = cyl(R, R + 0.05, 1.0, s); skirt.position.y = 0.5; g.add(skirt);
  const col = cyl(R, R, H, m); col.position.y = 0.9 + H / 2; g.add(col);
  const top = cap(R, m); top.position.y = 0.9 + H; g.add(top);
  platform(g, R, 0.9 + H * 0.62, s);
  ladder(g, -R - 0.4, 0, 0.9 + H * 0.62, s);
  const reb = cyl(0.6, 0.6, 2.2, m); reb.rotation.z = Math.PI / 2; reb.position.set(R + 1.8, 1.1, 0); g.add(reb);
  for (const sx of [R + 1.0, R + 2.6]) { const sad = box(0.4, 0.6, 1.0, s); sad.position.set(sx, 0.4, 0); g.add(sad); }
  const link = tube(0.16, 2.0, pipe()); link.rotation.z = Math.PI / 2; link.position.set(R + 0.9, 1.6, 0); g.add(link);
  // fine detail: bolted top head, a bolted manway, gauge, relief valve and a block valve on the reboiler line
  bolts(g, 0, 0.9 + H, 0, R * 0.82, 12, 'y');
  const manway = cyl(0.33, 0.33, 0.22, clayDark()); manway.rotation.z = Math.PI / 2; manway.position.set(R, 2.0, 0); g.add(manway);
  bolts(g, R + 0.06, 2.0, 0, 0.36, 8, 'x');
  gauge(g, R + 0.02, 3.4, 0.25, clayDark());
  const psv = tube(0.1, 0.5, s); psv.position.set(0.35, 0.9 + H + 0.25, 0); g.add(psv);
  const psvt = cyl(0.14, 0.09, 0.36, clayDark()); psvt.position.set(0.35, 0.9 + H + 0.55, 0); g.add(psvt);
  valveBW(g, R + 0.9, 1.9, 0, 0.14);
  return 0.9 + H + 1.0;
}

function buildAmine(g) {
  const m = clay(), s = steel();
  const specs = [[-1.4, 7, 0.9], [1.4, 6, 1.0]]; // contactor + regenerator
  for (const [x, H, R] of specs) {
    const skirt = cyl(R, R + 0.05, 1.0, s); skirt.position.set(x, 0.5, 0); g.add(skirt);
    const col = cyl(R, R, H, m); col.position.set(x, 0.9 + H / 2, 0); g.add(col);
    const top = cap(R, m); top.position.set(x, 0.9 + H, 0); g.add(top);
    platform(g, R, 0.9 + H * 0.6, s);
  }
  const cross = tube(0.18, 2.8, pipe()); cross.rotation.z = Math.PI / 2; cross.position.set(0, 5.5, 0); g.add(cross);
  ladder(g, -2.4, 0, 6.5, s);
  // fine detail: bolted heads on both columns, a crossover block valve and rich-amine gauges
  for (const [x, H, R] of specs) { bolts(g, x, 0.9 + H, 0, R * 0.82, 12, 'y'); gauge(g, x + R + 0.02, 0.9 + H * 0.6, 0.2, clayDark()); }
  valveBW(g, 0, 5.5, 0, 0.15);
  return 8.0;
}

function buildNGL(g) {
  const m = clay(), s = steel();
  const cbx = box(2.2, 5.2, 2.2, clayDark()); cbx.position.set(-1.3, 2.9, 0); g.add(cbx); // insulated cold box
  const col = cyl(0.7, 0.7, 6, m); col.position.set(1.3, 3.3, 0); g.add(col);
  const top = cap(0.7, m); top.position.set(1.3, 6.3, 0); g.add(top);
  platform(g, 0.7, 4.2, s);
  const link = tube(0.16, 2.4, pipe()); link.rotation.z = Math.PI / 2; link.position.set(0, 5.5, 0); g.add(link);
  // fine detail: insulation banding on the cold box, a bolted column head and a demethaniser gauge
  for (let by = 1.0; by < 5.0; by += 1.1) { const band = box(2.26, 0.06, 2.26, steel()); band.position.set(-1.3, by, 0); band.castShadow = false; g.add(band); }
  bolts(g, 1.3, 6.3, 0, 0.6, 10, 'y');
  gauge(g, 1.3 + 0.72, 3.6, 0.2, clayDark());
  return 6.8;
}

function buildGathering(g) {
  const m = clay(), s = steel();
  const L = 6.4, y = 0.8;
  const line = cyl(0.42, 0.42, L, pipe()); line.rotation.x = Math.PI / 2; line.position.set(0, y, 0); g.add(line);
  for (const sz of [-L / 2 + 0.6, 0, L / 2 - 0.6]) { const post = box(0.4, y - 0.2, 0.4, s); post.position.set(0, (y - 0.2) / 2, sz); g.add(post); const sad = cyl(0.5, 0.5, 0.3, s); sad.rotation.x = Math.PI / 2; sad.position.set(0, y, sz); g.add(sad); }
  // gate valve mid-span with a red handwheel
  const vb = box(0.7, 0.9, 0.7, m); vb.position.set(0, y + 0.2, 0); g.add(vb);
  const stem = tube(0.08, 0.8, s); stem.position.set(0, y + 1.0, 0); g.add(stem);
  const w = wheel(0.4, accentR()); w.position.set(0, y + 1.4, 0); g.add(w);
  // fine detail: bolted pipeline flanges either side of the gate valve, plus a line pressure gauge
  bolts(g, 0, y, 0.42, 0.5, 8, 'z');
  bolts(g, 0, y, -0.42, 0.5, 8, 'z');
  gauge(g, 0.45, y + 0.45, 0.4, clayDark());
  return y + 1.8;
}

function buildVessel(g) {
  const m = clay(), s = steel();
  const R = 1.0, H = 4;
  const skirt = cyl(R, R + 0.05, 1.0, s); skirt.position.y = 0.5; g.add(skirt);
  const v = cyl(R, R, H, m); v.position.y = 0.9 + H / 2; g.add(v);
  const top = cap(R, m); top.position.y = 0.9 + H; g.add(top);
  // fine detail: bolted top head, a nozzle + block valve, and a gauge
  bolts(g, 0, 0.9 + H, 0, R * 0.8, 10, 'y');
  const noz = tube(0.16, 0.7, pipe()); noz.rotation.z = Math.PI / 2; noz.position.set(R + 0.35, 0.9 + H * 0.7, 0); g.add(noz);
  valveBW(g, R + 0.7, 0.9 + H * 0.7, 0, 0.12);
  gauge(g, 0, 0.9 + H * 0.45, R + 0.02, clayDark());
  return 0.9 + H + 0.8;
}

const BUILDERS = {
  compressor: buildCompressor, separator: buildSeparator, wellhead: buildWellhead,
  custody_meter: buildCustodyMeter, flare: buildFlare,
  dehydration_teg: g => buildColumn(g, true), acid_gas_removal_amine: buildAmine,
  ngl_recovery: buildNGL, gathering_segment: buildGathering,
};

// ---------------- labels ----------------
function labelSprite(text) {
  const pad = 12, fs = 44, dpr = 2, rad = 9, bw = 2;
  const c = document.createElement("canvas"), g = c.getContext("2d");
  const font = `400 ${fs}px -apple-system,Segoe UI,Roboto,sans-serif`;
  g.font = font;
  const w = g.measureText(text).width + pad * 2, h = fs + pad * 2;
  c.width = w * dpr; c.height = h * dpr; g.scale(dpr, dpr);
  g.font = font;
  // translucent, glassy grey tag: mostly see-through fill with a slightly darker grey border.
  const x0 = bw / 2, y0 = bw / 2, ww = w - bw, hh = h - bw;
  g.beginPath();
  if (g.roundRect) g.roundRect(x0, y0, ww, hh, rad); else g.rect(x0, y0, ww, hh);
  g.fillStyle = "rgba(122,130,140,0.20)"; g.fill();                 // translucent grey box
  g.lineWidth = bw; g.strokeStyle = "rgba(84,92,102,0.78)"; g.stroke(); // slightly darker grey boundary
  g.textBaseline = "middle";
  g.shadowColor = "rgba(10,14,18,0.45)"; g.shadowBlur = 3; g.shadowOffsetY = 0.5; // subtle legibility only
  g.fillStyle = "rgba(231,235,239,0.96)";                           // asset (clay) grey text
  g.fillText(text, pad, h / 2 + 1);
  const tex = new THREE.CanvasTexture(c); tex.minFilter = THREE.LinearFilter;
  const sp = new THREE.Sprite(new THREE.SpriteMaterial({ map: tex, depthTest: false, depthWrite: false, opacity: 0.9, transparent: true }));
  sp.scale.set(w / 44 * 0.5, h / 44 * 0.5, 1);
  sp.renderOrder = 10;
  return sp;
}

// Build a typed equipment model. Returns a Group whose userData carries the asset.
function equip(a) {
  const g = new THREE.Group();
  const t = a.equipment_type || "";
  const builder = BUILDERS[t] || buildVessel;
  const top = builder(g);
  // status colour lives only on the tag now (a levitating ball beside the label); the equipment
  // itself is left uncoloured/true-to-life (no coloured glow lamp).
  const col = COL[a.pin_colour] || COL.green;
  // label with the status ball beside it
  const val = a.latest_vibration != null ? " · " + Number(a.latest_vibration).toFixed(1) : "";
  const sp = labelSprite(a.asset_id + val); sp.position.y = top + 1.2; g.add(sp);
  // status ball: a separate glowing sphere levitating just outside the tag; updateTagBalls() keeps
  // it beside the camera-facing label from any angle. Always-on-top (depthTest off) like the tag.
  const ballMat = new THREE.MeshStandardMaterial({ color: col, emissive: col, emissiveIntensity: 0.95, metalness: 0.1, roughness: 0.35, depthTest: false, depthWrite: false });
  const ball = mesh(new THREE.SphereGeometry(0.32, 18, 14), ballMat); ball.castShadow = false; ball.renderOrder = 11;
  ball.position.set(0, top + 1.2, 0); g.add(ball);
  tagBalls.push({ sprite: sp, ball });
  g.userData.asset = a;
  g.traverse(o => { if (o.isMesh || o.isSprite) o.userData.asset = a; });
  return g;
}

// ---------------- renderer / scene ----------------
function updateCam() {
  phi = Math.max(phiMin, Math.min(phiMax, phi));
  radius = Math.max(10, Math.min(maxRadius, radius));
  camera.position.set(
    target.x + radius * Math.sin(phi) * Math.cos(theta),
    target.y + radius * Math.cos(phi),
    target.z + radius * Math.sin(phi) * Math.sin(theta));
  camera.lookAt(target);
}

// ---------------- scene environment (indoor plant hall vs outdoor field) ----------------
// Equipment that normally lives inside a building at the plant.
const INDOOR = new Set(["compressor", "separator", "dehydration_teg", "acid_gas_removal_amine", "ngl_recovery"]);
function wallMat() { return new THREE.MeshStandardMaterial({ color: 0xdde3ea, roughness: 0.97 }); }
function ceilMat() { return new THREE.MeshStandardMaterial({ color: 0xd5dce4, roughness: 0.98 }); }
function foliageMat() { return new THREE.MeshStandardMaterial({ color: 0x6aa64a, roughness: 0.92 }); } // leaf green
function trunkMat() { return new THREE.MeshStandardMaterial({ color: 0x6b5236, roughness: 0.9 }); }       // brown bark
function hillMat() { return new THREE.MeshStandardMaterial({ color: 0xdce2e9, roughness: 1 }); }
function waterMat() { return new THREE.MeshStandardMaterial({ color: 0xcfe1ee, roughness: 0.35, metalness: 0.1 }); } // faint light blue
function _rng(seed) { let a = seed | 0; return function () { a = a + 0x6D2B79F5 | 0; let t = Math.imul(a ^ a >>> 15, 1 | a); t = t + Math.imul(t ^ t >>> 7, 61 | t) ^ t; return ((t ^ t >>> 14) >>> 0) / 4294967296; }; }

function clearEnv() {
  if (!envGroup) return;
  envGroup.children.slice().forEach(c => { c.traverse(o => { if (o.geometry) o.geometry.dispose(); if (o.material) o.material.dispose && o.material.dispose(); }); envGroup.remove(c); });
}

// Switch the whole scene between a bright outdoor daylight look and a dim indoor hall (dark
// surroundings so the per-asset spotlights make the equipment pop, like the reference render).
function setMood(indoor) {
  indoorMode = indoor;
  if (indoor) {
    // Near-black hall: only the per-asset ceiling spotlights light the equipment; everything else
    // sinks into the dark and fog so the nearest asset pops like the reference clay render.
    // dark GREY (not black): between black and the mid-grey the equipment is modelled in, so the
    // surroundings read as a real plant hall and the background equipment stays faintly visible,
    // while the per-asset spotlights still make the nearest asset the clear hero.
    scene.background.set(0x33383f); scene.fog.color.set(0x33383f); scene.fog.near = 30; scene.fog.far = 134;
    hemiLight.intensity = 0.30; keyLight.intensity = 0.19; fillLight.intensity = 0.09;
    // polished-tile floor: low roughness + a touch of metalness gives a faint specular sheen so the
    // spotlights and equipment glint off the floor (a very little reflection), like the reference.
    groundMesh.material.color.set(0x40454d); groundMesh.material.roughness = 0.28; groundMesh.material.metalness = 0.34; gridHelp.visible = false;
    phiMin = 1.34; phiMax = 1.53;   // eye-level only: a slight up/down tilt, full horizontal orbit
  } else {
    scene.background.set(0xeef1f5); scene.fog.color.set(0xeef1f5); scene.fog.near = 130; scene.fog.far = 320;
    hemiLight.intensity = 0.85; keyLight.intensity = 1.05; fillLight.intensity = 0.35;
    groundMesh.material.color.set(0xe7ebf0); groundMesh.material.roughness = 1; groundMesh.material.metalness = 0; gridHelp.visible = true;
    phiMin = 0.25; phiMax = 1.45;
  }
}

function buildIndoor(span, placed) {
  const R = span * 0.62 + 13, H = 14;
  // Dark hall: walls single-sided facing inward (vanish when you orbit outside so they never block
  // the view), a tiled ceiling, dark floor, structural columns. The only real light comes from a
  // bright ceiling panel + spotlight directly above each asset, so the equipment reads as a lit
  // island in near-black surroundings (like the reference clay render).
  const wm = new THREE.MeshStandardMaterial({ color: 0x3a3f47, roughness: 0.97 });
  [[0, -R, 0], [0, R, Math.PI], [-R, 0, Math.PI / 2], [R, 0, -Math.PI / 2]].forEach(([x, z, ry]) => {
    const w = new THREE.Mesh(new THREE.PlaneGeometry(R * 2, H), wm);
    w.position.set(x, H / 2, z); w.rotation.y = ry; w.receiveShadow = true; envGroup.add(w); indoorShell.push(w);
  });
  const ceil = new THREE.Mesh(new THREE.PlaneGeometry(R * 2, R * 2), new THREE.MeshStandardMaterial({ color: 0x424852, roughness: 0.98 }));
  ceil.rotation.x = Math.PI / 2; ceil.position.y = H; envGroup.add(ceil); indoorShell.push(ceil);
  const cg = new THREE.GridHelper(R * 2, Math.max(12, Math.round(R)), 0x646b78, 0x59606c); cg.position.y = H - 0.05; envGroup.add(cg); indoorShell.push(cg); // false-ceiling tile grid (clear, tile-sized seams)
  const fg = new THREE.GridHelper(R * 2, Math.max(8, Math.round(R / 2.4)), 0x4d535d, 0x373d45); fg.position.y = 0.02; envGroup.add(fg);
  const cm = new THREE.MeshStandardMaterial({ color: 0x484e58, roughness: 0.9, metalness: 0.2 });
  for (const sx of [-1, 1]) for (const sz of [-1, 1]) { const c = box(0.6, H, 0.6, cm); c.position.set(sx * (R - 0.6), H / 2, sz * (R - 0.6)); c.castShadow = false; envGroup.add(c); }
  // Per-asset: a bright emissive ceiling panel + a shadowing spotlight aimed straight down at the
  // equipment. Registered in assetSpots so updateSpots() can make the nearest one the lit hero and
  // dim the rest to near-dark as the camera walks around.
  (placed || []).forEach(([px, pz]) => {
    const panel = new THREE.Mesh(new THREE.PlaneGeometry(7.0, 4.4),
      new THREE.MeshStandardMaterial({ color: 0xffffff, emissive: 0xeef4fb, emissiveIntensity: 0.12, roughness: 0.5 }));
    panel.rotation.x = Math.PI / 2; panel.position.set(px, H - 0.12, pz); panel.castShadow = false; envGroup.add(panel);
    const spot = new THREE.SpotLight(0xf3f7ff, 0.5, 90, 0.62, 0.45, 1.0);
    spot.position.set(px + 2.4, H - 0.6, pz + 3.2);
    spot.target.position.set(px, 1.1, pz);
    spot.shadow.mapSize.set(1024, 1024); spot.shadow.bias = -0.0006;
    spot.shadow.camera.near = 2; spot.shadow.camera.far = 46;
    envGroup.add(spot); envGroup.add(spot.target);
    assetSpots.push({ spot, panel, x: px, z: pz });
  });
  // Real 'other equipment in the hall': a scatter of detailed procedural skids around the floor,
  // dim (no spotlight, no status ball) so the plant reads as populated but recedes into the grey.
  buildBackgroundEquipment(R, span);
}

// Scatter a handful of fully-modelled equipment skids around the hall floor as "other plant",
// beyond the foreground grid but inside the walls. They are unlit background (shadows off for perf)
// with a glowing stack-light so they show as detailed silhouettes in the grey gloom.
function buildBackgroundEquipment(R, span) {
  const r = _rng(211 + Math.round(span * 7));
  const kinds = [buildCompressor, buildSeparator, g => buildColumn(g, false), buildVessel, buildCustodyMeter, buildAmine];
  const n = 6;
  for (let i = 0; i < n; i++) {
    const ang = (i / n) * Math.PI * 2 + (r() - 0.5) * 0.7;
    const rad = span * 0.72 + R * 0.12 + r() * (R * 0.14);
    const sub = new THREE.Group();
    (kinds[Math.floor(r() * kinds.length)])(sub);
    sub.position.set(Math.cos(ang) * rad, 0, Math.sin(ang) * rad);
    sub.rotation.y = r() * Math.PI * 2;
    // darken the background pieces (2 notches) so they recede and don't pull the eye off the
    // foreground assets. Collect unique materials first so shared ones aren't darkened twice.
    const mats = new Set();
    sub.traverse(o => { if (o.isMesh) { o.castShadow = false; o.receiveShadow = false; if (o.material) mats.add(o.material); } });
    mats.forEach(mt => { if (mt.color) mt.color.multiplyScalar(0.45); });
    envGroup.add(sub);   // no status ball / beacon on background equipment
  }
}

// Make the asset nearest the camera the lit "hero": full spotlight + bright ceiling panel; every
// other asset dims toward dark. Only the hero casts a shadow (cheap + dramatic). Smoothly lerped so
// the light glides as the operator walks around the floor.
function updateSpots() {
  if (!indoorMode || !assetSpots.length) return;
  let best = 0, bd = Infinity;
  for (let i = 0; i < assetSpots.length; i++) {
    const s = assetSpots[i], dx = camera.position.x - s.x, dz = camera.position.z - s.z, d = dx * dx + dz * dz;
    if (d < bd) { bd = d; best = i; }
  }
  if (best !== nearestIdx) { nearestIdx = best; assetSpots.forEach((s, i) => { s.spot.castShadow = (i === best); }); }
  assetSpots.forEach((s, i) => {
    const hero = i === best;
    s.spot.intensity += ((hero ? 46 : 0.6) - s.spot.intensity) * 0.09;
    s.panel.material.emissiveIntensity += ((hero ? 2.2 : 0.10) - s.panel.material.emissiveIntensity) * 0.09;
  });
}

// Keep each status ball floating just to the left of its (always camera-facing) tag. Groups aren't
// rotated, so the world "camera-right" vector doubles as the local offset direction.
function updateTagBalls() {
  if (!tagBalls.length) return;
  camera.updateMatrixWorld();
  _camRight.setFromMatrixColumn(camera.matrixWorld, 0).normalize();
  for (const { sprite, ball } of tagBalls) {
    const off = sprite.scale.x / 2 + 0.32 + 0.30; // half tag width + ball radius + small gap
    ball.position.set(sprite.position.x - _camRight.x * off, sprite.position.y - _camRight.y * off, sprite.position.z - _camRight.z * off);
  }
}

function tree(x, z, h, fm, tm) {
  const g = new THREE.Group();
  const fm2 = fm.clone(); fm2.color.multiplyScalar(0.80);   // deeper green for shaded depth in the crown
  // short brown trunk that forks into a few visible branches climbing into the crown
  const trunkH = h * 0.5;
  const trunk = cyl(0.08 * h, 0.14 * h, trunkH, tm, 12); trunk.position.y = trunkH / 2; g.add(trunk);
  [[0.16 * h, trunkH * 0.82, 0, 0.5, 0], [-0.15 * h, trunkH * 0.94, 0.08 * h, -0.5, 0.28],
  [0.03 * h, trunkH * 1.05, -0.13 * h, 0.12, -0.5]].forEach(([bx, by, bz, rz, rx]) => {
    const br = cyl(0.035 * h, 0.07 * h, 0.5 * h, tm, 8); br.position.set(bx, by, bz); br.rotation.z = rz; br.rotation.x = rx; g.add(br);
  });
  // broad, dense, rounded crown: overlapping high-res green icosahedra (detail 2 = many facets)
  // arranged into a wide dome like a mature deciduous tree, with darker greens tucked underneath.
  const cy = trunkH + h * 0.4, cr = 0.6 * h;
  [[0, cy + 0.18 * h, 0, 0.64 * h, 0], [0.52 * cr, cy + 0.02 * h, 0.30 * cr, 0.5 * h, 1], [-0.55 * cr, cy - 0.02 * h, -0.26 * cr, 0.5 * h, 0],
  [0.30 * cr, cy + 0.24 * h, -0.44 * cr, 0.44 * h, 1], [-0.28 * cr, cy + 0.22 * h, 0.46 * cr, 0.44 * h, 0], [0.10 * cr, cy - 0.20 * h, 0.52 * cr, 0.46 * h, 1],
  [0.16 * cr, cy - 0.18 * h, -0.54 * cr, 0.42 * h, 0], [-0.5 * cr, cy + 0.14 * h, 0.22 * cr, 0.4 * h, 1], [0.5 * cr, cy + 0.16 * h, -0.12 * cr, 0.4 * h, 0]].forEach(([px, py, pz, pr, dark]) => {
    const f = mesh(new THREE.IcosahedronGeometry(pr, 2), dark ? fm2 : fm); f.position.set(px, py, pz); f.scale.y = 0.94; g.add(f);
  });
  g.position.set(x, 0, z); return g;
}

function river(span) {
  const shape = new THREE.Shape();
  const L = span * 3.6, w = span * 0.24, pts = 30, amp = span * 0.6;
  const top = [], bot = [];
  for (let i = 0; i <= pts; i++) { const t = i / pts; const x = -L / 2 + t * L; const cz = Math.sin(t * Math.PI * 1.7) * amp; top.push([x, cz - w / 2]); bot.push([x, cz + w / 2]); }
  shape.moveTo(top[0][0], top[0][1]);
  for (let i = 1; i < top.length; i++) shape.lineTo(top[i][0], top[i][1]);
  for (let i = bot.length - 1; i >= 0; i--) shape.lineTo(bot[i][0], bot[i][1]);
  const m = new THREE.Mesh(new THREE.ShapeGeometry(shape), waterMat());
  m.rotation.x = -Math.PI / 2; m.rotation.z = 0.35; m.position.set(span * 0.5, 0.05, -span * 0.9);
  return m;
}

function buildOutdoor(span) {
  const E = span * 0.5, r = _rng(97 + Math.round(span * 13));
  const fm = foliageMat(), tm = trunkMat();
  for (let i = 0, n = 15; i < n; i++) {
    const ang = (i / n) * Math.PI * 2 + (r() - 0.5) * 0.5;
    const rr = E + 7 + r() * (span * 0.85);
    envGroup.add(tree(Math.cos(ang) * rr, Math.sin(ang) * rr, 2.2 + r() * 2.0, fm, tm));
  }
  const hm = hillMat();
  for (let i = 0, n = 6; i < n; i++) {
    const ang = (i / n) * Math.PI * 2 + 0.5;
    const rr = E + span * 1.7 + r() * span;
    const w = span * (0.6 + r() * 0.7), hh = 3.5 + r() * 5;
    const hill = new THREE.Mesh(new THREE.SphereGeometry(1, 18, 10), hm);
    hill.scale.set(w, hh, w); hill.position.set(Math.cos(ang) * rr, -hh * 0.55, Math.sin(ang) * rr); hill.receiveShadow = true; envGroup.add(hill);
  }
  envGroup.add(river(span));
}

function buildEnvironment(assets, span, placed) {
  clearEnv();
  assetSpots = []; nearestIdx = -1; indoorShell = []; facilityStage = false;
  const indoor = false;  // light mode: always use the bright outdoor environment (no dark indoor hall)
  setMood(indoor);
  if (indoor) buildIndoor(span, placed); else buildOutdoor(span);
}

function initRenderer() {
  renderer = new THREE.WebGLRenderer({ antialias: true });
  renderer.setPixelRatio(Math.min(2, window.devicePixelRatio || 1));
  renderer.shadowMap.enabled = true; renderer.shadowMap.type = THREE.PCFSoftShadowMap;
  scene = new THREE.Scene(); scene.background = new THREE.Color(0xeef1f5);
  scene.fog = new THREE.Fog(0xeef1f5, 130, 320);
  camera = new THREE.PerspectiveCamera(50, 1, 0.1, 2000);
  hemiLight = new THREE.HemisphereLight(0xffffff, 0xc4ccd6, 0.85); scene.add(hemiLight);
  keyLight = new THREE.DirectionalLight(0xffffff, 1.05); keyLight.position.set(34, 54, 26);
  keyLight.castShadow = true; keyLight.shadow.mapSize.set(2048, 2048);
  const sc = keyLight.shadow.camera; sc.left = -70; sc.right = 70; sc.top = 70; sc.bottom = -70; sc.near = 1; sc.far = 260; keyLight.shadow.bias = -0.0004;
  scene.add(keyLight);
  fillLight = new THREE.DirectionalLight(0xffffff, 0.35); fillLight.position.set(-30, 24, -20); scene.add(fillLight);
  gridHelp = new THREE.GridHelper(400, 80, 0xc3cdd8, 0xdbe2ea); gridHelp.position.y = 0.01; scene.add(gridHelp);
  groundMesh = new THREE.Mesh(new THREE.PlaneGeometry(400, 400), new THREE.MeshStandardMaterial({ color: 0xe7ebf0, roughness: 1 }));
  groundMesh.rotation.x = -Math.PI / 2; groundMesh.receiveShadow = true; scene.add(groundMesh);
  assetGroup = new THREE.Group(); scene.add(assetGroup);
  envGroup = new THREE.Group(); scene.add(envGroup);
  bindControls();
}

function bindControls() {
  const el = renderer.domElement;
  el.style.cursor = "grab";
  el.addEventListener("pointerdown", e => { drag = true; lx = downX = e.clientX; ly = downY = e.clientY; el.style.cursor = "grabbing"; });
  window.addEventListener("pointerup", () => { drag = false; if (renderer) renderer.domElement.style.cursor = "grab"; });
  window.addEventListener("pointermove", e => {
    if (drag) { theta += (e.clientX - lx) * 0.006; phi -= (e.clientY - ly) * 0.006; lx = e.clientX; ly = e.clientY; updateCam(); }
  });
  el.addEventListener("wheel", e => {
    e.preventDefault();
    const dir = Math.sign(e.deltaY);
    if (dir < 0) overscroll = 0;   // zooming back in cancels any pending step-up to the map
    // At the facility-view stop, keep zooming out (a deliberate extra push) to step up to the map.
    if (dir > 0 && radius >= maxRadius - 1e-3) {
      overscroll += 1;
      if (overscroll >= 8 && onZoomOut) { const cb = onZoomOut; onZoomOut = null; cb(); return; }
      return; // hold at the stop; don't grow radius past the clamp
    }
    radius *= (1 + dir * 0.08);
    applyStage();
    updateCam();
  }, { passive: false });
  el.addEventListener("pointermove", onHover);
  el.addEventListener("pointerleave", () => { if (tipEl) tipEl.style.display = "none"; hovered = null; });
  el.addEventListener("click", onClick);
}

// Between the asset scene and the map: once zoomed out past ~1.7x the initial distance, drop the
// interior shell (walls/ceiling) so the whole facility reads from outside, and relax the vertical
// tilt so the operator can look down over the site. Reverses when zooming back in.
function applyStage() {
  const want = radius >= initRadius * 1.5;
  if (want === facilityStage) return;
  facilityStage = want;
  if (indoorMode) {
    indoorShell.forEach(m => { m.visible = !want; });
    phiMax = want ? 1.28 : 1.53;
  }
  if (onStage) onStage(want ? "facility" : "asset");
}

function pickAt(e) {
  const r = renderer.domElement.getBoundingClientRect();
  mouse.x = ((e.clientX - r.left) / r.width) * 2 - 1;
  mouse.y = -((e.clientY - r.top) / r.height) * 2 + 1;
  ray.setFromCamera(mouse, camera);
  const hits = ray.intersectObjects(assetGroup.children, true);
  for (const h of hits) { let o = h.object; while (o && !o.userData.asset) o = o.parent; if (o) return o; }
  return null;
}

function onHover(e) {
  const o = pickAt(e);
  renderer.domElement.style.cursor = o ? "pointer" : (drag ? "grabbing" : "grab");
  if (o && o.userData.asset) {
    const a = o.userData.asset;
    tipEl.style.display = "block";
    tipEl.innerHTML = `<b>${a.asset_id}</b><br>${(a.equipment_type || "").replace(/_/g, " ")} · ${a.pin_colour || ""}` + (a.latest_vibration != null ? `<br>vibration ${Number(a.latest_vibration).toFixed(2)} mm/s` : "");
    const r = container.getBoundingClientRect();
    tipEl.style.left = Math.min(r.width - 180, e.clientX - r.left + 12) + "px";
    tipEl.style.top = (e.clientY - r.top + 12) + "px";
    hovered = o;
  } else if (hovered) { tipEl.style.display = "none"; hovered = null; }
}

function onClick(e) {
  if (Math.hypot(e.clientX - downX, e.clientY - downY) > 5) return;  // that was a drag-to-orbit, not a click
  const o = pickAt(e);
  if (o && o.userData.asset) { if (onPick) onPick(o.userData.asset.asset_id); }
  else if (onEmpty) onEmpty();   // clicked empty ground / sky / environment
}

function clearAssets() {
  tagBalls = [];
  assetGroup.children.slice().forEach(c => {
    c.traverse(o => { if (o.geometry) o.geometry.dispose(); if (o.material) { if (o.material.map) o.material.map.dispose(); o.material.dispose(); } });
    assetGroup.remove(c);
  });
}

function animate() { raf = requestAnimationFrame(animate); updateSpots(); updateTagBalls(); renderer.render(scene, camera); }

function resize() {
  if (!container || !renderer) return;
  const w = container.clientWidth, h = container.clientHeight;
  renderer.setSize(w, h, false); camera.aspect = w / Math.max(1, h); camera.updateProjectionMatrix();
}

const Twin3D = {
  load(containerSel, assets, onPickCb, opts) {
    container = document.querySelector(containerSel);
    if (!container) return;
    onPick = onPickCb;
    onZoomOut = (opts && opts.onZoomOut) || null;
    onStage = (opts && opts.onStage) || null;
    onEmpty = (opts && opts.onEmpty) || null;
    if (!renderer) {
      initRenderer();
      container.appendChild(renderer.domElement);
      tipEl = document.createElement("div"); tipEl.className = "tip3d"; container.appendChild(tipEl);
      window.addEventListener("resize", resize);
      animate();
    } else if (renderer.domElement.parentNode !== container) {
      container.appendChild(renderer.domElement); container.appendChild(tipEl);
    }
    renderer.domElement.style.width = "100%"; renderer.domElement.style.height = "100%";
    clearAssets();
    const n = assets.length, cols = Math.max(1, Math.ceil(Math.sqrt(n))), gap = 11;
    const rows = Math.max(1, Math.ceil(n / cols));
    const placed = [];
    assets.forEach((a, i) => {
      const c = i % cols, r = Math.floor(i / cols);
      const g = equip(a);
      const px = (c - (cols - 1) / 2) * gap, pz = (r - (rows - 1) / 2) * gap;
      g.position.set(px, 0, pz);
      assetGroup.add(g);
      placed.push([px, pz]);
    });
    const indoor = false;  // light mode: outdoor framing for every scene
    buildEnvironment(assets, Math.max(cols, rows) * gap, placed);
    // Indoor opens at standing eye level (ceiling visible up top, floor receding), locked to the
    // narrow tilt band; outdoor keeps the higher 3/4 survey view.
    target.set(0, indoor ? 2 : 3, 0);
    radius = Math.max(indoor ? 26 : 24, Math.max(cols, rows) * gap * 1.05); theta = 0.85; phi = indoor ? 1.45 : 1.0;
    initRadius = radius; facilityStage = false;   // zoom LOD baseline for this scene
    maxRadius = radius * 1.75; overscroll = 0;    // hard stop at the facility view
    if (onStage) onStage("asset");                // reset the stage hint for the fresh scene
    updateCam(); resize();
  },
  dispose() {
    if (raf) cancelAnimationFrame(raf); raf = null;
    if (renderer) { renderer.dispose(); if (renderer.domElement.parentNode) renderer.domElement.parentNode.removeChild(renderer.domElement); }
    renderer = null; scene = null; camera = null; assetGroup = null; envGroup = null;
  }
};

window.Twin3D = Twin3D;
