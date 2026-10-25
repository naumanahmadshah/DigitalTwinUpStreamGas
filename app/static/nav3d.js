// Real 3D scene for the Asset Twin Navigator. Each asset becomes a recognisable, procedurally-built
// piece of process equipment (compressor package, horizontal separator, wellhead tree, custody meter
// run, flare, TEG/amine columns, NGL cold box, gathering segment) rendered in a clean matte "clay"
// style with soft shadows, sat on a structural skid. Alarm state shows on a stack-light beacon so the
// equipment itself stays true-to-life. Three.js is vendored locally (single-command portable).
// Exposes window.Twin3D.

import * as THREE from "./vendor/three.module.js";
import { GLTFLoader } from "./vendor/GLTFLoader.js";
import { MeshoptDecoder } from "./vendor/libs/meshopt_decoder.module.js";
import { EffectComposer } from "./vendor/postprocessing/EffectComposer.js";
import { RenderPass } from "./vendor/postprocessing/RenderPass.js";
import { ShaderPass } from "./vendor/postprocessing/ShaderPass.js";
import { OutputPass } from "./vendor/postprocessing/OutputPass.js";

// ---------------- graphic-novel site view (Meshy models) ----------------
// Every equipment type and every piece of scenery is a hand-painted, ink-outlined model (static/models,
// made with Meshy from one style reference). Like a comic panel, the scenery renders in greyscale and
// every asset in full colour, so the equipment stands out; the site's ute is painted in the site's
// overall status colour. Types without a model fall back to the procedural builders further down.
// type -> [model file, real-world size in metres, which dimension that size is: "len" footprint or "h" height]
const MODELS = {
  compressor: ["compressor", 12, "len"], separator: ["separator", 10, "len"], wellhead: ["wellhead", 3.2, "h"],
  esp: ["esp", 4.5, "len"], gas_lift: ["gas_lift", 3.6, "len"], gathering_line: ["gathering", 7, "len"],
  teg_dehydration: ["teg", 12, "h"], amine: ["amine", 14, "h"], ngl_stab: ["ngl", 14, "h"], utilities: ["utilities", 12, "len"],
  flare: ["flare", 22, "h"], fire_gas: ["fire_gas", 3.8, "h"], metering: ["metering", 8, "len"],
};
const PROPS = {
  gum_tree: [11, "h"], scrub: [3.2, "len"], rocks: [4.5, "len"], mesa: [70, "len"], site_office: [7, "len"], ute: [5.6, "len"],
  pipe_rack: [12, "len"], tanks: [10, "len"], light_tower: [7, "h"], windsock: [6, "h"], fence: [3, "len"],
  worker_1: [1.25, "h"], worker_2: [1.8, "h"], worker_3: [1.8, "h"], compressor_old: [11, "len"],
};
// offshore scenery: the wellhead platform's jacket, deck kit, and what floats or swims around it
const SEA_PROPS = {
  platform_jacket: [20, "h"], deck_rail: [3, "len"], living_quarters: [10, "len"], helideck: [14, "len"], pedestal_crane: [16, "h"],
  deck_containers: [8, "len"], supply_vessel: [64, "len"], crew_boat: [19, "len"], buoy: [4.5, "h"], lng_carrier: [190, "len"],
  distant_platform: [70, "len"], whale_tail: [7, "h"], seagull: [1.6, "len"],
  worker_1: [1.25, "h"], worker_2: [1.8, "h"], worker_3: [1.8, "h"],
};
const DECK_H = 18;
// One wind per site: the windsock points downwind and the flare flame and smoke lean the same way.
const WIND = new THREE.Vector3(1, 0, 0);   // the platform deck stands this high above the sea (the deck top is y = 0, like the ground on land)
const gltf = new GLTFLoader(); gltf.setMeshoptDecoder(MeshoptDecoder);
const _models = {};   // file -> Promise<normalised template (Group) | null>
const _props = {};
function loadProps(sea) {   // every scenery model for a land or a sea site, as { name: template }
  const set = sea ? SEA_PROPS : PROPS, key = sea ? "sea" : "land";
  if (!_props[key]) { const k = Object.keys(set); _props[key] = Promise.all(k.map(n => loadModel(n, set[n][0], set[n][1]))).then(t => Object.fromEntries(k.map((n, i) => [n, t[i]]))); }
  return _props[key];
}
// Warm the cache in the background a few seconds after the app opens, so the first site view is instant.
function prefetch() {
  if (loadSeq) return;   // a site is already loading: don't compete with it for bandwidth
  loadProps(); loadEngineer(); Object.values(MODELS).forEach(m => loadModel(m[0], m[1], m[2])); setTimeout(() => loadProps(true), 4000);
}
const _idle = window.requestIdleCallback || (f => setTimeout(f, 1));
setTimeout(() => _idle(prefetch, { timeout: 5000 }), 3000);
function loadModel(file, size, dim) {
  if (!_models[file]) _models[file] = new Promise(ok => gltf.load(`models/${file}.glb?v=2`, g => {
    const o = g.scene, b = new THREE.Box3().setFromObject(o), z = b.getSize(new THREE.Vector3());
    o.scale.multiplyScalar(size / Math.max(1e-6, dim === "h" ? z.y : Math.max(z.x, z.z)));
    const b2 = new THREE.Box3().setFromObject(o), c = b2.getCenter(new THREE.Vector3());
    o.position.set(-c.x, -b2.min.y, -c.z);              // centred on its footprint, standing on the ground
    const t = new THREE.Group(); t.add(o); ok(t);
  }, undefined, () => ok(null)));
  return _models[file];
}
// One ink material per source material: the painted texture, lit softly; uSat 0 = greyscale, 1 = colour.
function inkMaterial(src, sat) {
  const m = new THREE.MeshStandardMaterial({ map: src.map || null, color: src.map ? 0xffffff : (src.color || new THREE.Color(0xbbbbbb)),
                                            roughness: 1, metalness: 0, side: THREE.DoubleSide });
  m.userData.sat = { value: sat };
  m.customProgramCacheKey = () => "ink";
  m.onBeforeCompile = sh => {
    sh.uniforms.uSat = m.userData.sat;
    sh.fragmentShader = "uniform float uSat;\n" + sh.fragmentShader.replace("#include <dithering_fragment>", `#include <dithering_fragment>
      vec3 c = gl_FragColor.rgb; float l = dot(c, vec3(0.299, 0.587, 0.114));
      vec3 lit = c * 1.3 + diffuseColor.rgb * 0.42;   // colour assets: brighter, with readable shadow detail
      gl_FragColor.rgb = mix(vec3(0.10 + 0.88 * smoothstep(0.0, 0.7, l)), lit, uSat);`);
  };
  return m;
}
// A fresh instance of a template with its own ink materials (so each asset can fade to colour on its own).
function instance(tpl, sat) {
  const o = tpl.clone(true), mats = [];
  o.traverse(m => { if (m.isMesh) { m.material = inkMaterial(m.material, sat); mats.push(m.material); m.castShadow = true; m.receiveShadow = true; } });
  o.userData.inkMats = mats; return o;
}
function setSat(o, v) { (o.userData.inkMats || []).forEach(m => { m.userData.sat.value = v; }); }

const COL = { red: 0xe0533a, amber: 0xe0a53a, green: 0x3fb27f };

let renderer, scene, camera, assetGroup, envGroup, raf, container, onPick;
let hemiLight, keyLight, groundMesh, seaMesh, sea = false, bobbers = [];
let ray = new THREE.Raycaster(), mouse = new THREE.Vector2();
let theta = 0.9, phi = 1.02, radius = 60;
const target = new THREE.Vector3(0, 3, 0);
let drag = false, lx = 0, ly = 0, hovered = null;
// vertical tilt band (polar angle from +Y)
const phiMin = 0.35, phiMax = 1.42;
// status balls: a glowing sphere that levitates just to the left of each asset's (camera-facing) tag.
let tagBalls = [];
// graphic-novel render state: ink outline post-pass, per-asset colour fade, pick proxies, flare flames
let composer, nrmT, outlinePass, normalMat, pickables = [], assetRoots = [], selectedId = null, flames = [], loadSeq = 0;
const _camRight = new THREE.Vector3();
// zoom level-of-detail: zooming out past a threshold reveals the facility (interior shell drops away),
// and further out hands off to the map. Callbacks let the app switch to the Leaflet map.
let onZoomOut = null, onStage = null, onEmpty = null, downX = 0, downY = 0, initRadius = 40, facilityStage = false;
// zoom-out is hard-clamped at the facility view (maxRadius); a deliberate extra push past that stop
// (overscroll ticks) is required to step up to the map.
let maxRadius = 220, overscroll = 0;

function mesh(geo, m) { const o = new THREE.Mesh(geo, m); o.castShadow = true; o.receiveShadow = true; return o; }

// ---------------- labels ----------------
function labelSprite(text) {
  const pad = 12, fs = 44, dpr = 2, rad = 9, bw = 2;
  const c = document.createElement("canvas"), g = c.getContext("2d");
  const font = `600 ${fs}px -apple-system,Segoe UI,Roboto,sans-serif`;
  g.font = font;
  const w = g.measureText(text).width + pad * 2, h = fs + pad * 2;
  c.width = w * dpr; c.height = h * dpr; g.scale(dpr, dpr);
  g.font = font;
  const x0 = bw / 2, y0 = bw / 2, ww = w - bw, hh = h - bw;
  g.beginPath();
  if (g.roundRect) g.roundRect(x0, y0, ww, hh, rad); else g.rect(x0, y0, ww, hh);
  g.fillStyle = "rgba(255,253,246,0.45)"; g.fill();                 // translucent light tag: the scene shows through
  g.lineWidth = bw; g.strokeStyle = "rgba(17,17,17,0.55)"; g.stroke(); // soft inked border
  g.textBaseline = "middle";
  g.shadowColor = "rgba(255,253,246,0.9)"; g.shadowBlur = 4;       // a light halo keeps the text crisp over busy backgrounds
  g.fillStyle = "#111111";                                          // dark ink text
  g.fillText(text, pad, h / 2 + 1);
  const tex = new THREE.CanvasTexture(c); tex.minFilter = THREE.LinearFilter;
  const sp = new THREE.Sprite(new THREE.SpriteMaterial({ map: tex, depthTest: false, depthWrite: false, transparent: true }));
  sp.scale.set(w / 44 * 0.5, h / 44 * 0.5, 1);
  sp.renderOrder = 10;
  return sp;
}

// Build a typed equipment model. Returns a Group whose userData carries the asset.
function equip(a, tpl) {
  const g = new THREE.Group();
  const t = a.equipment_type || "";
  let top, foot = 8;
  if (tpl) {
    const o = instance(tpl, 1); g.add(o); g.userData.model = o;
    const b = new THREE.Box3().setFromObject(o), z = b.getSize(new THREE.Vector3());
    top = b.max.y; foot = Math.max(z.x, z.z);
    // invisible box used for hover / click (fast and forgiving compared with the detailed mesh)
    const px = new THREE.Mesh(new THREE.BoxGeometry(z.x, z.y, z.z), new THREE.MeshBasicMaterial({ visible: false }));
    px.position.copy(b.getCenter(new THREE.Vector3())); px.layers.set(1); g.add(px); pickables.push(px);
    if (t === "flare") flames.push(flame(g, stackTip(o, top)));
  } else {   // model failed to load: a plain grey block keeps the asset clickable
    const o = mesh(new THREE.BoxGeometry(3, 2.4, 3), inkMaterial({ color: new THREE.Color(0x9a948b) }, 0)); o.position.y = 1.2; g.add(o);
    o.layers.enable(1); pickables.push(o); top = 2.4; foot = 3;
  }
  g.userData.foot = foot;
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
  ball.position.set(0, top + 1.2, 0); ball.userData.noInk = true; g.add(ball);
  tagBalls.push({ sprite: sp, ball });
  g.userData.asset = a;
  g.traverse(o => { if (o.isMesh || o.isSprite) o.userData.asset = a; });
  return g;
}

// ---------------- the site engineer ----------------
// A rigged, animated engineer (Meshy rig + moves) waits at the gate. Click an asset and he runs to it, and a
// thought bubble shows what he makes of it (one of five lines per status, at random, never the same twice
// running). Red: he turns to face you, smashes his hard hat on the ground, then freezes in that pose.
// Amber: he scratches his head for 3 seconds, then stands looking at it. Green: he just stands looking at it. The run is pinned in place and he is moved across the site
// here, at a speed matched to the run so his feet don't slide. (The model also carries other moves: add
// them to MOVES with their natural ground speed to put them in the random draw.)
const MOVES = { run: 3.9 };
const GRAB_T = 0.93, RELEASE_T = 1.1;        // hat_throw clip: hand at the hat / halfway through the slam
const GRIP = new THREE.Vector3(0, 0.12, 0);   // the hat held just past his fingers (hand-bone space)           // natural ground speed of each move in the draw (m/s; 0 = moves in place)
const LINES = {
  red: ["Great! I'm so getting fired!", "Perfect! There goes my bonus!", "Excellent! Guess who's applying for new jobs tonight!",
        "Fantastic! My performance review just wrote itself!", "Nice! Another one fails successfully!"],
  amber: ["I'd give it three more coffees and one weekend before it goes red.", "Not broken yet, boss. Just practising.",
          "It's held together by hope and one very tired bolt.", "It's fine. It's just humming the song it hums before it explodes.",
          "Don't make eye contact with it, boss. It can smell fear."],
  green: ["Nothing to see here, boss. Even the rust is on schedule.", "Running perfectly. I sprinted 200 metres for this?",
          "All good, boss. I'll just stand here and look important.", "So healthy it's making the rest of us look bad.",
          "All green. Quick, somebody take a photo for the newsletter."],
};
const _lastLine = {};
function line(status) {   // a random line for this status, never the one he said last time
  const L = LINES[status] || LINES.green; let i;
  do { i = Math.floor(Math.random() * L.length); } while (L.length > 1 && i === _lastLine[status]);
  _lastLine[status] = i; return L[i];
}
let ready = false, inkSkip = [], walk = null, handL = null, handR = null, hipBone = null, engFrozen = false, headEnd = null, followR = 0, lastE = 20, eng = null, engMixer = null, engActs = {}, engAct = null, hat = null, headBone = null, engTrip = null, bubbleEl = null;
const engClock = new THREE.Clock(), _v = new THREE.Vector3(), _q = new THREE.Quaternion(), _up = new THREE.Vector3(), _Y = new THREE.Vector3(0, 1, 0);

function loadEngineer() {
  if (eng !== null) return Promise.resolve(eng);
  return new Promise(ok => gltf.load("models/engineer.glb", g => {
    eng = g.scene; eng.scale.setScalar(1.45);
    eng.traverse(o => { if (o.isMesh) { o.material = inkMaterial(o.material, 1); o.castShadow = true; o.frustumCulled = false; } if (o.isBone && o.name === "Head") headBone = o; if (o.isBone && o.name === "head_end") headEnd = o; if (o.isBone && o.name === "LeftHand") handL = o; if (o.isBone && o.name === "RightHand") handR = o; if (o.isBone && o.name === "Hips") hipBone = o; });
    engMixer = new THREE.AnimationMixer(eng);
    g.animations.forEach(c => {
      // pin each move in place: keep the hips' height, drop their travel across the ground
      const hp = c.tracks.find(t => t.name === "Hips.position");
      if (hp) { const v = hp.values; for (let i = 0; i < v.length; i += 3) { v[i] = v[0]; v[i + 2] = v[2]; } }
      engActs[c.name] = engMixer.clipAction(c);
    });
    ["hat_throw", "throw_down"].forEach(k => { const a = engActs[k]; if (a) { a.setLoop(THREE.LoopOnce, 1); a.clampWhenFinished = true; } });
    gltf.load("models/hard_hat.glb", h => {
      hat = h.scene; hat.traverse(o => { if (o.isMesh) { o.material = inkMaterial(o.material, 1); o.castShadow = true; } });
      // find the brim (the widest horizontal slice; below it hangs the chin strap) and the crown just above it
      hat.updateMatrixWorld(true); const pts = [];
      hat.traverse(o => { if (o.isMesh) { const p = o.geometry.attributes.position, v = new THREE.Vector3(); for (let k = 0; k < p.count; k += 2) pts.push(v.fromBufferAttribute(p, k).applyMatrix4(o.matrixWorld).clone()); } });
      let y0 = Infinity, y1 = -Infinity; pts.forEach(p => { y0 = Math.min(y0, p.y); y1 = Math.max(y1, p.y); });
      const N = 24, sl = Array.from({ length: N }, () => ({ x0: Infinity, x1: -Infinity, z0: Infinity, z1: -Infinity, n: 0 }));
      pts.forEach(p => { const k = Math.min(N - 1, Math.floor((p.y - y0) / (y1 - y0) * N)), q = sl[k]; q.n++; q.x0 = Math.min(q.x0, p.x); q.x1 = Math.max(q.x1, p.x); q.z0 = Math.min(q.z0, p.z); q.z1 = Math.max(q.z1, p.z); });
      let bi = 0; sl.forEach((q, k) => { if (q.n && (q.x1 - q.x0) * (q.z1 - q.z0) > (sl[bi].x1 - sl[bi].x0) * (sl[bi].z1 - sl[bi].z0)) bi = k; });
      const crown = sl[Math.min(N - 1, bi + 2)], brimY = y0 + (y1 - y0) * (bi + 0.5) / N;
      // size the crown to fit over his head (crown width = 1.2 x head width), and hang the hat from its brim
      const headW = 0.24 * (eng.scale.x || 1);
      const k = (headW * 1.2) / Math.max(1e-6, crown.x1 - crown.x0);
      const cx = (crown.x0 + crown.x1) / 2, cz = (crown.z0 + crown.z1) / 2;
      hat.children.forEach(ch => ch.position.sub(new THREE.Vector3(cx, brimY, cz)));
      hat.scale.setScalar(k); hat.userData.crownH = (y1 - brimY) * k;
      hat.userData.onHead = true; hat.userData.base = hat.scale.x; ok(eng);
    }, undefined, () => ok(eng));
  }, undefined, () => { eng = false; ok(null); }));
}

function play(name, fade = 0.25, speed = 1) {
  const a = engActs[name]; if (!a) return;
  a.timeScale = speed;
  if (a === engAct) return;
  a.reset().fadeIn(fade).play(); if (engAct) engAct.fadeOut(fade); engAct = a;
}

function face(dx, dz) { eng.rotation.y = Math.atan2(dx, dz); }   // the rig faces +Z

// Put the engineer (and his hat) back on the gate side of the site, idling.
function placeEngineer(E) {
  if (!eng) return;
  scene.add(eng); walk = null; if (hat) { scene.add(hat); hat.userData.onHead = true; hat.userData.fly = null; hat.userData.hand = null; }
  eng.position.set(-1.5, 0, E + 2.5); eng.rotation.set(0, Math.PI, 0); engTrip = null; engFrozen = false; hideBubble(); play("idle", 0);
}

// ---- walking around the plant, not through it ----
// A coarse walk map of the site: every asset's footprint, the fence (bar the gate) and the larger scenery
// are blocked, with a margin. A* finds the shortest clear route, then the route is pulled taut so he only
// turns where he has to.
const CELL = 0.6, MARGIN = 1.0;
function buildWalkMap() {
  const E = lastE, B = E + 16, n = Math.ceil((2 * B) / CELL), grid = new Uint8Array(n * n);
  const toC = v => Math.max(0, Math.min(n - 1, Math.floor((v + B) / CELL)));
  const block = (x0, z0, x1, z1) => { for (let i = toC(x0); i <= toC(x1); i++) for (let j = toC(z0); j <= toC(z1); j++) grid[j * n + i] = 1; };
  const boxes = [];
  assetRoots.forEach(g => { const m = g.userData.model || g; const b = new THREE.Box3().setFromObject(m); boxes.push({ b, g }); block(b.min.x - MARGIN, b.min.z - MARGIN, b.max.x + MARGIN, b.max.z + MARGIN); });
  envGroup.children.forEach(o => { if (!SOLID.has(o.userData.kind)) return; const b = new THREE.Box3().setFromObject(o); block(b.min.x - 0.6, b.min.z - 0.6, b.max.x + 0.6, b.max.z + 0.6); });
  const t = 0.5;
  if (sea) {   // on a platform: everything past the deck handrails is sea
    block(-B, -B, B, -E + 1); block(-B, -B, -E + 1, B); block(E - 1, -B, B, B); block(-B, E + 15, B, B);
  } else {     // the fence line, with the gate left open on the near side
    block(-E - t, -E - t, E + t, -E + t); block(-E - t, -E - t, -E + t, E + t); block(E - t, -E - t, E + t, E + t);
    block(-E - t, E - t, -5.5, E + t); block(5.5, E - t, E + t, E + t);
  }
  return { n, B, grid, boxes, toC, at: (i, j) => (i < 0 || j < 0 || i >= n || j >= n) ? 1 : grid[j * n + i], w: (i) => -B + (i + 0.5) * CELL };
}
const SOLID = new Set(["site_office", "ute", "light_tower", "tanks", "pipe_rack", "compressor_old", "living_quarters", "helideck", "pedestal_crane", "deck_containers"]);

function findPath(from, g) {
  if (!walk) walk = buildWalkMap();
  const W = walk, n = W.n, box = (W.boxes.find(x => x.g === g) || {}).b;
  let si = W.toC(from.x), sj = W.toC(from.z);
  if (W.at(si, sj)) {   // standing in a blocked cell (e.g. right beside the last asset): step to the nearest free one
    let best = null, bd = 1e9;
    for (let r = 1; r < 12 && !best; r++) for (let i = si - r; i <= si + r; i++) for (let j = sj - r; j <= sj + r; j++) if (!W.at(i, j)) { const d = (i - si) ** 2 + (j - sj) ** 2; if (d < bd) { bd = d; best = [i, j]; } }
    if (best) [si, sj] = best;
  }
  const goalD = MARGIN + CELL * 1.6;
  const near = (i, j) => { if (!box) return false; const x = W.w(i), z = W.w(j); const dx = Math.max(box.min.x - x, 0, x - box.max.x), dz = Math.max(box.min.z - z, 0, z - box.max.z); return Math.hypot(dx, dz) <= goalD; };
  const cx = box ? (box.min.x + box.max.x) / 2 : 0, cz = box ? (box.min.z + box.max.z) / 2 : 0;
  const h = (i, j) => Math.hypot(W.w(i) - cx, W.w(j) - cz) / CELL;
  const gS = new Float32Array(n * n).fill(Infinity), came = new Int32Array(n * n).fill(-1), open = [[h(si, sj), si, sj]];
  gS[sj * n + si] = 0; let end = -1, guard = 0;
  while (open.length && guard++ < 60000) {
    let bi = 0; for (let k = 1; k < open.length; k++) if (open[k][0] < open[bi][0]) bi = k;
    const [, i, j] = open.splice(bi, 1)[0], id = j * n + i;
    if (near(i, j) && !W.at(i, j)) { end = id; break; }
    for (let di = -1; di <= 1; di++) for (let dj = -1; dj <= 1; dj++) {
      if (!di && !dj) continue; const a = i + di, b = j + dj; if (W.at(a, b)) continue;
      if (di && dj && (W.at(i + di, j) || W.at(i, j + dj))) continue;   // no cutting corners
      const ng = gS[id] + (di && dj ? 1.414 : 1), nid = b * n + a;
      if (ng < gS[nid]) { gS[nid] = ng; came[nid] = id; open.push([ng + h(a, b), a, b]); }
    }
  }
  if (end < 0) return null;
  const cells = []; for (let c = end; c >= 0; c = came[c]) cells.push(c); cells.reverse();
  const pts = cells.map(c => new THREE.Vector3(W.w(c % n), 0, W.w(Math.floor(c / n))));
  pts[0] = from.clone().setY(0);
  // pull the route taut: keep a corner only where a straight line would cross something
  const clear = (a, b) => { const L = a.distanceTo(b), k = Math.ceil(L / (CELL * 0.5)); for (let q = 1; q < k; q++) { const t = q / k; if (W.at(W.toC(a.x + (b.x - a.x) * t), W.toC(a.z + (b.z - a.z) * t))) return false; } return true; };
  const out = [pts[0]]; let i0 = 0;
  while (i0 < pts.length - 1) { let j = pts.length - 1; while (j > i0 + 1 && !clear(pts[i0], pts[j])) j--; out.push(pts[j]); i0 = j; }
  return out;
}

// Head off to an asset group, along a clear route.
function visit(g, force) {
  if (!eng || !g) return;
  hideBubble(); engFrozen = false; if (hat) { hat.userData.onHead = true; hat.userData.fly = null; hat.userData.hand = null; hat.userData.squash = 0; if (hat.userData.base) hat.scale.setScalar(hat.userData.base); }
  const from = eng.position.clone().setY(0);
  let path = findPath(from, g);
  if (!path || path.length < 2) {                      // no clear route found: fall back to straight in front of it
    const to = g.position.clone(), d = new THREE.Vector3().subVectors(from, to).setY(0); if (d.lengthSq() < 1e-4) d.set(0, 0, 1);
    to.add(d.normalize().multiplyScalar((g.userData.foot || 6) / 2 + 1.6)); path = [from, to];
  }
  const segs = []; let dist = 0; for (let i = 1; i < path.length; i++) { const L = path[i].distanceTo(path[i - 1]); segs.push(L); dist += L; }
  const choices = Object.keys(MOVES).filter(k => engActs[k]);
  const move = force && engActs[force] ? force : choices[Math.floor(Math.random() * choices.length)];
  // trip time: the move's own pace, kept between about 2.5 and 6 seconds
  const nat = MOVES[move] || 0, T = Math.min(7, Math.max(2.5, dist / (nat || 2.2))), speed = dist / T;
  play(move, 0.2, nat ? speed / nat : 1);
  engTrip = { path, segs, dist, move, t: 0, T, status: (g.userData.asset && g.userData.asset.pin_colour) || "green", target: g };
}

function updateEngineer(dt) {
  if (!eng || !engMixer) return;
  if (!engFrozen) engMixer.update(dt);
  const tr = engTrip;
  if (tr && tr.t < tr.T) {
    tr.t = Math.min(tr.T, tr.t + dt);
    let d = tr.dist * tr.t / tr.T, i = 0; while (i < tr.segs.length - 1 && d > tr.segs[i]) { d -= tr.segs[i]; i++; }
    const a = tr.path[i], b = tr.path[i + 1], k = tr.segs[i] ? Math.min(1, d / tr.segs[i]) : 1;
    eng.position.lerpVectors(a, b, k);
    let want = Math.atan2(b.x - a.x, b.z - a.z), dr = want - eng.rotation.y; dr = Math.atan2(Math.sin(dr), Math.cos(dr));
    eng.rotation.y += dr * Math.min(1, dt * 12);    // turn smoothly at each corner
    if (tr.t >= tr.T) arrive(tr);
  } else if (tr && tr.stage === "turn") {
    // turn round to face the viewer before the outburst
    _v.subVectors(camera.position, eng.position).setY(0);
    const want = Math.atan2(_v.x, _v.z); let dr = want - eng.rotation.y; dr = Math.atan2(Math.sin(dr), Math.cos(dr));
    eng.rotation.y += dr * Math.min(1, dt * 7);
    if (Math.abs(dr) < 0.05) { tr.stage = "throw"; tr.since = 0; play(engActs.hat_throw ? "hat_throw" : "throw_down", 0.2); }
  } else if (tr && tr.stage === "throw") {
    tr.since += dt;
    if (hat && handR) {
      // timed to the throw move itself (seconds into the clip): his right hand reaches the hat at the top of
      // the reach, eases it into his grip, and lets go halfway through the slam, so the hat flies down with it
      const ct = engActs.hat_throw ? engActs.hat_throw.time : tr.since;
      const hp = handR.getWorldPosition(new THREE.Vector3()), hq = handR.getWorldQuaternion(new THREE.Quaternion());
      if (hat.userData.onHead && ct >= GRAB_T) {
        hat.userData.onHead = false;
        hat.userData.hand = { off: hat.position.clone().sub(hp).applyQuaternion(hq.clone().invert()), rel: hq.clone().invert().multiply(hat.quaternion.clone()), prev: hp.clone(), t: 0 };
      } else if (hat.userData.hand) {
        const H = hat.userData.hand, vel = hp.clone().sub(H.prev).divideScalar(Math.max(dt, 1e-3)); H.prev.copy(hp);
        H.t = Math.min(1, H.t + dt / 0.07); H.off.lerp(GRIP, H.t);   // slides into his hand over ~0.07 s
        hat.position.copy(hp).add(H.off.clone().applyQuaternion(hq)); hat.quaternion.copy(hq).multiply(H.rel);
        if (ct >= RELEASE_T) {
          hat.userData.hand = null;
          const hv = new THREE.Vector3(vel.x, 0, vel.z).clampLength(0, 2.2);   // down and in front of him
          hat.userData.fly = { v: hv.setY(Math.min(-6, vel.y * 1.2)), spin: 6, bounced: 0, smash: () => { tr.smashAt = tr.since; } };
        }
      }
    }
    // bubble out a beat after the hat hits the ground, then he holds that pose
    if ((tr.smashAt != null && tr.since > tr.smashAt + 0.85) || tr.since > 5) { showBubble(line("red")); tr.stage = "rant"; engFrozen = true; }
  } else if (tr && tr.stage === "act") {
    // amber: three seconds of head scratching, then he settles; green: he settles straight away. Then he stands still.
    tr.since += dt;
    if (tr.since > (tr.status === "amber" ? 3 : 0) && !tr.settled) { tr.settled = true; tr.since = 3; play("idle", 0.4); }
    if (tr.settled && tr.since > 3.5) { tr.stage = "done"; engFrozen = true; }
  }
  // the hat rides on his head, or flies and bounces once it's thrown
  if (hat) {
    if (hat.userData.onHead && headBone && headEnd) {
      // sit on the crown: along the head bone -> top-of-skull direction, turned the way he faces
      headBone.getWorldPosition(_v); headEnd.getWorldPosition(_up);
      const dir = _up.clone().sub(_v); const len = dir.length(); dir.normalize();
      // brim at brow height, so the crown covers the top of his head (its top just clears the skull)
      hat.position.copy(_v).addScaledVector(dir, Math.max(len * 0.55, len + 0.03 - (hat.userData.crownH || 0)));
      _q.setFromUnitVectors(_Y, dir); hat.quaternion.setFromAxisAngle(_Y, eng.rotation.y).premultiply(_q);
    } else if (hat.userData.fly) {
      const f = hat.userData.fly; f.v.y -= 9.8 * dt; hat.position.addScaledVector(f.v, dt); hat.rotation.x += f.spin * dt;
      if (hat.position.y < 0.05) {
        hat.position.y = 0.05;
        if (f.smash) { f.smash(); f.smash = null; hat.userData.squash = 1; }
        if (f.bounced < 2 && Math.abs(f.v.y) > 1) { f.v.y = f.bounced ? 0.6 : 1.6; f.v.x *= 0.35; f.v.z *= 0.35; f.spin *= 0.4; f.bounced++; }   // a small hop, then a smaller one
        else { hat.userData.fly = null; hat.rotation.set(0, hat.rotation.y, 0); }
      }
    }
    if (hat.userData.squash) {          // a quick comic squash on impact
      const q = hat.userData.squash = Math.max(0, hat.userData.squash - dt * 4); const sy = 1 - 0.45 * Math.sin(q * Math.PI);
      hat.scale.set(hat.userData.base * (2 - sy), hat.userData.base * sy, hat.userData.base * (2 - sy));
    }
  }
  if (bubbleEl && bubbleEl.classList.contains("on")) placeBubble();
}

function arrive(tr) {
  const g = tr.target; _v.subVectors(g.position, eng.position); face(_v.x, _v.z); eng.position.y = 0;
  if (tr.status === "red") { tr.stage = "turn"; play("idle", 0.25); return; }
  tr.stage = "act"; tr.since = 0; showBubble(line(tr.status));
  if (tr.status === "amber") play("head_scratch", 0.25);            // looking at it, scratching his head
}

// The comic speech bubble sits above his head and follows him on screen.
function showBubble(text) {
  if (!bubbleEl) { bubbleEl = document.createElement("div"); bubbleEl.className = "speech3d"; container.appendChild(bubbleEl); }
  else if (bubbleEl.parentNode !== container) container.appendChild(bubbleEl);
  bubbleEl.innerHTML = `<span class="tx"></span><i class="p1"></i><i class="p2"></i>`; bubbleEl.querySelector(".tx").textContent = text;
  bubbleEl.classList.add("on", "thought"); placeBubble();
}
function hideBubble() { if (bubbleEl) bubbleEl.classList.remove("on"); }
function placeBubble() {
  if (!headBone) return;
  headBone.getWorldPosition(_v); _v.y += 0.55; _v.project(camera);
  const w = container.clientWidth, h = container.clientHeight;
  bubbleEl.style.left = ((_v.x * 0.5 + 0.5) * w) + "px"; bubbleEl.style.top = ((-_v.y * 0.5 + 0.5) * h) + "px";
  bubbleEl.style.display = _v.z < 1 ? "" : "none";
}

// ---------------- renderer / scene ----------------
function updateCam() {
  phi = Math.max(phiMin, Math.min(phiMax, phi));
  radius = Math.max(10, Math.min(maxRadius, radius));
  if (outlinePass) outlinePass.uniforms.view.value = radius;
  camera.position.set(
    target.x + radius * Math.sin(phi) * Math.cos(theta),
    target.y + radius * Math.cos(phi),
    target.z + radius * Math.sin(phi) * Math.sin(theta));
  camera.lookAt(target);
}

// ---------------- the site scenery ----------------
function _rng(seed) { let a = seed | 0; return function () { a = a + 0x6D2B79F5 | 0; let t = Math.imul(a ^ a >>> 15, 1 | a); t = t + Math.imul(t ^ t >>> 7, 61 | t) ^ t; return ((t ^ t >>> 14) >>> 0) / 4294967296; }; }

function clearEnv() {
  bobbers = [];
  if (!envGroup) return;
  envGroup.children.slice().forEach(c => { c.traverse(o => { if (o.geometry) o.geometry.dispose(); if (o.material) o.material.dispose && o.material.dispose(); }); envGroup.remove(c); });
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

// The site around the equipment, all from the same painted model set and always greyscale: a fenced
// gravel pad, a site office, ute and lighting tower at the gate, a windsock, process scenery on plant
// sites (tanks, pipe rack, an older compressor), field crew beside the equipment, gum trees, scrub and
// rocks outside the fence, and red mesas on the horizon.
async function buildSite(assets, span, placed, seq) {
  const r = _rng(97 + Math.round(span * 13));
  const T = await loadProps();
  if (seq !== loadSeq) return;
  const NO_LINE = new Set(["fence", "gum_tree", "scrub", "mesa"]);
  const put = (k, x, z, ry, s) => { if (!T[k]) return null; const o = instance(T[k], 0); o.position.set(x, 0, z); o.rotation.y = ry || 0; if (s) o.scale.multiplyScalar(s); o.traverse(m => { if (m.isMesh) m.castShadow = m.receiveShadow = true; }); if (NO_LINE.has(k)) o.userData.noInk = true; o.userData.kind = k; envGroup.add(o); return o; };
  const types = new Set(assets.map(a => a.equipment_type));
  const plant = types.has("compressor") || types.has("separator") || types.has("utilities");
  const E = span / 2 + 9;                                    // fence half-size around the pad
  // perimeter fence with a gate gap on the near (+z) side
  for (const side of [0, 1, 2, 3]) for (let u = -E + 1.5; u < E; u += 3) {
    if (side === 0 && Math.abs(u) < 5) continue;
    const [x, z, ry] = side === 0 ? [u, E, 0] : side === 1 ? [u, -E, 0] : side === 2 ? [E, u, Math.PI / 2] : [-E, u, Math.PI / 2];
    put("fence", x, z, ry);
  }
  // at the gate: site office, ute, lighting tower; windsock on a corner
  put("site_office", -9, E + 9, Math.PI * 0.1); put("light_tower", 10, E + 3, 0.4);
  // the site ute, painted in the site's overall status: red if any asset is red, else amber if any is amber, else green
  const worst = assets.some(a => a.pin_colour === "red") ? "red" : assets.some(a => a.pin_colour === "amber") ? "amber" : "green";
  const ute = put("ute", 4, E + 7, -0.5);
  if (ute) { setSat(ute, 1); ute.userData.inkMats.forEach(m => m.color.set(COL[worst])); }
  windsock(put("windsock", E - 2, -E + 2, 0));
  if (plant) { put("tanks", -E - 9, -E * 0.35, 0.2); put("pipe_rack", E + 8, -E * 0.2, Math.PI / 2); put("compressor_old", -E - 10, E * 0.45, 1.1); }
  // crew beside the equipment: one at the first asset needing attention, one at another asset, one at the office
  const busy = placed[Math.max(0, assets.findIndex(a => a.pin_colour === "red" || a.pin_colour === "amber"))] || [0, 0];
  put("worker_1", busy[0] + 4.2, busy[1] + 3.6, -2.4);
  const other = placed[Math.min(placed.length - 1, Math.floor(placed.length / 2))] || [0, 0];
  if (other !== busy) put("worker_2", other[0] - 4, other[1] + 3.8, 2.6);
  put("worker_3", -3, E + 6, 2.9);
  // outside the fence: gum trees, scrub and rocks; red mesas on the horizon
  for (let i = 0; i < 14; i++) { const a = r() * Math.PI * 2, d = E + 10 + r() * 55; put("gum_tree", Math.cos(a) * d, Math.sin(a) * d, r() * 6, 0.8 + r() * 0.5); }
  for (let i = 0; i < 28; i++) { const a = r() * Math.PI * 2, d = E + 4 + r() * 70; put("scrub", Math.cos(a) * d, Math.sin(a) * d, r() * 6, 0.7 + r() * 0.8); }
  for (let i = 0; i < 8; i++) { const a = r() * Math.PI * 2, d = E + 12 + r() * 60; put("rocks", Math.cos(a) * d, Math.sin(a) * d, r() * 6, 0.6 + r() * 0.9); }
  for (let i = 0; i < 6; i++) { const a = i / 6 * Math.PI * 2 + r() * 0.6, d = 230 + r() * 90; const m = put("mesa", Math.cos(a) * d, Math.sin(a) * d, r() * 6, 0.8 + r() * 0.8); if (m) m.traverse(o => { o.castShadow = false; }); }
}

// An offshore wellhead platform or manifold, in the same painted greyscale set: the equipment stands on a
// steel deck on a jacket rising out of a soft blue sea, with handrails round the deck edge, living quarters,
// a helideck, a pedestal crane and cargo on deck, and the crew. In the water: the crew boat tied up at the
// boat landing (painted in the site's status colour, like the ute on land), a supply vessel standing off,
// marker buoys, a whale now and then, gulls circling, and other platforms and an LNG carrier on the horizon.
async function buildPlatform(assets, span, placed, seq) {
  const r = _rng(53 + Math.round(span * 7));
  const T = await loadProps(true);
  if (seq !== loadSeq) return;
  const put = (k, x, y, z, ry, s) => { if (!T[k]) return null; const o = instance(T[k], 0); o.position.set(x, y, z); o.rotation.y = ry || 0; if (s) o.scale.multiplyScalar(s); o.traverse(m => { if (m.isMesh) m.castShadow = m.receiveShadow = true; }); o.userData.kind = k; envGroup.add(o); return o; };
  const E = span / 2 + 9, Z0 = -E, Z1 = E + 16, W = 2 * E, D = Z1 - Z0, zc = (Z0 + Z1) / 2;
  // the deck: a steel plate with a painted walkway grid, its top level with y = 0
  const deck = mesh(new THREE.BoxGeometry(W, 1.4, D), inkMaterial({ map: deckTexture(W, D) }, 0)); deck.position.set(0, -0.7, zc); deck.userData.kind = "deck"; envGroup.add(deck);
  // the jacket under it, stretched to the deck's footprint, its feet in the water
  const j = put("platform_jacket", 0, -DECK_H - 0.6, zc, 0);
  if (j) { const b = new THREE.Box3().setFromObject(j), z = b.getSize(new THREE.Vector3()); j.scale.multiply(new THREE.Vector3(W * 0.82 / z.x, (DECK_H - 0.8) / z.y, D * 0.82 / z.z)); j.traverse(o => { o.castShadow = false; }); }
  // handrails round the deck edge
  for (let u = -E + 1.5; u < E; u += 3) { put("deck_rail", u, 0, Z0 + 0.4, 0); put("deck_rail", u, 0, Z1 - 0.4, 0); }
  for (let u = Z0 + 1.5; u < Z1; u += 3) { put("deck_rail", -E + 0.4, 0, u, Math.PI / 2); put("deck_rail", E - 0.4, 0, u, Math.PI / 2); }
  // deck kit at the near end, clear of the equipment; crane and cargo at the far corners
  put("living_quarters", -E + 6.5, 0, E + 9, Math.PI * 0.5); put("helideck", E - 5, 1.2, E + 12, 0.2);   // the helideck overhangs the corner
  put("pedestal_crane", E - 3.5, 0, Z0 + 3.5, -0.8); put("deck_containers", -E + 5, 0, Z0 + 5, 0.3);
  const busy = placed[Math.max(0, assets.findIndex(a => a.pin_colour === "red" || a.pin_colour === "amber"))] || [0, 0];
  put("worker_1", busy[0] + 4.2, 0, busy[1] + 3.6, -2.4);
  const other = placed[Math.min(placed.length - 1, Math.floor(placed.length / 2))] || [0, 0];
  if (other !== busy) put("worker_2", other[0] - 4, 0, other[1] + 3.8, 2.6);
  put("worker_3", -3, 0, E + 6, 2.9);
  // on the water: things ride the swell (bob), the whale surfaces now and then, the gulls wheel overhead
  const SEA = -DECK_H, bob = (o, amp, roll) => { if (o) bobbers.push({ o, y: o.position.y, amp, roll, p: r() * 10, ry: o.rotation.y }); return o; };
  const worst = assets.some(a => a.pin_colour === "red") ? "red" : assets.some(a => a.pin_colour === "amber") ? "amber" : "green";
  const boat = bob(put("crew_boat", -E * 0.15, SEA - 0.4, Z0 - 42, 0.75), 0.35, 0.03);   // clear of the deck, on the side the camera looks across
  if (boat) { setSat(boat, 1); boat.userData.inkMats.forEach(m => m.color.set(COL[worst])); }
  bob(put("supply_vessel", E + 30, SEA - 0.8, zc - 6, Math.PI / 2 + 0.08), 0.45, 0.02);   // standing off, parallel to the platform
  for (let i = 0; i < 5; i++) { const a = r() * Math.PI * 2, d = E + 45 + r() * 90; bob(put("buoy", Math.cos(a) * d, SEA - 0.5, Math.sin(a) * d, r() * 6), 0.5, 0.08); }
  for (let i = 0; i < 3; i++) { const a = -0.6 + i * 1.9 + r() * 0.5, d = 360 + r() * 110; const o = put("distant_platform", Math.cos(a) * d, SEA - 1, Math.sin(a) * d, r() * 6, 0.8 + r() * 0.4); if (o) { o.userData.noInk = true; o.traverse(m => { m.castShadow = false; }); } }
  { const a = 2.6 + r() * 0.6, d = 520; const o = put("lng_carrier", Math.cos(a) * d, SEA - 1.5, Math.sin(a) * d, a + Math.PI / 2); if (o) { o.userData.noInk = true; o.traverse(m => { m.castShadow = false; }); bob(o, 0.3, 0.0); } }
  { const a = r() * Math.PI * 2, d = E + 70 + r() * 40; const w = put("whale_tail", Math.cos(a) * d, SEA - 9, Math.sin(a) * d, r() * 6); if (w) bobbers.push({ o: w, whale: true, y: SEA - 9, p: r() * 20 }); }
  for (let i = 0; i < 4; i++) { const g = put("seagull", 0, 0, 0, 0); if (g) { g.userData.noInk = true; g.traverse(m => { m.castShadow = false; }); bobbers.push({ o: g, gull: true, rad: 22 + r() * 30, h: 14 + r() * 12, sp: (0.18 + r() * 0.12) * (r() < 0.5 ? -1 : 1), p: r() * 6.3, cz: zc }); } }
}

// The swell, the whale and the gulls (offshore only).
function updateSea(t) {
  if (!sea) return;
  if (seaMesh) { const m = seaMesh.material.map; m.offset.x = (t * 0.0035) % 1; m.offset.y = (t * 0.0021) % 1; }
  for (const b of bobbers) {
    if (b.gull) { const a = b.p + t * b.sp; b.o.position.set(Math.cos(a) * b.rad, b.h + Math.sin(t * 0.7 + b.p) * 1.2, b.cz + Math.sin(a) * b.rad); b.o.rotation.set(0, -a + (b.sp > 0 ? 0 : Math.PI), (b.sp > 0 ? 1 : -1) * 0.35); continue; }
    if (b.whale) { const c = ((t + b.p) % 26) / 26, up = c < 0.25 ? Math.sin(c / 0.25 * Math.PI) : 0; b.o.position.y = b.y + up * 9; b.o.visible = up > 0.02; continue; }
    b.o.position.y = b.y + Math.sin(t * 0.9 + b.p) * b.amp; b.o.rotation.z = Math.sin(t * 0.7 + b.p) * b.roll; b.o.rotation.y = b.ry + Math.sin(t * 0.23 + b.p) * 0.03;
  }
}

// Turn the windsock so its sock streams downwind: find which way the sock sticks out from the mast in the
// model (the centre of its upper part against the mast foot), then rotate that onto the wind.
function windsock(o) {
  if (!o) return;
  o.updateMatrixWorld(true); const b = new THREE.Box3().setFromObject(o), yc = b.min.y + (b.max.y - b.min.y) * 0.6, v = new THREE.Vector3();
  let tx = 0, tz = 0, tn = 0, fx = 0, fz = 0, fn = 0;
  o.traverse(m => { if (!m.isMesh) return; const p = m.geometry.attributes.position;
    for (let i = 0; i < p.count; i += 3) { v.fromBufferAttribute(p, i).applyMatrix4(m.matrixWorld); if (v.y > yc) { tx += v.x; tz += v.z; tn++; } else if (v.y < b.min.y + 0.8) { fx += v.x; fz += v.z; fn++; } } });
  if (!tn || !fn) return;
  const sock = Math.atan2(tx / tn - fx / fn, tz / tn - fz / fn), want = Math.atan2(WIND.x, WIND.z);
  o.rotation.y += want - sock;
}

function buildEnvironment(assets, span, placed, seq) {
  { const a = _rng(31 + assets.length * 17 + (assets[0] ? assets[0].asset_id.length * 7 : 0))() * Math.PI * 2; WIND.set(Math.sin(a), 0, Math.cos(a)); }   // this site's wind
  clearEnv(); facilityStage = false;
  return sea ? buildPlatform(assets, span, placed, seq) : buildSite(assets, span, placed, seq);
}

// Land or sea: the gravel ground and dusty haze, or a soft blue sea under a pale sky.
function setSetting(offshore) {
  sea = offshore;
  groundMesh.visible = !sea;
  if (sea && !seaMesh) {
    seaMesh = new THREE.Mesh(new THREE.PlaneGeometry(2400, 2400), new THREE.MeshStandardMaterial({ map: seaTexture(), roughness: 0.9, metalness: 0 }));
    seaMesh.rotation.x = -Math.PI / 2; seaMesh.position.y = -DECK_H; seaMesh.receiveShadow = false; scene.add(seaMesh);   // in the ink depth pass, so nothing under the water shows through
  }
  if (seaMesh) seaMesh.visible = sea;
  const sky = sea ? 0x8fbbdb : 0xa9a49d;   // offshore: a clear sky blue, so the deep blue sea fades into it at the horizon
  scene.background.set(sky); scene.fog.color.set(sky); scene.fog.near = sea ? 170 : 90; scene.fog.far = sea ? 820 : 420;
}

// Where the flare stack actually ends: the centre of the model's highest vertices (the stack stands to one
// side of its knock-out drum, so the footprint centre is not the tip).
function stackTip(o, top) {
  o.updateMatrixWorld(true); const inv = new THREE.Matrix4().copy(o.parent.matrixWorld).invert(), v = new THREE.Vector3();
  let sx = 0, sz = 0, n = 0;
  o.traverse(m => { if (!m.isMesh) return; const p = m.geometry.attributes.position;
    for (let i = 0; i < p.count; i++) { v.fromBufferAttribute(p, i).applyMatrix4(m.matrixWorld).applyMatrix4(inv); if (v.y > top - 0.6) { sx += v.x; sz += v.z; n++; } } });
  return new THREE.Vector3(n ? sx / n : 0, top, n ? sz / n : 0);
}
// A soft round flame / smoke puff for sprites: a radial gradient from a hot core to a clear edge.
let _flameTex = null, _smokeTex = null;
function puffTexture(stops) {
  const cv = document.createElement("canvas"); cv.width = cv.height = 128; const g = cv.getContext("2d");
  const gr = g.createRadialGradient(64, 64, 0, 64, 64, 64); stops.forEach(([k, c]) => gr.addColorStop(k, c));
  g.fillStyle = gr; g.fillRect(0, 0, 128, 128); const t = new THREE.CanvasTexture(cv); t.colorSpace = THREE.SRGBColorSpace; return t;
}
// A burning flare at the stack tip: a stack of flickering, wind-bent flame tongues (white-yellow core, orange
// body, red fringe) with a thin smoke plume drifting off the top (always in colour, the eye goes to it).
function flame(g, tip) {
  if (!_flameTex) {
    _flameTex = puffTexture([[0, "rgba(255,250,220,1)"], [0.25, "rgba(255,210,90,0.95)"], [0.55, "rgba(255,120,30,0.6)"], [1, "rgba(200,40,10,0)"]]);
    _smokeTex = puffTexture([[0, "rgba(70,66,62,0.55)"], [0.6, "rgba(90,86,82,0.25)"], [1, "rgba(110,106,100,0)"]]);
  }
  const root = new THREE.Group(); root.position.copy(tip); root.userData.noInk = true; g.add(root);
  const parts = [];
  for (let i = 0; i < 10; i++) {   // the flame body: solid orange-to-red tongues (they read against a bright sky)
    const sp = new THREE.Sprite(new THREE.SpriteMaterial({ map: _flameTex, depthWrite: false, transparent: true, color: new THREE.Color().setHSL(0.085 - i * 0.007, 1, 0.52) }));
    sp.userData.noInk = true; sp.renderOrder = 5; root.add(sp); parts.push({ sp, k: i / 9, p: Math.random() * 10, smoke: false, w: 2.6 });
  }
  for (let i = 0; i < 5; i++) {    // the hot core: bright yellow-white glow low in the flame
    const sp = new THREE.Sprite(new THREE.SpriteMaterial({ map: _flameTex, blending: THREE.AdditiveBlending, depthWrite: false, transparent: true, color: 0xfff1b0 }));
    sp.userData.noInk = true; sp.renderOrder = 6; root.add(sp); parts.push({ sp, k: i / 4 * 0.45, p: Math.random() * 10, smoke: false, w: 1.5 });
  }
  for (let i = 0; i < 6; i++) {   // smoke
    const sp = new THREE.Sprite(new THREE.SpriteMaterial({ map: _smokeTex, depthWrite: false, transparent: true }));
    sp.userData.noInk = true; root.add(sp); parts.push({ sp, k: i / 5, p: Math.random() * 10, smoke: true });
  }
  return { root, parts, p: Math.random() * 10 };
}

// Ground: a hand-inked gravel pad (canvas texture), tiled.
function gravelTexture() {
  const cv = document.createElement("canvas"); cv.width = cv.height = 1024; const g = cv.getContext("2d");
  g.fillStyle = "#827c75"; g.fillRect(0, 0, 1024, 1024);
  for (let i = 0; i < 1600; i++) { const x = Math.random() * 1024, y = Math.random() * 1024, rr = 30 + Math.random() * 90, v = 118 + Math.random() * 22 | 0; g.fillStyle = `rgba(${v},${v - 5},${v - 10},0.10)`; g.beginPath(); g.arc(x, y, rr, 0, 7); g.fill(); }
  for (let i = 0; i < 700; i++) { const x = Math.random() * 1024, y = Math.random() * 1024, s = 1.5 + Math.random() * 3, v = 128 + Math.random() * 22 | 0;
    g.fillStyle = `rgb(${v},${v - 5},${v - 11})`; g.beginPath(); g.ellipse(x, y, s * 1.4, s, Math.random() * 3, 0, 7); g.fill(); g.strokeStyle = "rgba(30,26,22,0.35)"; g.lineWidth = 1; g.stroke(); }
  g.strokeStyle = "rgba(30,26,22,0.30)"; g.lineWidth = 1.4;
  for (let i = 0; i < 500; i++) { const x = Math.random() * 1024, y = Math.random() * 1024, l = 6 + Math.random() * 14; g.beginPath(); g.moveTo(x, y); g.lineTo(x + l, y + (Math.random() - 0.5) * 3); g.stroke(); }
  const t = new THREE.CanvasTexture(cv); t.wrapS = t.wrapT = THREE.RepeatWrapping; t.repeat.set(40, 40); t.colorSpace = THREE.SRGBColorSpace; t.anisotropy = 8;
  return t;
}

// The sea: a deep ocean blue with hand-inked ripple strokes, tiled and drifting slowly.
function seaTexture() {
  const cv = document.createElement("canvas"); cv.width = cv.height = 1024; const g = cv.getContext("2d");
  g.fillStyle = "#1f6fa8"; g.fillRect(0, 0, 1024, 1024);
  // draw each mark at its spot and wrapped across the edges, so the tile repeats with no seam
  const wrap = f => { for (const dx of [-1024, 0, 1024]) for (const dy of [-1024, 0, 1024]) f(dx, dy); };
  for (let i = 0; i < 700; i++) { const x = Math.random() * 1024, y = Math.random() * 1024, rr = 40 + Math.random() * 120, c = Math.random() < 0.5 ? "rgba(70,150,200,0.10)" : "rgba(14,72,120,0.10)";
    wrap((dx, dy) => { g.fillStyle = c; g.beginPath(); g.ellipse(x + dx, y + dy, rr * 1.8, rr * 0.6, 0, 0, 7); g.fill(); }); }
  g.lineCap = "round";
  for (let i = 0; i < 520; i++) { const x = Math.random() * 1024, y = Math.random() * 1024, l = 14 + Math.random() * 30, light = Math.random() < 0.6, bend = 4 + Math.random() * 3;
    wrap((dx, dy) => { g.strokeStyle = light ? "rgba(190,228,248,0.55)" : "rgba(12,60,104,0.45)"; g.lineWidth = light ? 2.2 : 1.6;
      g.beginPath(); g.moveTo(x + dx, y + dy); g.quadraticCurveTo(x + dx + l / 2, y + dy - bend, x + dx + l, y + dy); g.stroke(); }); }
  const t = new THREE.CanvasTexture(cv); t.wrapS = t.wrapT = THREE.RepeatWrapping; t.repeat.set(22, 22); t.colorSpace = THREE.SRGBColorSpace; t.anisotropy = 8;
  return t;
}
// The deck: grey steel plate with welded seams and a painted walkway border.
function deckTexture(w, d) {
  const cv = document.createElement("canvas"); cv.width = cv.height = 512; const g = cv.getContext("2d");
  g.fillStyle = "#6f6c69"; g.fillRect(0, 0, 512, 512);
  for (let i = 0; i < 500; i++) { const v = 96 + Math.random() * 30 | 0; g.fillStyle = `rgba(${v},${v},${v - 4},0.18)`; g.fillRect(Math.random() * 512, Math.random() * 512, 6 + Math.random() * 40, 2 + Math.random() * 6); }
  g.strokeStyle = "rgba(30,28,26,0.28)"; g.lineWidth = 1.5;
  for (let k = 0; k <= 512; k += 128) { g.beginPath(); g.moveTo(k, 0); g.lineTo(k, 512); g.stroke(); g.beginPath(); g.moveTo(0, k); g.lineTo(512, k); g.stroke(); }
  const t = new THREE.CanvasTexture(cv); t.wrapS = t.wrapT = THREE.RepeatWrapping; t.repeat.set(w / 12, d / 12); t.colorSpace = THREE.SRGBColorSpace; t.anisotropy = 8;
  return t;
}

// Ink lines from depth and surface-direction breaks (second difference of depth, so flat or evenly
// sloping ground never inks), drawn over the lit scene before tone mapping.
const OutlineShader = {
  uniforms: { tDiffuse: { value: null }, tNormal: { value: null }, tDepth: { value: null }, res: { value: new THREE.Vector2(1, 1) }, near: { value: 0.1 }, far: { value: 2000 }, view: { value: 60 } },
  vertexShader: "varying vec2 vUv; void main(){ vUv = uv; gl_Position = projectionMatrix * modelViewMatrix * vec4(position, 1.0); }",
  fragmentShader: `uniform sampler2D tDiffuse, tNormal, tDepth; uniform vec2 res; uniform float near, far, view; varying vec2 vUv;
    float lin(float d){ float z = d * 2.0 - 1.0; return (2.0 * near * far) / (far + near - z * (far - near)); }
    vec3 N(vec2 uv){ return texture2D(tNormal, uv).xyz * 2.0 - 1.0; }
    void main(){
      vec4 col = texture2D(tDiffuse, vUv); vec2 t = 1.5 / res;
      vec3 n0 = N(vUv); float d0 = lin(texture2D(tDepth, vUv).x);
      vec2 o[4]; o[0] = vec2(t.x, 0.0); o[1] = vec2(-t.x, 0.0); o[2] = vec2(0.0, t.y); o[3] = vec2(0.0, -t.y);
      float ne = 0.0, dd[4];
      for (int i = 0; i < 4; i++) { ne += 1.0 - dot(n0, N(vUv + o[i])); dd[i] = lin(texture2D(tDepth, vUv + o[i]).x); }
      float de = (abs(dd[0] + dd[1] - 2.0 * d0) + abs(dd[2] + dd[3] - 2.0 * d0)) / d0;
      // silhouettes everywhere in view; interior crease lines only on things close to the camera
      float fade = 1.0 - smoothstep(view * 1.6, view * 3.2, d0);
      float crease = smoothstep(0.55, 0.95, ne) * (1.0 - smoothstep(view * 0.7, view * 1.1, d0));
      float e = max(crease, smoothstep(0.03, 0.08, de)) * fade;
      gl_FragColor = vec4(mix(col.rgb, vec3(0.03, 0.025, 0.02), e), col.a);
    }`,
};

function initRenderer() {
  renderer = new THREE.WebGLRenderer({ antialias: true });
  renderer.setPixelRatio(Math.min(2, window.devicePixelRatio || 1));
  renderer.shadowMap.enabled = true; renderer.shadowMap.type = THREE.PCFShadowMap;
  renderer.toneMapping = THREE.ACESFilmicToneMapping; renderer.toneMappingExposure = 1.0;
  // graphic-novel daylight: warm hard sun with crisp shadows over a grey world, distant haze
  scene = new THREE.Scene(); scene.background = new THREE.Color(0xa9a49d);
  scene.fog = new THREE.Fog(0xa9a49d, 90, 420);
  camera = new THREE.PerspectiveCamera(42, 1, 0.5, 2000);
  hemiLight = new THREE.HemisphereLight(0xfff3df, 0x5a5048, 0.95); scene.add(hemiLight);
  keyLight = new THREE.DirectionalLight(0xfff1d6, 3.0); keyLight.position.set(-40, 60, 32);
  keyLight.castShadow = true; keyLight.shadow.mapSize.set(4096, 4096);
  const sc = keyLight.shadow.camera; sc.left = -70; sc.right = 70; sc.top = 70; sc.bottom = -70; sc.near = 1; sc.far = 260; keyLight.shadow.bias = -0.0004;
  scene.add(keyLight);
  groundMesh = new THREE.Mesh(new THREE.PlaneGeometry(1200, 1200), inkMaterial({ map: gravelTexture() }, 0));
  groundMesh.rotation.x = -Math.PI / 2; groundMesh.receiveShadow = true; scene.add(groundMesh);
  assetGroup = new THREE.Group(); scene.add(assetGroup);
  envGroup = new THREE.Group(); scene.add(envGroup);
  normalMat = new THREE.MeshNormalMaterial({ side: THREE.DoubleSide });
  nrmT = new THREE.WebGLRenderTarget(1, 1, { type: THREE.HalfFloatType }); nrmT.depthTexture = new THREE.DepthTexture(1, 1);
  composer = new EffectComposer(renderer);
  composer.addPass(new RenderPass(scene, camera));
  outlinePass = new ShaderPass(OutlineShader);
  outlinePass.uniforms.tNormal.value = nrmT.texture; outlinePass.uniforms.tDepth.value = nrmT.depthTexture;
  outlinePass.uniforms.near.value = camera.near; outlinePass.uniforms.far.value = camera.far;
  composer.addPass(outlinePass); composer.addPass(new OutputPass());
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
  el.addEventListener("pointerleave", () => { hovered = null; });
  el.addEventListener("click", onClick);
}

// Between the asset scene and the map: once zoomed out past 1.5x the initial distance, tell the app the
// view is now the whole facility (and back again when zooming in).
function applyStage() {
  const want = radius >= initRadius * 1.5;
  if (want === facilityStage) return;
  facilityStage = want;
  if (onStage) onStage(want ? "facility" : "asset");
}

function pickAt(e) {
  const r = renderer.domElement.getBoundingClientRect();
  mouse.x = ((e.clientX - r.left) / r.width) * 2 - 1;
  mouse.y = -((e.clientY - r.top) / r.height) * 2 + 1;
  ray.setFromCamera(mouse, camera); ray.layers.set(1);
  const hits = ray.intersectObjects(pickables, false);
  for (const h of hits) { let o = h.object; while (o && !o.userData.asset) o = o.parent; if (o) return o; }
  return null;
}

function onHover(e) {   // the pointer becomes a hand over an asset (no tooltip: the tag already names it)
  const o = pickAt(e);
  renderer.domElement.style.cursor = o ? "pointer" : (drag ? "grabbing" : "grab");
  hovered = o && o.userData.asset ? o : null;
}

function onClick(e) {
  if (Math.hypot(e.clientX - downX, e.clientY - downY) > 5) return;  // that was a drag-to-orbit, not a click
  const o = pickAt(e);
  if (o && o.userData.asset) { selectedId = o.userData.asset.asset_id; let g = o; while (g && !assetRoots.includes(g)) g = g.parent; visit(g); if (onPick) onPick(selectedId); }
  else { selectedId = null; hideBubble(); if (onEmpty) onEmpty(); }   // clicked empty ground / sky / environment: the bubble goes too
}

// Everything the outline pass must not ink (tags, status balls, flames, wire fence, foliage, far hills),
// collected once per site instead of walking the whole scene every frame.
function collectInkSkip() { inkSkip = []; scene.traverse(o => { if (o.isSprite || o.userData.noInk) inkSkip.push(o); }); }

function clearAssets() {
  tagBalls = []; pickables = []; assetRoots = []; flames = []; inkSkip = [];
  assetGroup.children.slice().forEach(c => {
    c.traverse(o => { if (o.geometry) o.geometry.dispose(); if (o.material) { if (o.material.map) o.material.map.dispose(); o.material.dispose(); } });
    assetGroup.remove(c);
  });
}

// Animated touches: the flare flames flicker.
function updateColour() {
  const t = performance.now() / 1000;
  for (const f of flames) {
    const gust = 0.5 + 0.25 * Math.sin(t * 0.7 + f.p) + 0.1 * Math.sin(t * 2.3 + f.p);   // the wind bends the flame downwind (WIND)
    const wx = WIND.x, wz = WIND.z, cx = -wz, cz = wx;   // downwind, and across it for the flicker
    for (const q of f.parts) {
      if (q.smoke) {   // smoke rises off the flame and drifts away, fading
        const a = (t * 0.25 + q.k + q.p) % 1, h = 3.5 + a * 9;
        q.sp.position.set(wx * gust * h * 0.8 + cx * Math.sin(q.p) * 0.6, h, wz * gust * h * 0.8 + cz * Math.sin(q.p) * 0.6); q.sp.scale.setScalar(1.4 + a * 4); q.sp.material.opacity = 0.5 * (1 - a);
        continue;
      }
      const fl = 1 + 0.22 * Math.sin(t * 13 + q.p) + 0.12 * Math.sin(t * 29 + q.p * 3), h = (0.5 + q.k * 4.6) * fl;
      const sw = Math.sin(t * 9 + q.p) * 0.15, sc = Math.cos(t * 7 + q.p) * 0.15;
      q.sp.position.set(wx * (gust * h * 0.75 + sw) + cx * sc, h, wz * (gust * h * 0.75 + sw) + cz * sc);
      const w = q.w * (1 - q.k * 0.6) * fl; q.sp.scale.set(w, w * 1.4, 1); q.sp.material.opacity = 0.8 + 0.15 * Math.sin(t * 17 + q.p);
    }
  }
}
function renderInk() {
  // pass 1: normals + depth of the solid geometry (no tags / status balls) for the outline edge test
  const hidden = [];
  for (const o of inkSkip) if (o.visible) { o.visible = false; hidden.push(o); }
  const bg = scene.background; scene.background = null; scene.overrideMaterial = normalMat;
  renderer.setRenderTarget(nrmT); renderer.clear(); renderer.render(scene, camera);
  scene.overrideMaterial = null; scene.background = bg; renderer.setRenderTarget(null);
  hidden.forEach(o => { o.visible = true; });
  // pass 2: the lit scene, with dark ink lines drawn where depth or surface direction breaks
  composer.render();
}
function animate() {
  raf = requestAnimationFrame(animate);
  if (!ready || !renderer.domElement.isConnected || !container.offsetParent) { engClock.getDelta(); return; }   // nothing to draw yet, or the view is hidden
  const dt = Math.min(0.05, engClock.getDelta()) * (window.__slow ? 0.45 : 1); /* __slow: test hook for frame-by-frame checks */ updateTagBalls(); updateColour(); updateSea(performance.now() / 1000); updateEngineer(dt); if (followR && eng) { target.set(eng.position.x, eng.position.y + 1.2, eng.position.z); radius = followR; updateCam(); } renderInk(); }

function resize() {
  if (!container || !renderer) return;
  const w = container.clientWidth, h = container.clientHeight;
  renderer.setSize(w, h, false); camera.aspect = w / Math.max(1, h); camera.updateProjectionMatrix();
  const pr = renderer.getPixelRatio();
  if (composer) { composer.setSize(w, h); nrmT.setSize(w * pr, h * pr); outlinePass.uniforms.res.value.set(w * pr, h * pr); }
}

const Twin3D = {
  async load(containerSel, assets, onPickCb, opts) {
    container = document.querySelector(containerSel);
    if (!container) return;
    onPick = onPickCb;
    onZoomOut = (opts && opts.onZoomOut) || null;
    onStage = (opts && opts.onStage) || null;
    onEmpty = (opts && opts.onEmpty) || null;
    if (!renderer) {
      initRenderer();
      container.appendChild(renderer.domElement);
      window.addEventListener("resize", resize);
      new ResizeObserver(() => resize()).observe(container);   // follows the layout as the menu slides in or out
      animate();
    } else if (renderer.domElement.parentNode !== container) {
      container.appendChild(renderer.domElement);
    }
    renderer.domElement.style.width = "100%"; renderer.domElement.style.height = "100%";
    const seq = ++loadSeq; selectedId = null;
    clearAssets(); clearEnv(); resize();
    setSetting(assets.some(a => /^(PLT|MNF)/.test(a.area_code || "")));   // offshore platforms and manifolds stand in the sea
    // the engineer and his thought bubble leave with the old site; he comes back at the gate of the new one
    hideBubble(); engTrip = null; engFrozen = false; walk = null;
    if (eng) scene.remove(eng); if (hat) scene.remove(hat);
    // start every download for this site at once: equipment models, scenery and the engineer
    const files = [...new Set(assets.map(a => MODELS[a.equipment_type]).filter(Boolean).map(m => m[0]))];
    const equipP = Promise.all(files.map(f => { const m = Object.values(MODELS).find(v => v[0] === f); return loadModel(f, m[1], m[2]); }));
    const propsP = loadProps(sea), engP = loadEngineer();
    const tpl = {}; (await equipP).forEach((t, i) => { tpl[files[i]] = t; });
    if (seq !== loadSeq) return;   // the user switched site while models were loading
    const groups = assets.map(a => { const m = MODELS[a.equipment_type]; return equip(a, m ? tpl[m[0]] : null); });
    // grid spacing fits the largest footprint on the site, with a walkway between units
    const n = assets.length, cols = Math.max(1, Math.ceil(Math.sqrt(n))), rows = Math.max(1, Math.ceil(n / cols));
    const gap = Math.max(11, Math.max(...groups.map(g => g.userData.foot || 8)) + 6);
    const placed = [];
    groups.forEach((g, i) => {
      const c = i % cols, r = Math.floor(i / cols);
      const px = (c - (cols - 1) / 2) * gap, pz = (r - (rows - 1) / 2) * gap;
      g.position.set(px, 0, pz); assetGroup.add(g); assetRoots.push(g); placed.push([px, pz]);
    });
    const span = Math.max(cols, rows) * gap;
    const sc = keyLight.shadow.camera, ext = span / 2 + 60; sc.left = -ext; sc.right = ext; sc.top = ext; sc.bottom = -ext; sc.far = 400; sc.updateProjectionMatrix();
    target.set(0, 3, sea ? 8 : 0);                // a platform deck runs on past the equipment (quarters, helideck): centre on all of it
    const whole = Math.max(30, span * 1.05);      // close enough to read every tag
    radius = Math.max(70, (span / 2 + 22) * 2.4); theta = 0.85; phi = 1.0;   // open on the complete site: pad (or deck), fence and scenery
    initRadius = whole; facilityStage = false;    // zoom LOD baseline for this scene
    maxRadius = Math.max(radius * 1.25, whole * 1.9); overscroll = 0;     // hard stop just past the complete view
    if (onStage) onStage("asset");                // reset the stage hint for the fresh scene
    updateCam(); resize(); collectInkSkip(); ready = true;   // equipment on screen now
    await Promise.all([propsP, engP]);
    if (seq !== loadSeq) return;
    await buildEnvironment(assets, span, placed, seq);
    if (seq !== loadSeq) return;
    lastE = span / 2 + 9; placeEngineer(lastE); collectInkSkip();
  },
  // empty the view straight away when another site is chosen (load() fills it once the site's data arrives)
  clear() {
    if (!renderer) return;
    ++loadSeq; selectedId = null; clearAssets(); clearEnv();
    hideBubble(); engTrip = null; engFrozen = false; walk = null;
    if (eng) scene.remove(eng); if (hat) scene.remove(hat);
  },
  // send the engineer to an asset by id (same as clicking it)
  // test hooks (used by the headless checks): put him back at the gate, have the camera follow him, read his state
  _home() { placeEngineer(lastE); },
  _wind() { const ws = envGroup && envGroup.children.find(o => o.userData.kind === "windsock"); if (!ws) return { wind: WIND.toArray() };
    ws.updateMatrixWorld(true); const b = new THREE.Box3().setFromObject(ws), yc = b.min.y + (b.max.y - b.min.y) * 0.6, v = new THREE.Vector3(); let tx = 0, tz = 0, tn = 0, fx = 0, fz = 0, fn = 0;
    ws.traverse(m => { if (!m.isMesh) return; const p = m.geometry.attributes.position; for (let i = 0; i < p.count; i += 3) { v.fromBufferAttribute(p, i).applyMatrix4(m.matrixWorld); if (v.y > yc) { tx += v.x; tz += v.z; tn++; } else if (v.y < b.min.y + 0.8) { fx += v.x; fz += v.z; fn++; } } });
    return { wind: WIND.toArray().map(x => +x.toFixed(2)), sock: [+(tx / tn - fx / fn).toFixed(2), +(tz / tn - fz / fn).toFixed(2)] }; },
  _view(th, ph, r, ty) { followR = 0; target.set(0, ty || 3, 0); theta = th; phi = ph; radius = r; updateCam(); },
  _info() { return { frames: renderer ? renderer.info.render.frame : 0, ready, assets: assetRoots.length, props: envGroup ? envGroup.children.length : 0 }; },
  _follow(r) { followR = r || 0; },
  _cam(th, ph, r, y) { followR = 0; if (eng) target.set(eng.position.x, (y || 2.3) * eng.scale.y / 1.45 + eng.position.y, eng.position.z); theta = th; phi = ph; radius = r; updateCam(); },
  _hands() { const f = v => v.toArray().map(x => +x.toFixed(3)); const a = engActs.hat_throw;
    return { t: a ? +a.time.toFixed(3) : null, w: a ? +a.getEffectiveWeight().toFixed(2) : null, R: handR && f(handR.getWorldPosition(new THREE.Vector3())), L: handL && f(handL.getWorldPosition(new THREE.Vector3())), H: headEnd && f(headEnd.getWorldPosition(new THREE.Vector3())), hat: hat && f(hat.position), held: !!(hat && hat.userData.hand), onHead: !!(hat && hat.userData.onHead), stage: engTrip && engTrip.stage, since: engTrip && +(engTrip.since || 0).toFixed(2) }; },
  _headInfo() { if (!eng || !headBone) return null; const h = headBone.getWorldPosition(new THREE.Vector3()), e = headEnd.getWorldPosition(new THREE.Vector3());
    let hb = new THREE.Box3(); eng.traverse(o => { if (o.isSkinnedMesh) { o.geometry.computeBoundingBox(); } });
    const hatB = new THREE.Box3().setFromObject(hat); return { head: h.toArray(), headEnd: e.toArray(), hatSize: hatB.getSize(new THREE.Vector3()).toArray(), hatPos: hat.position.toArray() }; },
  _eng() { if (!eng) return { eng }; const h = new THREE.Vector3(); if (headBone) headBone.getWorldPosition(h); const bb = new THREE.Box3().setFromObject(eng); return { pos: eng.position.toArray(), head: h.toArray(), box: [bb.min.toArray(), bb.max.toArray()], act: engAct && engAct.getClip().name, trip: engTrip && { move: engTrip.move, t: engTrip.t, T: engTrip.T, stage: engTrip.stage, path: engTrip.path && engTrip.path.map(v => [+v.x.toFixed(1), +v.z.toFixed(1)]) }, boxes: walk && walk.boxes.map(x => [x.g.userData.asset.asset_id, +x.b.min.x.toFixed(1), +x.b.min.z.toFixed(1), +x.b.max.x.toFixed(1), +x.b.max.z.toFixed(1)]), cam: camera.position.toArray(), hat: hat && hat.position.toArray() }; },
  visit(assetId, move) { const g = assetRoots.find(x => x.userData.asset && x.userData.asset.asset_id === assetId); visit(g, move); },
  dispose() {
    if (raf) cancelAnimationFrame(raf); raf = null;
    if (renderer) { renderer.dispose(); if (renderer.domElement.parentNode) renderer.domElement.parentNode.removeChild(renderer.domElement); }
    renderer = null; scene = null; camera = null; assetGroup = null; envGroup = null;
  }
};

window.Twin3D = Twin3D;
