// Pack the rigged engineer and every move into one GLB for the app: the rigged model, plus each
// anim_<name>.glb's animation retargeted onto it by bone name and renamed <name>. Then compress it
// (meshopt geometry, 1024px WebP textures) with gltf-transform, and compress the hard hat too.
//   GLTF_TRANSFORM_DIR=<dir with node_modules/@gltf-transform> node tools/meshy/pack_character.mjs <in dir> <out dir>
import fs from "fs"; import path from "path"; import { execFileSync } from "child_process";
const G = process.env.GLTF_TRANSFORM_DIR;
const { NodeIO } = await import(path.join(G, "node_modules/@gltf-transform/core/dist/index.js"));
const { ALL_EXTENSIONS } = await import(path.join(G, "node_modules/@gltf-transform/extensions/dist/index.js"));
const [src, out] = process.argv.slice(2);
const io = new NodeIO().registerExtensions(ALL_EXTENSIONS);
const base = await io.read(path.join(src, "engineer_rigged.glb"));
const root = base.getRoot(), buf = root.listBuffers()[0];
root.listAnimations().forEach(a => a.dispose());                 // keep only the moves we name below
const byName = new Map(root.listNodes().map(n => [n.getName(), n]));
for (const f of fs.readdirSync(src).filter(f => /^anim_.+\.glb$/.test(f)).sort()) {
  const name = f.replace(/^anim_|\.glb$/g, ""), d = await io.read(path.join(src, f));
  const anim = d.getRoot().listAnimations()[0]; if (!anim) { console.log("no animation in", f); continue; }
  const na = base.createAnimation(name); let n = 0;
  for (const ch of anim.listChannels()) {
    const tgt = byName.get(ch.getTargetNode()?.getName()); if (!tgt) continue;
    const s = ch.getSampler(), copy = a => base.createAccessor().setArray(a.getArray().slice()).setType(a.getType()).setBuffer(buf);
    const ns = base.createAnimationSampler().setInput(copy(s.getInput())).setOutput(copy(s.getOutput())).setInterpolation(s.getInterpolation());
    na.addSampler(ns).addChannel(base.createAnimationChannel().setTargetNode(tgt).setTargetPath(ch.getTargetPath()).setSampler(ns)); n++;
  }
  console.log("move", name, n, "channels");
}
const tmp = path.join(src, "engineer_packed.glb"); await io.write(tmp, base);
const cli = path.join(G, "node_modules/.bin/gltf-transform");
execFileSync(cli, ["optimize", tmp, path.join(out, "engineer.glb"), "--compress", "meshopt", "--texture-compress", "webp", "--texture-size", "1024"], { stdio: "inherit" });
execFileSync(cli, ["optimize", path.join(src, "hat.glb"), path.join(out, "hard_hat.glb"), "--compress", "meshopt", "--texture-compress", "webp", "--texture-size", "512"], { stdio: "inherit" });
