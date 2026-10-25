"""Paint 2D concept art for an asset in the reference art style (Meshy image-to-image).

Feeds the style reference frames plus a prompt to Meshy and saves the concept images, which are then
turned into 3D with image-to-3D (generate.py --from-image). Key handling as in generate.py.

  python3 tools/meshy/concept.py compressor --refs ref1.png ref2.png --n 3 --out <dir>
"""
import argparse, base64, json, os, sys, time, urllib.request
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from generate import call, fetch, HERE

STYLE_ASK = ("Using exactly the art style of the reference images (a stylized graphic-novel 3D game look: hand-painted "
             "textures, bold dark ink outlines, cel-shaded comic lighting, chunky slightly exaggerated proportions, "
             "worn weathered surfaces), paint a single full-colour asset in the same style as the compressor and the "
             "treasure chest in the references: {subject} Three-quarter view from slightly above, the whole {what} visible "
             "and centred, plain light grey studio background, {only}, no text, no letters, no logos.")
ONLY = {"person": ("figure", "only this one person and no other objects"),
        "equipment": ("object", "no people, no other objects"), "surroundings": ("object", "no people, no other objects")}


def uri(p):
    ext = "png" if p.lower().endswith(".png") else "jpeg"
    with open(p, "rb") as f:
        return f"data:image/{ext};base64," + base64.b64encode(f.read()).decode()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("names", nargs="*"); ap.add_argument("--all-except", nargs="*", default=None)
    ap.add_argument("--refs", nargs="+", required=True)
    ap.add_argument("--n", type=int, default=2); ap.add_argument("--out", required=True)
    ap.add_argument("--model", default="nano-banana-pro")
    a = ap.parse_args()
    cfg = json.load(open(os.path.join(HERE, "prompts.json")))
    names = a.names or [k for k in cfg["assets"] if k not in (a.all_except or [])]
    refs = [uri(r) for r in a.refs]
    os.makedirs(a.out, exist_ok=True)
    print("credits before:", call("GET", "/v1/balance").get("balance"))
    jobs = []   # submit everything first, then collect: the tasks run in parallel on Meshy's side
    for n in names:
        spec = cfg["assets"][n]
        what, only = ONLY[spec.get("kind", "equipment")]
        prompt = STYLE_ASK.format(subject=spec.get("concept", spec["prompt"]), what=what, only=only)
        for i in range(1, a.n + 1):
            jobs.append((n, i, call("POST", "/v1/image-to-image", {"ai_model": a.model, "prompt": prompt,
                                                                     "reference_image_urls": refs, "aspect_ratio": "4:3"})["result"]))
    for n, i, tid in jobs:
        while True:
            t = call("GET", f"/v1/image-to-image/{tid}")
            if t["status"] in ("SUCCEEDED", "FAILED", "CANCELED", "EXPIRED"):
                break
            time.sleep(8)
        if t["status"] != "SUCCEEDED":
            print(f"{n} concept {i}: {t['status']} {t.get('task_error')}", flush=True); continue
        p = os.path.join(a.out, f"{n}_concept_{i}.png"); fetch(t["image_urls"][0], p)
        with open(os.path.join(HERE, "tasks.log"), "a") as f:
            f.write(json.dumps({"asset": n, "concept": i, "task": tid, "file": p, "at": time.strftime("%Y-%m-%d %H:%M")}) + "\n")
        print("saved", os.path.basename(p), flush=True)
    print("credits after:", call("GET", "/v1/balance").get("balance"))


if __name__ == "__main__":
    main()
