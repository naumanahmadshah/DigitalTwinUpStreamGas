"""Generate the site-view 3D models with Meshy (text to 3D), all with identical settings.

Reads prompts.json (a shared style block + one prompt per asset), runs a preview then a textured
refine for each asset, and saves app/static/models/<name>.glb plus <name>.png (Meshy's thumbnail).
Skips assets whose GLB already exists, so it is safe to re-run. The API key is read from
$MESHY_API_KEY or ~/.meshy_key and is never printed.

  python3 tools/meshy/generate.py compressor            # one asset
  python3 tools/meshy/generate.py --all                 # every asset in prompts.json
  python3 tools/meshy/generate.py compressor --style-image app/static/models/compressor.png
"""
import argparse, base64, json, os, sys, time, urllib.request

API = "https://api.meshy.ai/openapi"
HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "..", "..", "app", "static", "models")


def key():
    k = os.environ.get("MESHY_API_KEY")
    if not k:
        with open(os.path.expanduser("~/.meshy_key")) as f:
            k = f.read().strip()
    return k


def call(method, path, body=None):
    req = urllib.request.Request(API + path, method=method, data=json.dumps(body).encode() if body else None,
                                 headers={"Authorization": "Bearer " + key(), "Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=120) as r:
        return json.loads(r.read() or b"{}")


def wait(task_id, label):
    while True:
        t = call("GET", f"/v2/text-to-3d/{task_id}")
        st = t.get("status")
        print(f"  {label}: {st} {t.get('progress', '')}%", flush=True)
        if st == "SUCCEEDED":
            return t
        if st in ("FAILED", "CANCELED", "EXPIRED"):
            raise SystemExit(f"{label} {st}: {t.get('task_error')}")
        time.sleep(15)


def fetch(url, path):
    with urllib.request.urlopen(url, timeout=300) as r, open(path, "wb") as f:
        f.write(r.read())


def generate(name, spec, style, style_image=None):
    glb = os.path.join(OUT, name + ".glb")
    if os.path.exists(glb):
        print(f"{name}: exists, skipped"); return
    prompt = f"{spec['prompt']} {style}"
    assert len(prompt) <= 800, f"{name}: prompt is {len(prompt)} characters (max 800)"
    print(f"{name}: preview", flush=True)
    pre = call("POST", "/v2/text-to-3d", {
        "mode": "preview", "prompt": prompt, "ai_model": "latest",
        "should_remesh": True, "topology": "triangle", "target_polycount": spec.get("polycount", 15000),
        "pose_mode": spec.get("pose", ""), "target_formats": ["glb"]})["result"]
    wait(pre, name + " preview")
    ref = {"mode": "refine", "preview_task_id": pre, "enable_pbr": True, "texture_resolution": "2k",
           "target_formats": ["glb"]}
    if style_image:
        with open(style_image, "rb") as f:
            ref["texture_image_url"] = "data:image/png;base64," + base64.b64encode(f.read()).decode()
    else:
        ref["texture_prompt"] = style
    print(f"{name}: refine (textures)", flush=True)
    t = wait(call("POST", "/v2/text-to-3d", ref)["result"], name + " refine")
    fetch(t["model_urls"]["glb"], glb)
    if t.get("thumbnail_url"):
        fetch(t["thumbnail_url"], os.path.join(OUT, name + ".png"))
    with open(os.path.join(HERE, "tasks.log"), "a") as f:
        f.write(json.dumps({"asset": name, "preview": pre, "refine": t["id"], "at": time.strftime("%Y-%m-%d %H:%M")}) + "\n")
    print(f"{name}: saved {os.path.getsize(glb) / 1e6:.1f} MB", flush=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("names", nargs="*")
    ap.add_argument("--all", action="store_true")
    ap.add_argument("--style-image", help="reference image for the textures, so every asset matches it")
    a = ap.parse_args()
    cfg = json.load(open(os.path.join(HERE, "prompts.json")))
    names = list(cfg["assets"]) if a.all else a.names
    os.makedirs(OUT, exist_ok=True)
    print("credits before:", call("GET", "/v1/balance").get("balance"))
    for n in names:
        generate(n, cfg["assets"][n], cfg["style"], a.style_image)
    print("credits after:", call("GET", "/v1/balance").get("balance"))


if __name__ == "__main__":
    main()
