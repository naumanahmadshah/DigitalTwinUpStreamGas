"""Turn an approved concept image into a textured 3D model (Meshy multi-view + multi-image-to-3D).

1. Paints the concept from several angles (image-to-image, generate_multi_view) so the back is modelled, not guessed.
2. Builds the model from the concept plus those views (multi-image-to-3D), textured from the concept itself, keeping
   the painted comic shading (remove_lighting off, no PBR re-lighting).
3. Saves app/static/models/<name>.glb (and the views next to the concept).

  python3 tools/meshy/build3d.py compressor --concept <concept.png> [--polycount 30000]
"""
import argparse, json, os, sys, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from generate import call, fetch, HERE, OUT
from concept import uri


def wait(path, label, every=10):
    while True:
        t = call("GET", path)
        print(f"  {label}: {t['status']} {t.get('progress', '')}%", flush=True)
        if t["status"] == "SUCCEEDED":
            return t
        if t["status"] in ("FAILED", "CANCELED", "EXPIRED"):
            raise SystemExit(f"{label} {t['status']}: {t.get('task_error')}")
        time.sleep(every)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("name"); ap.add_argument("--concept", required=True)
    ap.add_argument("--polycount", type=int, default=30000)
    ap.add_argument("--out-name", default=None)
    a = ap.parse_args()
    out_name = a.out_name or a.name
    vdir = os.path.dirname(os.path.abspath(a.concept))
    print("credits before:", call("GET", "/v1/balance").get("balance"))
    mv = call("POST", "/v1/image-to-image", {"ai_model": "nano-banana-pro", "generate_multi_view": True,
          "prompt": "The same object from the reference image, identical design, colours and art style, shown from multiple angles (front, side and back), plain light grey background, no text.",
          "reference_image_urls": [uri(a.concept)]})["result"]
    t = wait(f"/v1/image-to-image/{mv}", "multi-view")
    views = []
    for i, u in enumerate(t["image_urls"], 1):
        p = os.path.join(vdir, f"{a.name}_view_{i}.png"); fetch(u, p); views.append(p); print("  saved", p)
    body = {"image_urls": [uri(a.concept)] + [uri(v) for v in views][:3], "ai_model": "latest", "should_texture": True,
            "enable_pbr": False, "remove_lighting": False, "texture_image_url": uri(a.concept),
            "should_remesh": True, "topology": "triangle", "target_polycount": a.polycount, "target_formats": ["glb"]}
    m = call("POST", "/v1/multi-image-to-3d", body)["result"]
    t = wait(f"/v1/multi-image-to-3d/{m}", "3D model", 15)
    glb = os.path.join(OUT, out_name + ".glb"); fetch(t["model_urls"]["glb"], glb)
    if t.get("thumbnail_url"):
        fetch(t["thumbnail_url"], os.path.join(OUT, out_name + ".png"))
    with open(os.path.join(HERE, "tasks.log"), "a") as f:
        f.write(json.dumps({"asset": out_name, "multiview": mv, "model": m, "concept": a.concept, "at": time.strftime("%Y-%m-%d %H:%M")}) + "\n")
    print(f"saved {glb} {os.path.getsize(glb) / 1e6:.1f} MB")
    print("credits after:", call("GET", "/v1/balance").get("balance"))


if __name__ == "__main__":
    main()
