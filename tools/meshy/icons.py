"""Menu icons (Meshy text-to-image): flat hand-drawn doodle line art in the app's blue-flame colours.

The look is described in words (no reference image): confident black ink outlines, flat fills, small doodle
sparkles, lots of white space; colours limited to the loading flame's blues and cyans. Two options per menu
item, transparent background, saved to <out>/<key>_<n>.png.
  python3 tools/meshy/icons.py --out <dir> [--n 2]
"""
import argparse, json, os, sys, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from generate import call, fetch, HERE

STYLE = ("A single flat hand-drawn doodle icon in a modern editorial line-illustration style: confident, slightly uneven black ink "
         "outlines with a hand-drawn feel, flat solid colour fills with no gradients and no shading, simple chunky shapes, two or "
         "three tiny doodle sparkles, asterisks or little open circles floating around it, generous white space. Colour palette "
         "strictly limited to shades of electric blue and cyan like a gas flame (#0090F0, #48C0F0, #78F0F0, #90D8F0) plus the "
         "black outlines and white; no other colours. Centred, square, no text, no letters, no numbers, no people. Subject: ")
ICONS = {
    "command_centre": "a control-room dashboard monitor on a small stand showing a dial gauge, a little bar chart and a wavy trend line, with a small alert bell above it.",
    "assets_map": "a folded paper map with three location pins and a dotted route line between them.",
    "gas_flow_chain": "a short gas pipeline running from a small wellhead with a valve wheel to a gas flame, with arrows showing the flow along the pipe.",
    "analytics": "a bar chart and a line chart on a card, with a magnifying glass looking at the rising line.",
    "recommendation_inbox": "an inbox tray holding a card with a glowing light bulb and a tick mark on it.",
    "asset_explorer": "an open folder with documents and an equipment tag with a barcode hanging from it on a string.",
}

# The five value-chain pictures on the Command Centre, in the same style.
STOPS = {
    "stop_reservoir": "an underground gas reservoir: a cutaway of layered rock strata with a pocket of gas bubbles trapped under a dome, a dotted line rising to the surface.",
    "stop_wells": "a gas wellhead christmas tree with stacked valves and two round valve wheels, standing on the ground with a small flowline leaving sideways.",
    "stop_gathering": "a gathering pipeline manifold: several short pipes joining into one larger pipe, with a valve wheel and flow arrows.",
    "stop_processing": "a gas processing plant: two tall process columns and a horizontal vessel joined by pipes, with a small flame on a stack.",
    "stop_custody": "a custody transfer gas meter skid: a meter with a round dial gauge on a pipe run between two valves, with a small handover tick mark.",
}


def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--out", required=True); ap.add_argument("--n", type=int, default=2)
    ap.add_argument("--set", choices=["menu", "stops"], default="menu")
    a = ap.parse_args(); os.makedirs(a.out, exist_ok=True)
    print("credits before:", call("GET", "/v1/balance").get("balance"), flush=True)
    jobs = [(k, i, call("POST", "/v1/text-to-image", {"ai_model": "nano-banana-pro", "prompt": STYLE + v, "aspect_ratio": "1:1",
                                                       "remove_background": True})["result"])
            for k, v in (STOPS if a.set == "stops" else ICONS).items() for i in range(1, a.n + 1)]
    for k, i, tid in jobs:
        while True:
            t = call("GET", f"/v1/text-to-image/{tid}")
            if t["status"] in ("SUCCEEDED", "FAILED", "CANCELED", "EXPIRED"):
                break
            time.sleep(6)
        if t["status"] != "SUCCEEDED":
            print(k, i, t["status"], t.get("task_error"), flush=True); continue
        p = os.path.join(a.out, f"{k}_{i}.png"); fetch(t["image_urls"][0], p); print("saved", os.path.basename(p), flush=True)
    print("credits after:", call("GET", "/v1/balance").get("balance"), flush=True)


if __name__ == "__main__":
    main()
