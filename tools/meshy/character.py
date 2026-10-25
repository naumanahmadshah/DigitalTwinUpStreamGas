"""Build the animated site engineer: concept art -> 3D (A-pose) -> rig -> animations, plus his hard hat.

Steps (each saves its result and is skipped when that file already exists, so re-runs resume):
  concept   paint the engineer (A-pose, front view, no hat) and the hard hat in the reference style
  model     image-to-3D for both (the engineer in A-pose so he can be rigged)
  rig       auto-rig the engineer (Meshy rigging)
  anims     apply library moves (run, flips, skip, walks, idle, head scratch, angry) and custom text-to-motion moves
            (moonwalk, Superman flight, take hat off and throw it down)
Outputs land in <out>; tools/meshy/pack_character.mjs then packs them for the app.
"""
import argparse, base64, json, os, sys, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from generate import call, fetch, HERE
from concept import uri

STYLE = ("Using exactly the art style of the reference images (a stylized graphic-novel 3D game look: hand-painted textures, "
         "bold dark ink outlines, cel-shaded comic lighting, chunky slightly exaggerated proportions, worn weathered surfaces), ")
ENGINEER = (STYLE + "paint one full-body gas field engineer character for rigging: a man in orange hi-vis coveralls with reflective "
            "stripes, safety glasses, gloves and steel-cap boots, short hair and NO hat, standing in a neutral A-pose with arms angled "
            "down and away from the body and legs slightly apart, facing straight to the front, full body visible head to toe, "
            "plain light grey background, nothing else in the image, no text.")
HAT = (STYLE + "paint a single white industrial safety hard hat with a short brim and a small chin strap, slightly scuffed, "
       "three-quarter view from slightly above, plain light grey background, nothing else, no text.")
LIBRARY = {"run": 509, "sprint_roll_flip": 401, "run_jump_roll": 459, "walk_backward": 544, "skip": 118, "funky_walk": 554,
           "sneaky_walk": 559, "idle": 0, "head_scratch": 36, "angry_stomp": 26, "shout_angry": 51, "talk_angry": 311, "throw_down": 389}
MOTIONS = {"moonwalk": ("A smooth moonwalk dance: the person glides backwards in place-like sliding steps, one heel lifted at a time, "
                        "arms relaxed and slightly bent, stylish and cool.", 4.0),
           "superman": ("Flying like Superman: the body is stretched out horizontally face down, one fist punched straight forward "
                        "ahead of the head, the other arm along the body, legs straight together, a slight bobbing glide.", 4.0),
           "hat_throw": ("An angry worker turns to face forward, grabs the hard hat off his head with his right hand and slams it hard down "
                         "onto the ground in front of him, then stands with fists clenched, furious.", 3.5)}


def wait(path, label, every=8):
    while True:
        t = call("GET", path)
        if t["status"] == "SUCCEEDED":
            return t
        if t["status"] in ("FAILED", "CANCELED", "EXPIRED"):
            raise SystemExit(f"{label} {t['status']}: {t.get('task_error')}")
        time.sleep(every)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--refs", nargs="+", required=True); ap.add_argument("--out", required=True)
    a = ap.parse_args(); os.makedirs(a.out, exist_ok=True)
    st_path = os.path.join(a.out, "state.json")
    st = json.load(open(st_path)) if os.path.exists(st_path) else {}
    save = lambda: json.dump(st, open(st_path, "w"), indent=1)
    print("credits before:", call("GET", "/v1/balance").get("balance"), flush=True)
    refs = [uri(r) for r in a.refs]
    # 1. concepts
    for key, prompt in (("engineer", ENGINEER), ("hat", HAT)):
        p = os.path.join(a.out, f"{key}_concept.png")
        if not os.path.exists(p):
            tid = call("POST", "/v1/image-to-image", {"ai_model": "nano-banana-pro", "prompt": prompt, "reference_image_urls": refs,
                                                      "aspect_ratio": "3:4" if key == "engineer" else "1:1"})["result"]
            fetch(wait(f"/v1/image-to-image/{tid}", key + " concept")["image_urls"][0], p); print("concept", key, flush=True)
    # 2. 3D models
    for key, pose, pc in (("engineer", "a-pose", 20000), ("hat", "", 3000)):
        p = os.path.join(a.out, f"{key}.glb")
        if not os.path.exists(p):
            tid = call("POST", "/v1/image-to-3d", {"image_url": uri(os.path.join(a.out, f"{key}_concept.png")), "ai_model": "latest",
                        "should_texture": True, "enable_pbr": False, "remove_lighting": False, "pose_mode": pose,
                        "should_remesh": True, "topology": "triangle", "target_polycount": pc, "target_formats": ["glb"]})["result"]
            t = wait(f"/v1/image-to-3d/{tid}", key + " 3D", 15); fetch(t["model_urls"]["glb"], p)
            st[key + "_task"] = tid; save(); print("model", key, flush=True)
    # 3. rig the engineer
    if "rig_task" not in st:
        rid = call("POST", "/v1/rigging", {"input_task_id": st["engineer_task"], "height_meters": 1.8})["result"]
        t = wait(f"/v1/rigging/{rid}", "rig", 10)
        fetch(t["result"]["rigged_character_glb_url"], os.path.join(a.out, "engineer_rigged.glb"))
        st["rig_task"] = rid; save(); print("rigged", flush=True)
    # 4a. library moves, one task per move so each lands in its own file
    for name, aid in LIBRARY.items():
        p = os.path.join(a.out, f"anim_{name}.glb")
        if os.path.exists(p):
            continue
        tid = call("POST", "/v1/animations", {"rig_task_id": st["rig_task"], "action_id": aid, "post_process": {"operation_type": "change_fps", "fps": 30}})["result"]
        st.setdefault("anim_tasks", {})[name] = tid; save()
    # 4b. custom moves from text
    for name, (prompt, dur) in MOTIONS.items():
        p = os.path.join(a.out, f"anim_{name}.glb")
        if os.path.exists(p) or name in st.get("anim_tasks", {}):
            continue
        mid = call("POST", "/v1/text-to-motion", {"prompt": prompt, "duration": dur, "mode": "prime"})["result"]
        wait(f"/v1/text-to-motion/{mid}", name + " motion", 10)
        tid = call("POST", "/v1/animations", {"rig_task_id": st["rig_task"], "motion_task_id": mid})["result"]
        st.setdefault("anim_tasks", {})[name] = tid; save(); print("motion", name, flush=True)
    for name, tid in st.get("anim_tasks", {}).items():
        p = os.path.join(a.out, f"anim_{name}.glb")
        if os.path.exists(p):
            continue
        t = wait(f"/v1/animations/{tid}", "anim " + name, 8)
        url = (t.get("result") or {}).get("animation_glb_url") or t.get("animation_glb_url")
        fetch(url, p); print("anim", name, flush=True)
    print("credits after:", call("GET", "/v1/balance").get("balance"), flush=True)


if __name__ == "__main__":
    main()
