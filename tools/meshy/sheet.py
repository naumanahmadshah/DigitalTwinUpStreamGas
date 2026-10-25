"""Lay concept images out on labelled contact sheets for review (one sheet per asset kind)."""
import json, os, sys
from PIL import Image, ImageDraw, ImageFont
HERE = os.path.dirname(os.path.abspath(__file__))
src, out = sys.argv[1], sys.argv[2]
cfg = json.load(open(os.path.join(HERE, "prompts.json")))["assets"]
try:
    font = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", 26)
except Exception:
    font = ImageFont.load_default()
TW, TH, PAD, LAB = 480, 360, 16, 40
for kind in ("equipment", "surroundings", "person"):
    names = [n for n, s in cfg.items() if s.get("kind") == kind and n != "compressor"]
    cells = [(n, i, os.path.join(src, f"{n}_concept_{i}.png")) for n in names for i in (1, 2)]
    cells = [c for c in cells if os.path.exists(c[2])]
    if not cells:
        continue
    cols = 4; rows = (len(cells) + cols - 1) // cols
    sheet = Image.new("RGB", (cols * (TW + PAD) + PAD, rows * (TH + LAB + PAD) + PAD), (245, 243, 239))
    d = ImageDraw.Draw(sheet)
    for k, (n, i, p) in enumerate(cells):
        x = PAD + (k % cols) * (TW + PAD); y = PAD + (k // cols) * (TH + LAB + PAD)
        im = Image.open(p).convert("RGB"); im.thumbnail((TW, TH)); sheet.paste(im, (x + (TW - im.width) // 2, y + LAB))
        d.text((x, y + 6), f"{n}  #{i}", fill=(30, 30, 30), font=font)
    path = os.path.join(out, f"sheet_{kind}.png"); sheet.save(path); print(path, len(cells), "concepts")
