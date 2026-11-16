"""Build the damage-classification training set under data/training_imgs/.

Classes (same 3 as the original training_imgs):  ok | minor | major

Sources
  hf        DrBimmer/comprehensive-car-damage (MIT), 2,300 real whole-car photos, front + rear.
            Normal -> ok, Crushed -> minor (dents), Breakage -> major (broken parts).  The dataset's own label is
            kept in labels.csv (source_label) in case you want to re-map.
  orig56    your original 56 close-up images (class is in the file name).
  commons   the 75 curated street photos (Wikimedia Commons, CC) -> held out in test_street/ ONLY. The per-claim
            photos in claims/images are made from these, so they must never be in training.

Output
    training_imgs/{train,val,test}/{ok,minor,major}/<files>     80/10/10 stratified split, seed 42
    training_imgs/test_street/{minor,major}/<files>            realistic out-of-domain test (street photos)
    training_imgs/labels.csv                                   one row per image

    # 1. download the HF data once (about 650 MB):
    uv run --with huggingface_hub python -c "from huggingface_hub import snapshot_download as s; \\
        s('DrBimmer/comprehensive-car-damage', repo_type='dataset', local_dir='PATH', allow_patterns=['F_*/*','R_*/*'])"
    # 2. build:
    uv run python src/smart_clames/build_training_set.py --hf-dir PATH
"""
import argparse
import csv
import hashlib
import random
import re
import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3] / "data"
OUT = ROOT / "training_imgs"
ARCHIVE = ROOT / "_archive"
# checked by eye: in this dataset "Crushed" = mild dents / crumpled panels, "Breakage" = broken lights, bumpers, torn-off parts
HF_MAP = {"Normal": "ok", "Crushed": "minor", "Breakage": "major"}
COMMONS_MAP = {"Low": "minor", "Medium": "major", "High": "major"}


def md5(p: Path) -> str:
    return hashlib.md5(p.read_bytes()).hexdigest()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--hf-dir", required=True, type=Path)
    args = ap.parse_args()
    rng = random.Random(42)

    items = []   # dicts: path, class, source, source_label, view, license, note
    orig = OUT if not (ARCHIVE / "original_56_training_imgs").exists() else ARCHIVE / "original_56_training_imgs"
    for p in sorted(orig.glob("*.png")):
        m = re.match(r"(\d+)-(ok|minor|major)", p.name)
        if m:
            items.append(dict(path=p, cls=m.group(2), source="orig56", source_label=m.group(2), view="closeup",
                              license="user-provided", note="", name=f"orig56_{m.group(1)}{p.suffix}"))
    for d in sorted(args.hf_dir.glob("[FR]_*")):
        view = "front" if d.name[0] == "F" else "rear"
        label = d.name[2:]
        for p in sorted(d.iterdir()):
            if p.suffix.lower() not in (".jpg", ".jpeg", ".png"):
                continue
            items.append(dict(path=p, cls=HF_MAP[label], source="hf", source_label=d.name, view=view, license="MIT",
                              note="DrBimmer/comprehensive-car-damage", name=f"hf_{p.name}"))
    seen, uniq = set(), []
    for it in items:   # drop byte-identical duplicates
        h = md5(it["path"])
        if h not in seen:
            seen.add(h)
            uniq.append(it)
    print(f"{len(items)} images found, {len(items) - len(uniq)} exact duplicates dropped")

    # stratified 80/10/10 split per class
    for cls in ("ok", "minor", "major"):
        grp = [i for i in uniq if i["cls"] == cls]
        rng.shuffle(grp)
        n = len(grp)
        for k, it in enumerate(grp):
            it["split"] = "train" if k < n * 0.8 else "val" if k < n * 0.9 else "test"

    # commons street photos: held out
    lic = {r["file"]: r for r in csv.DictReader(open(ARCHIVE / "source_photos" / "real_photos.csv", encoding="utf-8"))}
    for f, r in sorted(lic.items()):
        p = ARCHIVE / "source_photos" / f
        if p.exists():
            uniq.append(dict(path=p, cls=COMMONS_MAP[r["damage_class"]], source="commons", source_label=r["damage_class"],
                             view="street", license=r["license"], note=f"{r['author']} | {r['source_url']}",
                             name=f"commons_{f}", split="test_street"))

    rows = []
    for it in uniq:
        dest = OUT / it["split"] / it["cls"] / it["name"]
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(it["path"], dest)
        rows.append({"file": str(dest.relative_to(OUT)), "class": it["cls"], "split": it["split"], "source": it["source"],
                     "source_label": it["source_label"], "view": it["view"], "license": it["license"], "note": it["note"]})
    with open(OUT / "labels.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)

    # the 56 originals now live in the split folders; park the loose copies in the archive
    loose = [p for p in OUT.glob("*.png")]
    if loose:
        dst = ARCHIVE / "original_56_training_imgs"
        dst.mkdir(parents=True, exist_ok=True)
        for p in loose:
            shutil.move(str(p), dst / p.name)

    from collections import Counter
    print(Counter((r["split"], r["class"]) for r in rows))
    print(f"labels.csv: {len(rows)} rows")


if __name__ == "__main__":
    main()
