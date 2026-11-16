"""Give every claim its OWN damage photo instead of one of 15 shared files.

The base picture is the one image_metadata.csv assigns to the claim (damage class Low/Medium/High). It is then
varied the way real phone photos vary: crop / zoom, tilt, mirror, exposure, white balance, sensor noise, blur,
JPEG quality - and gets real-looking EXIF (phone model, time taken, GPS in New York).

Realism details:
  * photo time = shortly AFTER the incident (the claim's own incident_date + hour, New York time).
  * duplicate_submission claims reuse the ORIGINAL claim's photo byte for byte (the fraud signal: same sha256).
  * photo GPS = the claim's incident location, the same spot where the telematics car stops (producer.incident_location).
  * the photo severity follows the existing assignment, so it still agrees with the TRUE damage class.

    uv run python src/smart_clames/generate_claim_images.py              # only claims in replay_plan.csv
    uv run python src/smart_clames/generate_claim_images.py --all        # all synthetic claims with a photo
    uv run python src/smart_clames/generate_claim_images.py --limit 500

Layout (inputs live in data/_archive/, output is the dataset):
    data/claims/images/<claim_no>.jpg          one photo per claim
    data/claims/metadata/image_metadata.csv    the single metadata file (overwritten on each run, so use --all)
"""
import argparse
import csv
import hashlib
import io
import random
import sys
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from PIL import Image, ImageEnhance, ImageFilter

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "streaming"))
from producer import incident_location  # noqa: E402

DATA = Path(__file__).resolve().parents[3] / "data"
SYN = DATA / "sql_server_synthetic"
ARCHIVE = DATA.parent / "_archive"
BASE_DIR = ARCHIVE / "original_15_images"
OUT_DIR = DATA / "claims" / "images"
REAL_DIR = ARCHIVE / "source_photos"
REAL_CSV = ARCHIVE / "source_photos" / "real_photos.csv"
OUT_CSV = DATA / "claims" / "metadata" / "image_metadata.csv"
NY = ZoneInfo("America/New_York")
PHONES = [("Apple", "iPhone 13"), ("Apple", "iPhone 14 Pro"), ("Apple", "iPhone 12"), ("Samsung", "SM-S911B"),
          ("Samsung", "SM-G991U"), ("Google", "Pixel 7"), ("Apple", "iPhone SE"), ("Motorola", "moto g power")]
ORIG = DATA / "sql_server"
M_PER_DEG = 111_000


# damage class a photo shows for a given TRUE severity (same table the synthetic claims were built with)
IMAGE_CLASS = {"Trivial Damage": {"Low": 90, "Medium": 10}, "Minor Damage": {"Low": 75, "Medium": 25},
               "Major Damage": {"Medium": 65, "High": 25, "Low": 10}, "Total Loss": {"High": 85, "Medium": 15}}
CLASS_ID = {"High": 1, "Low": 2, "Medium": 3}


def _gps_ifd(lat: float, lon: float) -> dict:
    def dms(v):
        v = abs(v)
        d, m = int(v), int((v - int(v)) * 60)
        return (float(d), float(m), round((v - d - m / 60) * 3600, 4))
    return {1: "N" if lat >= 0 else "S", 2: dms(lat), 3: "E" if lon >= 0 else "W", 4: dms(lon)}


def make_photo(base: Path, taken_local: datetime, rng_seed: int, spot: tuple[float, float]) -> tuple[bytes, dict]:
    rng = random.Random(rng_seed)
    img = Image.open(base).convert("RGB")
    img.thumbnail((1280, 1280))
    w, h = img.size
    # crop / zoom (keep 78-100% of the frame), different framing per photo
    s = rng.uniform(0.78, 1.0)
    cw, ch = int(w * s), int(h * s)
    x0, y0 = rng.randint(0, w - cw), rng.randint(0, h - ch)
    img = img.crop((x0, y0, x0 + cw, y0 + ch))
    img = img.rotate(rng.uniform(-6, 6), resample=Image.BICUBIC, expand=False, fillcolor=(20, 20, 20))
    if rng.random() < 0.5:
        img = img.transpose(Image.FLIP_LEFT_RIGHT)
    img = ImageEnhance.Brightness(img).enhance(rng.uniform(0.7, 1.25))     # time of day / weather
    img = ImageEnhance.Contrast(img).enhance(rng.uniform(0.85, 1.15))
    img = ImageEnhance.Color(img).enhance(rng.uniform(0.75, 1.2))
    r, g, b = img.split()                                                  # white balance drift
    k = rng.uniform(0.94, 1.06)
    img = Image.merge("RGB", (r.point(lambda v: min(255, int(v * k))), g, b.point(lambda v: min(255, int(v / k)))))
    if rng.random() < 0.3:
        img = img.filter(ImageFilter.GaussianBlur(rng.uniform(0.4, 1.4)))   # shaky hand / focus miss
    noise = Image.effect_noise(img.size, rng.uniform(4, 14)).convert("RGB")  # sensor noise (low light)
    img = Image.blend(img, Image.blend(img, noise, 0.5), rng.uniform(0.05, 0.2))
    img.thumbnail((rng.choice([1024, 1280, 960, 800]),) * 2)

    make, model = rng.choice(PHONES)
    # the photographer stands within a few metres of where the car was hit (+ consumer GPS error)
    lat, lon = spot[0] + rng.gauss(0, 12 / M_PER_DEG), spot[1] + rng.gauss(0, 12 / M_PER_DEG)
    ts = taken_local.strftime("%Y:%m:%d %H:%M:%S")
    exif = Image.Exif()
    exif[0x010F], exif[0x0110], exif[0x0132] = make, model, ts
    exif.get_ifd(0x8769).update({0x9003: ts, 0x9004: ts})
    exif.get_ifd(0x8825).update(_gps_ifd(lat, lon))
    buf = io.BytesIO()
    img.save(buf, "JPEG", quality=rng.randint(70, 92), exif=exif)
    return buf.getvalue(), {"make": make, "model": model, "lat": round(lat, 6), "lon": round(lon, 6)}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--all", action="store_true", help="every claim (original + synthetic) that has a photo (~24k files)")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--seed", type=int, default=7)
    args = ap.parse_args()

    # pool of real photos (fetch_damage_photos.py) by damage class; falls back to the 15 base images
    pool: dict[str, list[str]] = {}
    if REAL_CSV.exists():
        for r in csv.DictReader(open(REAL_CSV, newline="", encoding="utf-8")):
            pool.setdefault(r["damage_class"], []).append(r["file"])
    print("real photo pool:", {k: len(v) for k, v in pool.items()} or "none (using the 15 base images)")

    GROUPED = {"serial_claimant", "staged_accident"}   # the SAME damaged car shows up in every claim of the group
    SIGNAL = {"duplicate_submission": "byte-identical photo to the claim it copies",
              "inflated_claim": "photo shows less damage than the claimed severity",
              "serial_claimant": "same damaged car re-photographed across one claimant's claims (group_id)",
              "staged_accident": "same damaged car re-used across a staged-accident ring (group_id)",
              "theft_fraud": "no photo (nothing to photograph)"}
    group_cls: dict[str, str] = {}

    def pick_base(image_name: str, origin: str, g: dict) -> Path:
        cls, key = image_name.split("_")[1][:-4], origin          # Low / Medium / High
        if g["scenario"] in GROUPED and g.get("group_id"):
            cls, key = group_cls.get(g["group_id"], cls), f"grp-{g['group_id']}"
        if pool.get(cls):
            return REAL_DIR / random.Random(f"{args.seed}-base-{key}").choice(pool[cls])
        return BASE_DIR / image_name

    meta, claims = {}, {}
    for folder, meta_file in ((ORIG, ARCHIVE / "original_image_metadata.csv"),
                              (SYN, ARCHIVE / "synthetic_image_metadata.csv")):
        meta.update({r["claim_no"]: r for r in csv.DictReader(open(meta_file, newline=""))})
        claims.update({r["claim_no"]: r for r in csv.DictReader(open(folder / "claims.csv", newline=""))})
    gt = {r["claim_no"]: r for r in csv.DictReader(open(SYN / "claims_ground_truth.csv", newline=""))}
    for c in claims:   # original claims have no answer key
        gt.setdefault(c, {"scenario": "original", "linked_claim_no": "", "is_fraud": "", "group_id": "",
                          "claimed_severity": claims[c]["severity"], "true_severity": claims[c]["severity"]})
    for cn in sorted(meta):   # a group shares the damage class of its first member
        g = gt.get(cn)
        if g and g["scenario"] in GROUPED and g["group_id"]:
            group_cls.setdefault(g["group_id"], meta[cn]["image_name"].split("_")[1][:-4])
    if args.all:
        wanted = [c for c in claims if c in meta]  # synthetic + original
    else:
        plan = DATA / "replay_plan.csv"
        if not plan.exists():
            raise SystemExit("replay_plan.csv not found - run the producer first, or use --all")
        wanted = [r["claim_no"] for r in csv.DictReader(open(plan, newline="")) if r["claim_no"] in meta]
    if args.limit:
        wanted = wanted[:args.limit]
    # a duplicate submission re-uses the photo of the claim it copies, so make that original too
    for c in list(wanted):
        src = gt[c]["linked_claim_no"]
        if gt[c]["scenario"] == "duplicate_submission" and src in meta and src not in wanted:
            wanted.append(src)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    made: dict[str, tuple[str, dict]] = {}   # claim_no -> (sha256, details)

    def taken_at(claim_no: str) -> datetime:
        c = claims[claim_no]
        rng = random.Random(f"{args.seed}-t-{claim_no}")
        try:
            local = datetime.strptime(c["date"], "%Y-%m-%d").replace(hour=int(c["hour"]), tzinfo=NY)
        except ValueError:
            local = datetime(2020, 1, 1, 12, tzinfo=NY)   # unusable date in the source row
        return (local + timedelta(minutes=rng.randint(20, 48 * 60))).replace(tzinfo=None)

    rows = []
    for i, cn in enumerate(sorted(wanted, key=lambda c: gt[c]["scenario"] == "duplicate_submission"), 1):
        m, g = meta[cn], gt[cn]
        if g["scenario"] == "original":   # the original metadata pairs photos with claims at random: re-map by severity
            rr = random.Random(f"{args.seed}-cls-{cn}")
            w = IMAGE_CLASS.get(g["true_severity"], IMAGE_CLASS["Minor Damage"])
            cls = rr.choices(list(w), weights=list(w.values()))[0]
            k = int(m["image_name"].split("_")[0])
            m = {**m, "image_name": f"{k}_{cls}.jpg", "image_id": (k - 1) * 3 + CLASS_ID[cls]}
        origin = g["linked_claim_no"] if g["scenario"] == "duplicate_submission" and g["linked_claim_no"] in meta else cn
        out = OUT_DIR / f"{cn}.jpg"
        if origin != cn and origin in made:
            data = (OUT_DIR / f"{origin}.jpg").read_bytes()   # same file, submitted twice
            details = made[origin][1]
        else:
            data, details = make_photo(pick_base(m["image_name"], origin, g), taken_at(origin),
                                       int(hashlib.md5(f"{args.seed}{origin}".encode()).hexdigest()[:8], 16),
                                       incident_location(origin))
        out.write_bytes(data)
        sha = hashlib.sha256(data).hexdigest()
        made[cn] = (sha, details)
        rows.append({"claim_no": cn, "chassis_no": m["chassis_no"], "image_name": out.name, "base_image": m["image_name"],
                     "image_id": m["image_id"], "source_photo": pick_base(m["image_name"], origin, g).name, "photo_taken_local": taken_at(origin).strftime("%Y-%m-%d %H:%M:%S"),
                     "phone": f"{details['make']} {details['model']}", "gps_lat": details["lat"],
                     "gps_lon": details["lon"], "bytes": len(data), "sha256": sha,
                     "scenario": g["scenario"], "is_fraud": g.get("is_fraud", ""), "group_id": g.get("group_id", ""),
                     "claimed_severity": g["claimed_severity"], "true_severity": g["true_severity"],
                     "photo_class": m["image_name"].split("_")[1][:-4], "expected_signal": SIGNAL.get(g["scenario"], "")})
        if i % 200 == 0:
            print(f"  {i}/{len(wanted)}")
    with open(OUT_CSV, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    dup = len(rows) - len({r["sha256"] for r in rows})
    print(f"wrote {len(rows)} photos to {OUT_DIR.relative_to(DATA.parent)}  ({sum(r['bytes'] for r in rows) / 1e6:.0f} MB)")
    print(f"metadata: {OUT_CSV.relative_to(DATA.parent)}  | byte-identical re-submitted photos: {dup}")


if __name__ == "__main__":
    main()
