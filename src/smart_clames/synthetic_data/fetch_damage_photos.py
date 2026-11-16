"""Download REAL, openly licensed photos of damaged cars from Wikimedia Commons (for this study project).

Google Images is not used: those photos are copyrighted and scraping it breaks Google's terms. Commons files carry
a Creative Commons / public-domain licence, which is recorded per photo (author + licence + page URL) so the
attribution is never lost.

    uv run python src/smart_clames/fetch_damage_photos.py             # ~400 photos
    uv run python src/smart_clames/fetch_damage_photos.py --max 800

Output (resumable):
    data/_archive/source_photos/<n>.jpg
    data/_archive/source_photos/real_photos.csv  file, damage_class, license, author, source_url, categories
damage_class (Low = dent/scratch, Medium = heavy damage, High = crushed/burnt) is a keyword heuristic on the Commons
categories + file title, the same 3 classes image_metadata.csv uses. It is approximate.
"""
import argparse
import csv
import json
import re
import time
import urllib.parse
import urllib.request
from pathlib import Path

DATA = Path(__file__).resolve().parents[3] / "data" / "claims"
OUT = DATA.parent / "_archive" / "source_photos"
CSV_OUT = OUT / "real_photos.csv"
API = "https://commons.wikimedia.org/w/api.php"
UA = "study-project-damage-photos/1.0 (educational)"
ROOTS = ["Category:Damaged automobiles", "Category:Damaged automobiles by type of damage",
         "Category:Automobile accidents"]
SKIP = re.compile(r"\b(art|mural|racing|scrap|moss|crash test|appeal|toy|model|miniature|drawing|painting|cartoon"
                  r"|sculpture|motorcycle|bicycle|truck|bus|tram|train|aircraft|plane|ship|boat|tank|wreck removal)\b", re.I)
HIGH = re.compile(r"burn|burnt|torch|fire|wreck|crush|crumpl|rollover|overturned|total|destroy|flatten|smash", re.I)
LOW = re.compile(r"dent|scratch|scrape|bumper|mirror|light|headlight|taillight|window|windshield|minor|parking|hail", re.I)


def api(**params) -> dict:
    url = API + "?" + urllib.parse.urlencode({**params, "format": "json"})
    for attempt in range(4):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": UA}), timeout=30) as r:
                return json.load(r)
        except Exception:
            time.sleep(2 * (attempt + 1))
    return {}


def members(cat: str, kind: str) -> list[str]:
    out, cont = [], {}
    while True:
        d = api(action="query", list="categorymembers", cmtitle=cat, cmtype=kind, cmlimit=500, **cont)
        out += [m["title"] for m in d.get("query", {}).get("categorymembers", [])]
        if "continue" not in d:
            return out
        cont = {"cmcontinue": d["continue"]["cmcontinue"]}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--max", type=int, default=400)
    args = ap.parse_args()
    OUT.mkdir(parents=True, exist_ok=True)

    # 1. walk the category tree (depth 2) collecting files and the categories they sit in
    files: dict[str, list[str]] = {}
    seen, frontier = set(), [(c, 0) for c in ROOTS]
    while frontier:
        cat, depth = frontier.pop(0)
        if cat in seen or SKIP.search(cat):
            continue
        seen.add(cat)
        print("  crawling", cat, flush=True)
        for f in members(cat, "file"):
            files.setdefault(f, []).append(cat.replace("Category:", ""))
        if depth < 1:
            frontier += [(c, depth + 1) for c in members(cat, "subcat")]
        if len(files) > args.max * 3:
            break
    print(f"{len(seen)} categories, {len(files)} candidate files", flush=True)

    # 2. licence + size filter, then download a 1024px version
    titles = [t for t in files if t.lower().endswith((".jpg", ".jpeg")) and not SKIP.search(t)]
    rows, n = [], 0
    for i in range(0, len(titles), 40):
        d = api(action="query", titles="|".join(titles[i:i + 40]), prop="imageinfo", iiprop="url|size|mime|extmetadata",
                iiurlwidth=1024)
        for page in d.get("query", {}).get("pages", {}).values():
            info = (page.get("imageinfo") or [{}])[0]
            md = info.get("extmetadata", {})
            lic = md.get("LicenseShortName", {}).get("value", "")
            if not info or info.get("mime") != "image/jpeg" or info.get("width", 0) < 700 or info.get("height", 0) < 450:
                continue
            if not re.match(r"CC|Public domain|PD", lic, re.I):
                continue
            cats = files[page["title"]]
            text = page["title"] + " " + " ".join(cats)
            cls = "High" if HIGH.search(text) else "Low" if LOW.search(text) else "Medium"
            name = f"{n + 1:04d}.jpg"
            if not (OUT / name).exists():
                try:
                    req = urllib.request.Request(info["thumburl"], headers={"User-Agent": UA})
                    (OUT / name).write_bytes(urllib.request.urlopen(req, timeout=60).read())
                except Exception:
                    continue
                time.sleep(0.3)
            author = re.sub(r"<[^>]+>", "", md.get("Artist", {}).get("value", "")).strip()[:120]
            rows.append({"file": name, "damage_class": cls, "license": lic, "author": author,
                         "source_url": info.get("descriptionurl", ""), "categories": "; ".join(sorted(set(cats)))[:200]})
            n += 1
            if n >= args.max:
                break
        if n >= args.max:
            break
        print(f"  {n} photos kept ({i + 40}/{len(titles)} candidates checked)", flush=True)
    with open(CSV_OUT, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["file", "damage_class", "license", "author", "source_url", "categories"])
        w.writeheader()
        w.writerows(rows)
    from collections import Counter
    print(f"saved {n} photos to {OUT.relative_to(DATA.parents[1])} | classes: {dict(Counter(r['damage_class'] for r in rows))}")


if __name__ == "__main__":
    main()
