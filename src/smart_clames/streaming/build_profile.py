"""Distil data/*.parquet into a small profile.json that the producer simulates from.

Run once (or whenever the sample data changes):
    uv run python src/smart_clames/build_profile.py
"""
import collections
import glob
import json
import re
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[3]
VIN_RE = re.compile(r"^[A-HJ-NPR-Z0-9]{17}$")  # valid VINs never contain I, O, Q


def quantiles(values: list[float], n: int = 100) -> list[float]:
    v = sorted(values)
    return [round(v[min(len(v) - 1, int(i / n * len(v)))], 3) for i in range(n + 1)]


def main() -> None:
    files = sorted(glob.glob(str(ROOT / "data" / "telematics" / "*.parquet")))
    rows = pa.concat_tables([pq.read_table(f) for f in files]).to_pylist()
    n = len(rows)

    distinct = len({(r["chassis_no"], r["event_timestamp"]) for r in rows})
    bad_vin = sum(1 for r in rows if not VIN_RE.match(r["chassis_no"]))
    wmi = collections.Counter(r["chassis_no"][:3] for r in rows if VIN_RE.match(r["chassis_no"]))

    profile = {
        "source_rows": n,
        "bbox": {
            "lat_min": min(r["latitude"] for r in rows),
            "lat_max": max(r["latitude"] for r in rows),
            "lon_min": min(r["longitude"] for r in rows),
            "lon_max": max(r["longitude"] for r in rows),
        },
        "speed_quantiles": quantiles([r["speed"] for r in rows]),  # 101 points: p0..p100
        "wmi_weights": dict(wmi.most_common(150)),  # real manufacturer prefixes
        "duplicate_rate": round(1 - distinct / n, 4),
        "bad_chassis_rate": round(bad_vin / n, 4),
        "sample_ping_interval_seconds": 60,
    }
    out = Path(__file__).with_name("profile.json")
    out.write_text(json.dumps(profile, indent=1))
    print(f"wrote {out}: dup={profile['duplicate_rate']} bad_vin={profile['bad_chassis_rate']}")


if __name__ == "__main__":
    main()
