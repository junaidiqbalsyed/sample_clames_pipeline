"""Simulate a live fleet of cars and stream their telemetry to Azure Event Hubs.

The simulation is calibrated against data/ through profile.json (built by
build_profile.py): real manufacturer VIN prefixes, the real speed distribution,
the real NYC bounding box, and the real share of duplicate / malformed rows.
Every event is generated "now" in the same schema as the parquet files:
    chassis_no, latitude, longitude, event_timestamp (UTC string), speed (mph)

Two modes:
  REPLAY (default): every car is one real claim (synthetic + original). Its telemetry is stamped in
      business time (claim incident_date + incident_hour, New York time -> UTC) and ends in the matching event:
      a crash (speed -> 0 faster than braking allows, car stays put), a parked car, a theft (parked, then driven
      off) or - for fraudulent theft claims - nothing at all. Severity comes from claims_ground_truth (TRUE severity).
      chassis_no joins to policies -> claims -> images, and event_timestamp lines up with incident_date/hour.
  LIVE (--live): cars from the policies, stamped "now", normal driving only.

Usage:
    uv run python src/smart_clames/producer.py --vehicles 50 --speedup 30      # replay 50 claims, 30x faster
    uv run python src/smart_clames/producer.py --vehicles 5 --dry-run --speedup 100000
    uv run python src/smart_clames/producer.py --vehicles 50 --live --interval 2
    uv run python src/smart_clames/producer.py --vehicles 50 --dirty           # add dups + bad VINs
Replay writes the answer key (which car belongs to which claim/scenario) to data/replay_plan.csv.
"""
import argparse
import csv
import json
import math
import os
import random
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

PROFILE = json.loads(Path(__file__).with_name("profile.json").read_text())
BBOX = PROFILE["bbox"]
SPEED_Q = PROFILE["speed_quantiles"]  # 101 points: the real speed distribution (p0..p100)
WMIS = list(PROFILE["wmi_weights"])
WMI_W = list(PROFILE["wmi_weights"].values())

DATA_DIR = Path(__file__).resolve().parents[3] / "data"
NY = ZoneInfo("America/New_York")
SYN = DATA_DIR / "sql_server_synthetic"
POLICY_FILES = [DATA_DIR / "sql_server" / "policies.csv", DATA_DIR / "sql_server_synthetic" / "policies.csv"]

MPH_TO_MS = 0.44704
GRID_BEARING = math.radians(29)  # Manhattan's street grid is rotated ~29 degrees from north
MAX_ACCEL = 3.5  # mph per second (gentle city acceleration)
MAX_BRAKE = 7.0  # mph per second
GPS_NOISE_M = 4.0  # typical consumer GPS error
M_PER_DEG = 111_000


# ---------------------------------------------------------------- VIN generation
_VIN_CHARS = "ABCDEFGHJKLMNPRSTUVWXYZ0123456789"  # no I, O, Q
_TRANSLIT = {c: v for c, v in zip("ABCDEFGH", range(1, 9))}
_TRANSLIT.update({c: v for c, v in zip("JKLMN", range(1, 6))})
_TRANSLIT.update({"P": 7, "R": 9})
_TRANSLIT.update({c: v for c, v in zip("STUVWXYZ", [2, 3, 4, 5, 6, 7, 8, 9])})
_TRANSLIT.update({str(d): d for d in range(10)})
_WEIGHTS = [8, 7, 6, 5, 4, 3, 2, 10, 0, 9, 8, 7, 6, 5, 4, 3, 2]


def make_vin(rng: random.Random) -> str:
    """Real manufacturer prefix + random body + a mathematically valid check digit."""
    wmi = rng.choices(WMIS, weights=WMI_W)[0]
    vds = [rng.choice(_VIN_CHARS) for _ in range(5)]      # model / body / engine
    year = rng.choice("EFGHJKLMNPRSTVWXY")                  # model-year code
    plant = rng.choice(_VIN_CHARS)
    serial = [rng.choice("0123456789") for _ in range(6)]
    vin = list(wmi) + vds + ["0"] + [year, plant] + serial   # "0" = check digit placeholder
    total = sum(_TRANSLIT[c] * w for c, w in zip(vin, _WEIGHTS))
    vin[8] = "X" if total % 11 == 10 else str(total % 11)
    return "".join(vin)


IMPACT_MPH = {"Trivial Damage": (6, 14), "Minor Damage": (14, 28), "Major Damage": (28, 48), "Total Loss": (40, 62)}


def incident_location(claim_no: str) -> tuple[float, float]:
    """Where a claim's incident happened. Pure function of claim_no, so the producer (car position at impact)
    and generate_claim_images.py (photo GPS) agree without sharing any file."""
    r = random.Random(f"incident-location-{claim_no}")
    m_lat = min(0.03, (BBOX["lat_max"] - BBOX["lat_min"]) * 0.2)
    m_lon = min(0.03, (BBOX["lon_max"] - BBOX["lon_min"]) * 0.2)   # keep clear of the box edge (cars bounce there)
    return (round(r.uniform(BBOX["lat_min"] + m_lat, BBOX["lat_max"] - m_lat), 6),
            round(r.uniform(BBOX["lon_min"] + m_lon, BBOX["lon_max"] - m_lon), 6))


@dataclass
class Plan:
    claim_no: str
    chassis_no: str
    scenario: str
    kind: str            # crash | parked | theft | normal
    at: datetime         # incident moment, UTC
    start: datetime      # first ping, UTC
    impact_mph: float
    true_severity: str
    incident_type: str
    lat: float = 0.0     # where the incident happened (see incident_location)
    lon: float = 0.0
    car_seed: int = 0


def load_plans(rng: random.Random, n: int, lead_s: float, source: str = "both") -> list[Plan]:
    """Pick n claims (one per car) whose chassis is well formed, and turn each into a telemetry plan."""
    gt = {}
    with open(SYN / "claims_ground_truth.csv", newline="", encoding="utf-8-sig") as f:
        for r in csv.DictReader(f):
            gt[r["claim_no"]] = r
    sets = {"synthetic": SYN, "original": DATA_DIR / "sql_server"}
    now = datetime.now(timezone.utc)
    by_car: dict[str, list[dict]] = {}
    for name, folder in sets.items():
        if source not in (name, "both"):
            continue
        pol = {}
        with open(folder / "policies.csv", newline="", encoding="utf-8-sig") as f:
            for r in csv.DictReader(f):
                pol[r["POLICY_NO"]] = r["CHASSIS_NO"]
        with open(folder / "claims.csv", newline="", encoding="utf-8-sig") as f:
            for r in csv.DictReader(f):
                vin = pol.get(r["policy_no"], "")
                if len(vin) != 17 or set(vin) == {"0"} or (name == "synthetic" and r["claim_no"] not in gt):
                    continue
                try:
                    local = datetime.strptime(r["date"], "%Y-%m-%d").replace(hour=int(r["hour"]), tzinfo=NY)
                except ValueError:
                    continue   # malformed date/hour: the injected dirt, no usable incident time
                at = (local + timedelta(minutes=rng.randrange(60))).astimezone(timezone.utc)
                if at > now - timedelta(minutes=10):
                    continue   # not happened yet
                by_car.setdefault(vin, []).append({**r, "_at": at})
    vins = sorted(by_car)
    plans = []
    for vin in rng.sample(vins, min(n, len(vins))):
        c = rng.choice(by_car[vin])
        g = gt.get(c["claim_no"])   # original claims have no answer key
        scenario = g["scenario"] if g else "original"
        t = c["type"].strip().lower()
        if "theft" in t:
            kind = "normal" if scenario == "theft_fraud" else "theft"   # a faked theft: the car never leaves
        elif "parked" in t:
            kind = "parked"
        else:
            kind = "crash"
        sev = g["true_severity"] if g else c["severity"]   # what REALLY happened (a claim may overstate it)
        lo, hi = IMPACT_MPH.get(sev, (14, 40))
        # a duplicate submission re-reports the SAME incident, so it shares the original claim's location
        origin = g["linked_claim_no"] if scenario == "duplicate_submission" and g["linked_claim_no"] else c["claim_no"]
        lat, lon = incident_location(origin)
        plans.append(Plan(c["claim_no"], vin, scenario, kind, c["_at"], c["_at"] - timedelta(seconds=lead_s),
                          rng.uniform(lo, hi), sev, c["type"], lat, lon, rng.getrandbits(32)))
    return plans


def build_vehicle(plan: Plan, interval: float) -> Vehicle:
    """A car whose drive ends exactly at the claim's incident location at the incident time.
    Dry-run the drive, measure how far the car ends up from the target, shift the start by that error, repeat."""
    start = None
    for _ in range(8):
        v = Vehicle(random.Random(plan.car_seed), plan.chassis_no, plan, start)
        first = v.start0
        while v.clock < plan.at:
            v.step(interval)
        e_lat, e_lon = plan.lat - v.lat, plan.lon - v.lon
        if abs(e_lat) * M_PER_DEG < 3 and abs(e_lon) * M_PER_DEG * math.cos(math.radians(plan.lat)) < 3:
            break
        start = (first[0] + e_lat, first[1] + e_lon)
    return Vehicle(random.Random(plan.car_seed), plan.chassis_no, plan, start)


def policy_vins() -> list[str]:
    """Well-formed (17 char) chassis numbers of insured cars. Malformed ones are left out: --dirty adds those."""
    vins = set()
    for path in POLICY_FILES:
        with open(path, newline="", encoding="utf-8-sig") as f:
            vins.update(r["CHASSIS_NO"] for r in csv.DictReader(f) if len(r["CHASSIS_NO"]) == 17)
    return sorted(vins)


def corrupt_vin(rng: random.Random, vin: str) -> str:
    """Malformed chassis numbers like the ones in the real data."""
    return rng.choice([
        vin[:-1],                       # truncated (16 chars)
        vin + rng.choice("0123456789"),  # 18 chars
        vin[:12],                        # short id
        "AS PER LIST ATCHD.",            # junk text seen in the sample
        vin.lower(),
    ])


# ---------------------------------------------------------------- vehicle model
def sample_speed(rng: random.Random) -> float:
    """Draw a speed from the REAL distribution (inverse CDF over the sample quantiles)."""
    u = rng.random() * 100
    i = min(int(u), 99)
    return SPEED_Q[i] + (SPEED_Q[i + 1] - SPEED_Q[i]) * (u - i)


class Vehicle:
    """One car: drives along a rotated street grid, turning at intersections,
    accelerating/braking towards a target speed drawn from the real distribution."""

    def __init__(self, rng: random.Random, vin: str | None = None, plan: "Plan | None" = None,
                 start: tuple[float, float] | None = None):
        self.rng = rng
        self.plan = plan
        self.clock = plan.start if plan else None   # business time; None = use the wall clock
        self.frozen = 0.0                            # seconds the car must stay put (after a crash)
        self.fired = False
        self.done = plan is None
        self.vin = vin or make_vin(rng)
        self.lat = rng.uniform(BBOX["lat_min"], BBOX["lat_max"])
        self.lon = rng.uniform(BBOX["lon_min"], BBOX["lon_max"])
        if start:
            self.lat, self.lon = start
        self.start0 = (self.lat, self.lon)
        self.bearing = GRID_BEARING + rng.randrange(4) * math.pi / 2  # N/E/S/W along the grid
        self.speed = sample_speed(rng)
        self.target = sample_speed(rng)
        self.target_left = rng.uniform(10, 60)   # seconds until the driver "changes pace"
        self.block_left = rng.uniform(50, 270)   # metres until the next intersection

    def _incident(self, dt: float) -> bool:
        """Apply the claim's event. True = the car is held stationary this tick."""
        p, r = self.plan, self.rng
        to_go = (p.at - self.clock).total_seconds()
        if p.kind == "parked":
            self.speed = self.target = 0.0
            self.done = to_go < -300
            return True
        if p.kind == "theft":
            if to_go > 0:                      # parked until it is taken
                self.speed = self.target = 0.0
                return True
            self.done = to_go < -300
            if not self.fired:
                self.fired = True
                self.target, self.target_left = max(15.0, sample_speed(r)), r.uniform(20, 60)
            return False
        if p.kind == "crash":
            if self.frozen > 0:                # waiting for police / tow
                self.frozen -= dt
                self.speed = 0.0
                self.done = self.frozen <= 0
                return True
            if to_go > 0:
                if to_go <= 30:                # last seconds before impact: at the severity's impact speed
                    self.target, self.target_left = p.impact_mph, 10.0
                return False
            if not self.fired:
                self.fired = True
                self.speed = 0.0               # instant stop: faster than any braking
                self.frozen = r.uniform(120, 600)
                return True
        # normal / no event: just keep driving through the window
        self.done = to_go < -300
        return False

    def step(self, dt: float) -> dict:
        r = self.rng
        if self.plan:
            self.clock += timedelta(seconds=dt)
            if self._incident(dt):
                return self._report()
        # 1. speed: move towards the target within physical acceleration limits
        self.target_left -= dt
        if self.target_left <= 0:
            self.target, self.target_left = sample_speed(r), r.uniform(10, 60)
        delta = self.target - self.speed
        self.speed += max(-MAX_BRAKE * dt, min(MAX_ACCEL * dt, delta))
        self.speed = max(0.0, self.speed)

        # 2. distance travelled -> intersections / turns
        dist = self.speed * MPH_TO_MS * dt
        self.block_left -= dist
        if self.block_left <= 0:
            self.block_left = r.uniform(80, 270)
            self.bearing += r.choices([0, math.pi / 2, -math.pi / 2, math.pi], [60, 18, 18, 4])[0]

        # 3. move on the map
        self.lat += dist * math.cos(self.bearing) / M_PER_DEG
        self.lon += dist * math.sin(self.bearing) / (M_PER_DEG * math.cos(math.radians(self.lat)))
        if not BBOX["lat_min"] <= self.lat <= BBOX["lat_max"]:
            self.lat = min(max(self.lat, BBOX["lat_min"]), BBOX["lat_max"])
            self.bearing = math.pi - self.bearing  # reflect off the edge
        if not BBOX["lon_min"] <= self.lon <= BBOX["lon_max"]:
            self.lon = min(max(self.lon, BBOX["lon_min"]), BBOX["lon_max"])
            self.bearing = -self.bearing

        return self._report()

    def _report(self) -> dict:
        """Report with GPS noise (the true position is kept noise-free)."""
        r = self.rng
        noise = GPS_NOISE_M / M_PER_DEG
        now = self.clock if self.clock else datetime.now(timezone.utc)
        return {
            "chassis_no": self.vin,
            "latitude": round(self.lat + r.gauss(0, noise), 6),
            "longitude": round(self.lon + r.gauss(0, noise), 6),
            "event_timestamp": now.strftime("%Y-%m-%d %H:%M:%S"),
            "speed": round(self.speed, 2),
        }


def add_dirt(rng: random.Random, events: list[dict], rate_scale: float) -> list[dict]:
    """Inject the data-quality problems found in the sample, at the sample's own rates."""
    dup, bad = PROFILE["duplicate_rate"] * rate_scale, PROFILE["bad_chassis_rate"] * rate_scale
    out = []
    for e in events:
        if rng.random() < bad:
            e = {**e, "chassis_no": corrupt_vin(rng, e["chassis_no"])}
        out.append(e)
        if rng.random() < dup:
            out.append(dict(e))  # exact duplicate
    return out


# ---------------------------------------------------------------- Event Hubs
def send_all(producer, events: list[dict]) -> None:
    from azure.eventhub import EventData

    # Group by VIN: a batch can only carry one partition key, and the same key
    # always maps to the same partition, so each car's events stay in order.
    by_vin: dict[str, list[dict]] = {}
    for e in events:
        by_vin.setdefault(e["chassis_no"], []).append(e)

    for vin, group in by_vin.items():
        batch = producer.create_batch(partition_key=vin)
        for e in group:
            try:
                batch.add(EventData(json.dumps(e)))
            except ValueError:  # batch full -> flush and start another
                producer.send_batch(batch)
                batch = producer.create_batch(partition_key=vin)
                batch.add(EventData(json.dumps(e)))
        if len(batch):
            producer.send_batch(batch)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--vehicles", type=int, default=20)
    p.add_argument("--interval", type=float, default=2.0, help="seconds between pings per vehicle")
    p.add_argument("--ticks", type=int, default=0, help="stop after N ticks (0 = run forever)")
    p.add_argument("--dirty", action="store_true", help="inject duplicates + malformed chassis numbers")
    p.add_argument("--live", action="store_true", help="old behaviour: policy cars stamped 'now', normal driving only")
    p.add_argument("--claims-from", choices=["synthetic", "original", "both"], default="both",
                   help="replay: which claims to replay")
    p.add_argument("--lead-minutes", type=float, default=3.0, help="replay: driving shown before the incident")
    p.add_argument("--speedup", type=float, default=1.0, help="replay: run N times faster than real time (sleep only)")
    p.add_argument("--random-vins", action="store_true", help="invent VINs instead of using policy chassis numbers")
    p.add_argument("--seed", type=int, default=None)
    p.add_argument("--dry-run", action="store_true", help="print events instead of sending")
    args = p.parse_args()

    rng = random.Random(args.seed)
    plans: list[Plan] = []
    if not args.live and not args.random_vins:
        plans = load_plans(rng, args.vehicles, args.lead_minutes * 60, args.claims_from)
        fleet = [build_vehicle(pl, args.interval) for pl in plans]
        plan_file = DATA_DIR / "replay_plan.csv"
        with open(plan_file, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["chassis_no", "claim_no", "scenario", "event", "incident_type", "true_severity",
                        "incident_utc", "first_ping_utc", "impact_mph", "incident_lat", "incident_lon"])
            for pl in plans:
                w.writerow([pl.chassis_no, pl.claim_no, pl.scenario, pl.kind, pl.incident_type, pl.true_severity,
                            pl.at.strftime("%Y-%m-%d %H:%M:%S"), pl.start.strftime("%Y-%m-%d %H:%M:%S"),
                            round(pl.impact_mph, 1), pl.lat, pl.lon])
        print(f"replaying {len(fleet)} claims (answer key: {plan_file.relative_to(DATA_DIR.parent)})")
        vins = []
    elif args.random_vins:
        vins = [None] * args.vehicles
    else:
        pool = policy_vins()
        vins = rng.sample(pool, min(args.vehicles, len(pool)))  # distinct cars
        print(f"fleet uses {len(vins)} real chassis numbers from policies")
    if not plans:
        fleet = [Vehicle(rng, v) for v in vins]

    producer = None
    if not args.dry_run:
        from azure.eventhub import EventHubProducerClient
        from dotenv import load_dotenv

        load_dotenv()
        producer = EventHubProducerClient.from_connection_string(
            os.environ["EVENTHUB_CONNECTION_STRING"],
            eventhub_name=os.environ.get("EVENTHUB_NAME"),
        )

    tick, last, next_at = 0, time.monotonic(), time.monotonic()
    try:
        while not args.ticks or tick < args.ticks:
            now = time.monotonic()
            dt, last = (args.interval if plans or not tick else now - last), now  # replay: fixed sim step
            events = [v.step(dt) for v in fleet]
            if args.dirty:
                events = add_dirt(rng, events, 1.0)
            if args.dry_run:
                for e in events:
                    print(json.dumps(e))
            else:
                send_all(producer, events)
                print(f"tick {tick}: sent {len(events)} events")
            tick += 1
            if plans and all(v.done for v in fleet):
                print("replay finished: every car has passed its incident")
                break
            next_at += args.interval / (args.speedup if plans else 1.0)  # fixed cadence: no drift
            time.sleep(max(0.0, next_at - time.monotonic()))
    except KeyboardInterrupt:
        print("stopped")
    finally:
        if producer:
            producer.close()


if __name__ == "__main__":
    main()
