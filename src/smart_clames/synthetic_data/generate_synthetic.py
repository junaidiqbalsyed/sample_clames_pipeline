"""Generate realistic synthetic NYC auto-insurance data: customers -> policies -> claims.

    uv run python src/smart_clames/generate_synthetic.py --customers 22000

Output: data/sql_server_synthetic/{customers,policies,claims}.csv
(same columns as the original sql_server CSVs, but clean formats: ISO dates, plain ints).

Design goals - everything is internally consistent, unlike the original claims data:
  * customers : NYC names, valid borough/neighborhood/ZIP combinations, adult drivers
  * policies  : real US makes/models/years, VINs with correct WMI, year code and check digit,
                value depreciates with age, premiums driven by driver age / borough / use / value,
                annual terms with renewals on the same car
  * claims    : happen INSIDE the policy term, reported after the incident, amounts scale with
                severity and vehicle value, theft/parked/single/multi-vehicle behave differently,
                third-party (TP) policies never pay for own-vehicle damage, fraud flag is
                correlated with realistic risk signals instead of being a coin flip
Everything is seeded, so the same --seed gives the same data.

Then a controlled amount of GARBAGE is injected (for practising @dp.expect in Lakeflow pipelines):
bad formats, impossible values, out-of-place categories, cross-table rule violations.
Only garbage that the SQL table constraints still accept is generated. Each affected row gets exactly
one issue. Answer keys are written next to the CSVs:
  dirty_manifest.csv  every injected value: table, key, issue, column, original, injected
  dirty_issues.csv    every issue type with a suggested expectation and how many rows were hit
Use --dirty-rate 0 for a perfectly clean dataset, 2 for twice as much garbage.
"""
import argparse
import csv
import math
import random
import uuid
from collections import Counter, defaultdict
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
SRC_DIR = ROOT / "data" / "sql_server"
OUT_DIR = ROOT / "data" / "sql_server_synthetic"
# CSV column -> database column (answer keys refer to the database names)
DB_COLUMN = {("claims", "age"): "driver_age", ("claims", "date"): "incident_date", ("claims", "hour"): "incident_hour",
             ("claims", "type"): "incident_type", ("claims", "severity"): "incident_severity"}

FIRST_CUSTOMER_ID = 100_001   # original customers use 104..14583
POLICY_PREFIX = 3             # original policy numbers start with 10...

# ---------------------------------------------------------------- reference data
FIRST_F = ("Maria Jennifer Linda Sofia Isabella Emily Olivia Ashley Jessica Sarah Michelle Angela Aaliyah "
           "Priya Mei Yuki Fatima Aisha Keisha Tiffany Nicole Samantha Rachel Rebecca Hannah Elena Natalia "
           "Olga Anna Katarzyna Giulia Francesca Carmen Rosa Yolanda Daniela Valentina Camila Lucia Gabriela "
           "Alexandra Christina Stephanie Amanda Brittany Danielle Monique Shanice Latoya Tamika Destiny "
           "Jasmine Sophia Ava Mia Chloe Grace Lily Zoe Naomi Leah Esther Miriam Sara Layla Nadia Sunita "
           "Anjali Deepa Hyun Jiyoon Linh Thu Ngoc Ana Marisol Ximena").split()
FIRST_M = ("James Michael Robert David Daniel Joseph Anthony Christopher Matthew Andrew Joshua Ryan Brandon "
           "Kevin Jason Eric Jose Luis Carlos Juan Miguel Jorge Pedro Angel Javier Diego Mateo Alejandro Wei "
           "Jian Ming Hiroshi Kenji Raj Amit Vikram Arjun Mohammed Ahmed Omar Ibrahim Hassan Tyrone Darnell "
           "Jamal Marcus DeShawn Terrence Anton Dmitri Ivan Pavel Tomasz Marek Giovanni Salvatore Vincent "
           "Dominic Patrick Sean Liam Noah Ethan Lucas Jacob Samuel Nathan Aaron Jonathan Ari Moshe Yosef "
           "Nicholas Alexander Steven Timothy Kenneth Gregory Raymond Edwin Wilfredo Hector Ramon Ruben "
           "Felix Victor Oscar Andre Jerome Leon Kwame Emmanuel Pierre Nikhil Sanjay Tuan Minh Joon Sung").split()
SURNAMES = ("Rodriguez Martinez Garcia Hernandez Lopez Gonzalez Perez Sanchez Ramirez Torres Rivera Flores "
            "Gomez Diaz Reyes Morales Cruz Ortiz Gutierrez Chavez Ramos Vargas Castillo Jimenez Medina "
            "Smith Johnson Williams Brown Jones Davis Miller Wilson Moore Taylor Anderson Thomas Jackson "
            "White Harris Martin Thompson Robinson Clark Lewis Walker Hall Allen Young King Wright Scott "
            "Green Baker Adams Nelson Hill Campbell Mitchell Carter Roberts Phillips Evans Turner Parker "
            "Collins Edwards Stewart Morris Murphy Cook Rogers Morgan Cooper Peterson Reed Bailey Bell "
            "Kelly Howard Ward Cox Richardson Wood Watson Brooks Bennett Gray James Hughes Price Sanders "
            "Patterson Washington Butler Simmons Foster Bryant Alexander Russell Griffin Hayes "
            "Chen Wang Li Zhang Liu Lin Huang Wu Zhou Yang Kim Park Lee Choi Nguyen Tran Pham Tanaka "
            "Patel Singh Sharma Gupta Khan Ahmed Hussain Ali Rahman Islam Begum "
            "Rossi Russo Ferrari Esposito Romano Colombo Marino Greco Bruno Ricci Conti Gallo Costa "
            "Fontana O'Brien Ryan Walsh McCarthy Sullivan Cohen Levy Katz Friedman Goldberg Schwartz "
            "Rosen Shapiro Weiss Stein Kowalski Nowak Ivanov Petrov Volkov Kozlov Popov Kuznetsov "
            "Papadopoulos Nikolaidis Haddad Nasser Khalil Baptiste Joseph Charles Pierre Louis Francois "
            "Clarke Mensah Okafor Adeyemi").split()

# make, model, WMI prefixes, MSRP in 2024 USD, popularity weight, class, first model year
CATALOG = [
    ("TOYOTA", "CAMRY", ("4T1",), 28500, 9, "sedan", 2008), ("TOYOTA", "COROLLA", ("2T1", "5YF"), 23000, 8, "sedan", 2008),
    ("TOYOTA", "RAV4", ("2T3", "JTM"), 31000, 9, "suv", 2008), ("TOYOTA", "HIGHLANDER", ("5TD",), 42000, 3, "suv", 2008),
    ("TOYOTA", "PRIUS", ("JTD",), 28000, 3, "sedan", 2008), ("TOYOTA", "SIENNA", ("5TD",), 40000, 2, "van", 2008),
    ("HONDA", "CIVIC", ("2HG", "19X"), 25000, 8, "sedan", 2008), ("HONDA", "ACCORD", ("1HG",), 29000, 6, "sedan", 2008),
    ("HONDA", "CR-V", ("2HK", "5J6"), 32000, 8, "suv", 2008), ("HONDA", "PILOT", ("5FN",), 42000, 2, "suv", 2008),
    ("NISSAN", "ALTIMA", ("1N4",), 27000, 6, "sedan", 2008), ("NISSAN", "ROGUE", ("5N1", "JN8"), 30000, 7, "suv", 2008),
    ("NISSAN", "SENTRA", ("3N1",), 21000, 4, "sedan", 2008), ("HYUNDAI", "ELANTRA", ("KMH", "5NP"), 22000, 6, "sedan", 2008),
    ("HYUNDAI", "SONATA", ("5NP", "KMH"), 27000, 4, "sedan", 2008), ("HYUNDAI", "TUCSON", ("KM8", "5NM"), 29000, 5, "suv", 2010),
    ("KIA", "FORTE", ("3KP",), 21000, 3, "sedan", 2010), ("KIA", "SPORTAGE", ("KNA", "5XY"), 29000, 4, "suv", 2008),
    ("KIA", "SORENTO", ("5XY",), 33000, 2, "suv", 2008), ("FORD", "F-150", ("1FT",), 45000, 5, "truck", 2008),
    ("FORD", "ESCAPE", ("1FM",), 30000, 4, "suv", 2008), ("FORD", "EXPLORER", ("1FM",), 40000, 3, "suv", 2008),
    ("FORD", "TRANSIT", ("1FT",), 48000, 2, "van", 2015), ("CHEVROLET", "MALIBU", ("1G1",), 26000, 3, "sedan", 2008),
    ("CHEVROLET", "EQUINOX", ("2GN",), 28000, 4, "suv", 2008), ("CHEVROLET", "SILVERADO", ("1GC",), 44000, 3, "truck", 2008),
    ("JEEP", "GRAND CHEROKEE", ("1C4",), 43000, 3, "suv", 2008), ("SUBARU", "OUTBACK", ("4S4",), 32000, 3, "suv", 2008),
    ("SUBARU", "FORESTER", ("JF2",), 30000, 3, "suv", 2008), ("VOLKSWAGEN", "JETTA", ("3VW",), 22000, 3, "sedan", 2008),
    ("MAZDA", "CX-5", ("JM3",), 30000, 3, "suv", 2013), ("BMW", "3 SERIES", ("WBA",), 46000, 3, "lux", 2008),
    ("BMW", "X5", ("5UX",), 66000, 2, "lux", 2008), ("MERCEDES-BENZ", "C-CLASS", ("W1K", "55S"), 48000, 2, "lux", 2008),
    ("LEXUS", "RX 350", ("2T2",), 50000, 2, "lux", 2008), ("AUDI", "Q5", ("WA1",), 47000, 2, "lux", 2009),
    ("TESLA", "MODEL 3", ("5YJ",), 40000, 4, "ev", 2018), ("TESLA", "MODEL Y", ("7SA",), 47000, 4, "ev", 2020),
    ("RAM", "PROMASTER", ("3C6",), 42000, 1, "van", 2014), ("MERCEDES-BENZ", "SPRINTER", ("W1Y",), 55000, 1, "van", 2010),
]
P_COMMERCIAL = {"van": 0.70, "truck": 0.30, "sedan": 0.09, "suv": 0.04, "lux": 0.02, "ev": 0.05}

VIN_CHARS = "ABCDEFGHJKLMNPRSTUVWXYZ0123456789"
VIN_YEAR = dict(zip(range(2008, 2028), "89ABCDEFGHJKLMNPRSTV"))
_TRANS = {**{c: v for c, v in zip("ABCDEFGH", range(1, 9))}, **{c: v for c, v in zip("JKLMN", range(1, 6))},
          "P": 7, "R": 9, **{c: v for c, v in zip("STUVWXYZ", range(2, 10))}, **{str(d): d for d in range(10)}}
_W = [8, 7, 6, 5, 4, 3, 2, 10, 0, 9, 8, 7, 6, 5, 4, 3, 2]

BOROUGH_PREMIUM = {"BRONX": 1.28, "BROOKLYN": 1.18, "MANHATTAN": 1.12, "QUEENS": 1.05, "STATEN ISLAND": 0.92}
BOROUGH_CLAIMS = {"BRONX": 1.20, "BROOKLYN": 1.15, "MANHATTAN": 1.10, "QUEENS": 1.00, "STATEN ISLAND": 0.85}
USE_PREMIUM = {"PRIVATE": 1.0, "COMMERCIAL": 1.45, "RENTACAR": 1.7}
USE_CLAIMS = {"PRIVATE": 1.0, "COMMERCIAL": 1.8, "RENTACAR": 2.2}
CLASS_PREMIUM = {"lux": 1.25, "ev": 1.18, "truck": 1.05}

HOUR_W = {
    "Multi-vehicle Collision": [2, 1.5, 1.5, 1.2, 1.2, 1.8, 3.5, 6, 7, 5, 4.5, 5, 5.5, 5.5, 5.5, 6.5, 8.5, 9, 8, 6, 5, 4, 3.5, 2.8],
    "Single Vehicle Collision": [3.6, 2.7, 2.7, 2.2, 2.2, 2.2, 3.5, 6, 6, 4.5, 4, 4.5, 5, 5, 5, 6, 7, 7.5, 7, 5.5, 5, 4.5, 4.5, 4.5],
    "Parked Car": [3, 2, 2, 2, 2.5, 4, 6, 7, 6, 4, 3, 3, 3, 3, 3, 3, 4, 5, 5, 4, 4, 3.5, 3, 3],
    "Vehicle Theft": [6, 7, 8, 8, 7, 5, 3, 2, 1.5, 1.5, 1.5, 1.5, 2, 2, 2, 2, 2, 2, 2.5, 3, 3.5, 4, 5, 5.5],
}
MONTH_W = [1.20, 1.15, 1.00, 0.95, 0.95, 0.95, 0.95, 0.95, 0.98, 1.05, 1.10, 1.20]  # winter = more claims


def pick(rng, weighted: dict):
    return rng.choices(list(weighted), weights=list(weighted.values()))[0]


# ---------------------------------------------------------------- helpers
def vin_check_digit(vin: str) -> str:
    total = sum(_TRANS[c] * w for c, w in zip(vin, _W))
    r = total % 11
    return "X" if r == 10 else str(r)


def make_vin(rng, wmi: str, model_year: int, plants: str) -> str:
    vds = "".join(rng.choice(VIN_CHARS) for _ in range(5))
    serial = "".join(rng.choice("0123456789") for _ in range(6))
    vin = wmi + vds + "0" + VIN_YEAR[model_year] + rng.choice(plants) + serial
    return vin[:8] + vin_check_digit(vin) + vin[9:]


def add_years(d: date, n: int) -> date:
    try:
        return d.replace(year=d.year + n)
    except ValueError:  # Feb 29
        return d.replace(year=d.year + n, day=28)


def years_between(a: date, b: date) -> int:
    return b.year - a.year - ((b.month, b.day) < (a.month, a.day))


def rnd10(x: float) -> int:
    return int(round(x / 10.0) * 10)


def load_geography() -> list[tuple[tuple[str, str, str], int]]:
    """Valid (borough, neighborhood, zip) combinations, weighted by how common they are in the source."""
    cnt: Counter = Counter()
    with open(SRC_DIR / "customers.csv", newline="", encoding="utf-8-sig") as f:
        for r in csv.DictReader(f):
            if r["borough"] != "null" and r["zip_code"] != "null":
                cnt[(r["borough"], r["neighborhood"], r["zip_code"])] += 1
    return list(cnt.items())


# ---------------------------------------------------------------- generation
def make_customer(rng, cid, first_eff, geo):
    gender = rng.choice("FM")
    first = rng.choice(FIRST_F if gender == "F" else FIRST_M)
    age = int(min(82, max(18, rng.gauss(41, 13))))
    dob = add_years(first_eff, -age) - timedelta(days=rng.randint(0, 364))
    r = rng.random()
    lic_age = rng.uniform(16, 18) if r < .35 else rng.uniform(18, 22) if r < .70 else rng.uniform(22, 31) if r < .92 else rng.uniform(31, 46)
    license_date = min(add_years(dob, 16) + timedelta(days=int((lic_age - 16) * 365.25)), first_eff - timedelta(days=90))
    (borough, hood, zipc) = rng.choices([g for g, _ in geo], weights=[w for _, w in geo])[0]
    return {"customer_id": cid, "dob": dob, "gender": gender, "borough": borough, "neighborhood": hood,
            "zip_code": zipc, "name": f"{rng.choice(SURNAMES)}, {first}", "license_date": license_date,
            "first_eff": first_eff}


def make_vehicle(rng, start: date, used_vins: set, plants_by_make: dict):
    make, model, wmis, msrp, _, cls, first_year = rng.choices(CATALOG, weights=[c[4] for c in CATALOG])[0]
    r = rng.random()
    age = rng.randint(0, 1) if r < .20 else rng.randint(2, 5) if r < .60 else rng.randint(6, 10) if r < .88 else rng.randint(11, 15)
    year = max(first_year, min(start.year + 1, start.year - age))
    while True:
        vin = make_vin(rng, rng.choice(wmis), year, plants_by_make[make])
        if vin not in used_vins:
            used_vins.add(vin)
            break
    use = "COMMERCIAL" if rng.random() < P_COMMERCIAL[cls] else "PRIVATE"
    if use == "PRIVATE" and rng.random() < 0.004:
        use = "RENTACAR"
    return {"make": make, "model": model, "year": year, "msrp": msrp, "cls": cls, "vin": vin, "use": use}


def vehicle_value(v, on: date) -> int:
    age = max(0, on.year - v["year"])
    value = v["msrp"] * 1.03 ** (v["year"] - 2024) * (0.88 ** age)
    return max(2500, int(round(value / 100.0) * 100))


def driver_age_factor(age: int) -> float:
    return 2.1 if age < 21 else 1.6 if age < 25 else 1.15 if age < 30 else 1.0 if age < 65 else 1.1 if age < 75 else 1.35


def make_policy(rng, cust, veh, eff: date, renewal: bool, ptype: str, product: str, ded: int):
    age = years_between(cust["dob"], eff)
    value = vehicle_value(veh, eff)
    f = (driver_age_factor(age) * BOROUGH_PREMIUM[cust["borough"]] * USE_PREMIUM[veh["use"]]
         * rng.uniform(0.92, 1.10) * 1.04 ** (eff.year - 2022))
    if ptype == "TP":
        premium = 700 * f
    else:
        premium = (600 + value * 0.09 * CLASS_PREMIUM.get(veh["cls"], 1.0) * f)
        premium *= {"BASIC": 0.88, "STANDARD": 1.0, "PREMIUM": 1.12, "RIDESHARE": 1.15}[product]
    premium = max(1100 if ptype != "TP" else 750, premium)
    issue = eff - timedelta(days=rng.randint(10, 35) if renewal else rng.randint(0, 10))
    return {"cust": cust, "veh": veh, "ptype": ptype, "issue": issue, "eff": eff,
            "expiry": add_years(eff, 1) - timedelta(days=1), "value": value,
            "premium": int(round(premium)), "deductible": ded, "product": product}


def build_policies(rng, customers_n: int, as_of: date, geo):
    plants = {c[0]: "".join(random.Random(c[0]).choice(VIN_CHARS) for _ in range(3)) for c in CATALOG}
    used_vins: set = set()
    customers, policies = [], []
    cid = FIRST_CUSTOMER_ID
    window_start = date(2022, 1, 1)
    span = (as_of - timedelta(days=10) - window_start).days
    for _ in range(customers_n):
        first_eff = window_start + timedelta(days=rng.randint(0, span))
        cust = make_customer(rng, cid, first_eff, geo)
        cid += rng.randint(1, 3)
        customers.append(cust)
        for k in range(rng.choices([1, 2, 3], weights=[70, 24, 6])[0]):
            start = first_eff if k == 0 else first_eff + timedelta(days=rng.randint(0, (as_of - first_eff).days))
            veh = make_vehicle(rng, start, used_vins, plants)
            # one coverage / product choice per vehicle, kept across renewals
            ptype = "TP" if rng.random() < 0.08 else "COMP"
            if ptype == "TP":
                product, ded = "TP", 0
            elif veh["use"] != "PRIVATE" and veh["cls"] in ("sedan", "suv", "ev") and rng.random() < 0.6:
                product, ded = "RIDESHARE", rng.choice([1000, 1000, 2000])
            else:
                product = rng.choices(["BASIC", "STANDARD", "PREMIUM"], weights=[25, 55, 20])[0]
                ded = {"BASIC": rng.choice([1000, 2000, 2000]), "STANDARD": rng.choices([500, 1000, 2000], [25, 60, 15])[0],
                       "PREMIUM": rng.choices([500, 1000], [70, 30])[0]}[product]
            eff, renewal = start, False
            while eff <= as_of + timedelta(days=30):
                pol = make_policy(rng, cust, veh, eff, renewal, ptype, product, ded)
                if pol["issue"] <= as_of:
                    policies.append(pol)
                if rng.random() > 0.82:   # customer churns
                    break
                eff, renewal = pol["expiry"] + timedelta(days=1), True
    policies.sort(key=lambda p: (p["issue"], p["cust"]["customer_id"]))
    num = POLICY_PREFIX * 10**8
    for p in policies:
        num += rng.randint(1, 7)
        p["policy_no"] = str(num)
    return customers, policies


# ---------------------------------------------------------------- claims
SEVERITIES = ["Trivial Damage", "Minor Damage", "Major Damage", "Total Loss"]
SEV_BY_TYPE = {
    "Multi-vehicle Collision": {"Trivial Damage": 15, "Minor Damage": 40, "Major Damage": 30, "Total Loss": 15},
    "Single Vehicle Collision": {"Trivial Damage": 8, "Minor Damage": 30, "Major Damage": 35, "Total Loss": 27},
    "Parked Car": {"Trivial Damage": 30, "Minor Damage": 50, "Major Damage": 18, "Total Loss": 2},
    "Vehicle Theft": {"Total Loss": 85, "Major Damage": 10, "Minor Damage": 5},
}
COLLISION_BY_TYPE = {
    "Multi-vehicle Collision": {"Rear Collision": 40, "Side Collision": 33, "Front Collision": 27},
    "Single Vehicle Collision": {"Front Collision": 55, "Side Collision": 30, "Rear Collision": 15},
    "Parked Car": {"Side Collision": 45, "Rear Collision": 25, "Front Collision": 30},
    "Vehicle Theft": None,
}
WITNESS_W = {"Vehicle Theft": [80, 12, 6, 2], "Parked Car": [65, 22, 9, 4],
             "Single Vehicle Collision": [45, 30, 15, 10], "Multi-vehicle Collision": [15, 35, 30, 20]}
# which photo class (Low = dent, Medium = heavy damage, High = crushed) a given TRUE severity produces
IMAGE_CLASS = {"Trivial Damage": {"Low": 90, "Medium": 10}, "Minor Damage": {"Low": 75, "Medium": 25},
               "Major Damage": {"Medium": 65, "High": 25, "Low": 10}, "Total Loss": {"High": 85, "Medium": 15}}
IMAGE_CLASS_ID = {"High": 1, "Low": 2, "Medium": 3}   # same numbering as the original image_metadata.csv

# share of ALL final claims per scenario (the fraud shares sum to ~9.8%)
FRAUD_SHARE = {"inflated_claim": .030, "soon_after_inception": .016, "staged_accident": .018,
               "theft_fraud": .010, "serial_claimant": .016, "duplicate_submission": .008}
GENUINE_SHARE = {"genuine_repeat": .030, "genuine_hard_negative": .015, "genuine_two_insured": .010}
# what the investigators' suspicious_activity flag catches: imperfect in BOTH directions (label noise)
FLAG_RATE = {"genuine": .02, "genuine_repeat": .04, "genuine_hard_negative": .22, "genuine_two_insured": .03,
             "inflated_claim": .75, "soon_after_inception": .85, "staged_accident": .90,
             "theft_fraud": .85, "serial_claimant": .80, "duplicate_submission": .70}


def claim_probability(pol, as_of: date) -> float:
    c, v = pol["cust"], pol["veh"]
    age = years_between(c["dob"], pol["eff"])
    af = 2.0 if age < 21 else 1.7 if age < 25 else 1.1 if age < 30 else 1.0 if age < 65 else 1.2
    annual = (0.10 * af * USE_CLAIMS[v["use"]] * BOROUGH_CLAIMS[c["borough"]]
              * {"lux": 1.1, "ev": 1.05}.get(v["cls"], 1.0) * (0.45 if pol["ptype"] == "TP" else 1.0))
    exposure_days = (min(pol["expiry"], as_of) - pol["eff"]).days + 1
    return max(0.0, annual * exposure_days / 365.0)


def sample_incident_date(rng, a: date, b: date) -> date:
    for _ in range(12):
        d = a + timedelta(days=rng.randint(0, (b - a).days))
        if rng.random() < MONTH_W[d.month - 1] / 1.2:
            return d
    return d


def make_claim(rng, pol, as_of: date, taken: list[date], **o):
    """One claim on one policy. Keyword overrides let the fraud/genuine scenarios shape the claim:
    incident, ctype, sev (claimed severity), true_sev (what the photo shows), collision, nveh, witnesses,
    hour, lag, injury_p, injury_mult, vehicle_exact, no_third_party."""
    c, v = pol["cust"], pol["veh"]
    last = min(pol["expiry"], as_of)
    if last < pol["eff"]:          # renewal issued but not yet in force
        return None
    incident = o.get("incident")
    if incident is None:
        for _ in range(6):   # keep a second claim at least a week from the first
            incident = sample_incident_date(rng, pol["eff"], last)
            if all(abs((incident - t).days) >= 7 for t in taken):
                break
        else:
            return None
    if not pol["eff"] <= incident <= last:
        return None
    value, ded, tp = pol["value"], pol["deductible"], pol["ptype"] == "TP"

    # ---- what happened
    if tp:                                        # liability only: someone else's loss
        ctype = "Multi-vehicle Collision"
        sev = o.get("sev") or pick(rng, {"Trivial Damage": 20, "Minor Damage": 45, "Major Damage": 35})
    else:
        theft_w = 6 * (4 if v["make"] in ("HYUNDAI", "KIA") and 2011 <= v["year"] <= 2022 else 1)
        ctype = o.get("ctype") or pick(rng, {"Multi-vehicle Collision": 50, "Parked Car": 26,
                                              "Single Vehicle Collision": 18, "Vehicle Theft": theft_w})
        sev = o.get("sev") or pick(rng, SEV_BY_TYPE[ctype])
        if "sev" not in o and sev == "Major Damage" and value < 9000 and ctype != "Vehicle Theft" and rng.random() < 0.45:
            sev = "Total Loss"                    # repair costs more than an old car is worth
    true_sev = o.get("true_sev", sev)
    cw = COLLISION_BY_TYPE[ctype]
    collision = o["collision"] if "collision" in o else (pick(rng, cw) if cw else None)
    nveh = o.get("nveh") or {"Multi-vehicle Collision": rng.choices([2, 3, 4], [78, 15, 7])[0], "Single Vehicle Collision": 1,
                             "Vehicle Theft": 1, "Parked Car": rng.choices([1, 2], [55, 45])[0]}[ctype]
    witnesses = o["witnesses"] if "witnesses" in o else rng.choices([0, 1, 2, 3], weights=WITNESS_W[ctype])[0]
    hour = o["hour"] if "hour" in o else rng.choices(range(24), weights=HOUR_W[ctype])[0]

    # ---- how much it cost
    sev_i = SEVERITIES.index(sev)
    inj_mult = o.get("injury_mult", 1.0)
    injury = prop = vehicle = 0
    if tp:
        prop = rnd10(rng.uniform(*{"Trivial Damage": (600, 2500), "Minor Damage": (2500, 7500), "Major Damage": (7500, 32000)}[sev]))
        if rng.random() < (0.04, 0.18, 0.50)[sev_i]:
            injury = rnd10((rng.uniform(1500, 9000) if sev != "Major Damage" else rng.uniform(8000, 60000)) * inj_mult)
    else:
        frac = {"Trivial Damage": (.01, .04), "Minor Damage": (.04, .15), "Major Damage": (.15, .55), "Total Loss": (.90, 1.0)}[sev]
        if ctype == "Vehicle Theft" and sev == "Total Loss":
            frac = (.92, 1.0)
        vehicle = max(rnd10(value * rng.uniform(*frac)), rnd10(ded * 1.1 + 100), 350)
        if "vehicle_exact" in o:
            vehicle = o["vehicle_exact"]
        if ctype in ("Multi-vehicle Collision", "Single Vehicle Collision") and not o.get("no_third_party"):
            p_inj = o.get("injury_p", ((.03, .12, .40, .55) if ctype.startswith("Multi") else (.02, .08, .30, .45))[sev_i])
            if rng.random() < p_inj:
                injury = rnd10((rng.uniform(800, 6000) if sev_i < 2 else rng.uniform(5000, 35000) if sev_i == 2
                                else rng.uniform(10000, 80000)) * inj_mult)
            if ctype.startswith("Multi") and rng.random() < 0.55:      # insured was at fault
                prop = max(400, rnd10(min(60000, vehicle * rng.uniform(.2, .9))))
            elif ctype.startswith("Single") and rng.random() < 0.30:   # pole, fence, guardrail
                prop = rnd10(rng.uniform(500, 6000))

    # ---- reporting
    lag = o["lag"] if "lag" in o else (min(int(rng.expovariate(1 / 1.2)), 10) if ctype == "Vehicle Theft"
                                       else min(int(rng.expovariate(1 / 4.0)), 45))
    lag = min(lag, (as_of - incident).days)
    claim_date = incident + timedelta(days=lag)
    age = years_between(c["dob"], claim_date)
    g, r = c["gender"], rng.random()
    rel = (("own-child", .45), ("unmarried", .30), ("not-in-family", .15), ("other-relative", .10)) if age < 25 else \
          ((("wife" if g == "F" else "husband"), .45), ("unmarried", .25), ("not-in-family", .15), ("own-child", .05), ("other-relative", .10)) if age < 65 else \
          ((("wife" if g == "F" else "husband"), .50), ("not-in-family", .25), ("unmarried", .15), ("other-relative", .10))
    return {"claim_no": str(uuid.UUID(int=rng.getrandbits(128), version=4)), "policy_no": pol["policy_no"],
            "claim_date": claim_date, "months_as_customer": int((claim_date - c["first_eff"]).days // 30.4375),
            "injury": injury, "property": prop, "vehicle": vehicle, "total": injury + prop + vehicle,
            "collision_type": collision, "number_of_vehicles_involved": nveh, "age": age,
            "insured_relationship": rng.choices([x for x, _ in rel], weights=[w for _, w in rel])[0],
            "license_issue_date": c["license_date"], "date": incident, "hour": hour, "type": ctype,
            "severity": sev, "number_of_witnesses": witnesses, "_true_sev": true_sev,
            "_cust_id": c["customer_id"], "_scenario": "genuine", "_group": "", "_linked": ""}


def build_claims(rng, policies, as_of: date, fraud_rate: float):
    """Baseline genuine claims first, then layered scenarios (fraud + genuine look-alikes)."""
    by_cust = defaultdict(list)
    comp_by_borough = defaultdict(list)
    for p in policies:
        by_cust[p["cust"]["customer_id"]].append(p)
        if p["ptype"] == "COMP":
            comp_by_borough[p["cust"]["borough"]].append(p)
    comp = [p for p in policies if p["ptype"] == "COMP"]
    day = timedelta
    claims: list = []
    taken_by_policy: dict = defaultdict(list)

    def add(pol, scenario, group="", linked="", **o):
        cl = make_claim(rng, pol, as_of, taken_by_policy[pol["policy_no"]], **o)
        if cl:
            cl["_scenario"], cl["_group"], cl["_linked"] = scenario, group, linked
            claims.append(cl)
            taken_by_policy[pol["policy_no"]].append(cl["date"])
        return cl

    def policy_on(cust_id, d, ptype="COMP"):
        return next((p for p in by_cust[cust_id] if p["ptype"] == ptype and p["eff"] <= d <= min(p["expiry"], as_of)), None)

    # ---- 1. baseline genuine claims (some policies legitimately have two)
    for pol in policies:
        if rng.random() < claim_probability(pol, as_of):
            for _ in range(2 if rng.random() < 0.10 else 1):
                add(pol, "genuine")
    n0 = len(claims)
    # genuine look-alikes are sized relative to the baseline; fraud is then sized so it makes up `fraud_rate` of ALL claims
    ref_n = n0 / (1 - sum(FRAUD_SHARE.values()) - sum(GENUINE_SHARE.values()))
    target = {k: int(round(ref_n * s)) for k, s in GENUINE_SHARE.items()}
    n_fraud = (n0 + sum(target.values())) * fraud_rate / (1 - fraud_rate)
    target.update({k: int(round(n_fraud * v / sum(FRAUD_SHARE.values()))) for k, v in FRAUD_SHARE.items()})
    made: Counter = Counter()

    def loop(name):   # generic "keep going until the scenario quota is met" with a safety valve
        for _ in range(target[name] * 60 + 200):
            if made[name] >= target[name]:
                return
            yield

    # ---- 2. GENUINE look-alikes
    # 2a. genuine repeat claimants: unlucky / high-risk drivers (young, commercial, rough boroughs), mixed incident types
    repeat_pool = [cid for cid, ps in by_cust.items() if len(ps) >= 2 and any(p["ptype"] == "COMP" for p in ps)]
    repeat_w = [(2.0 if years_between(by_cust[cid][0]["cust"]["dob"], as_of) < 28 else 1.0)
                * (1.8 if by_cust[cid][0]["veh"]["use"] != "PRIVATE" else 1.0) for cid in repeat_pool]
    for _ in loop("genuine_repeat"):
        cid = rng.choices(repeat_pool, weights=repeat_w)[0]
        group, n_new = f"REP-{cid}", 0
        for _k in range(rng.choices([1, 2], [65, 35])[0]):
            if add(rng.choice(by_cust[cid]), "genuine_repeat", group):
                made["genuine_repeat"] += 1
                n_new += 1
    # 2b. hard negatives: brand-new customers with a legit claim that LOOKS like fraud
    firsts = [p for p in comp if p["eff"] == p["cust"]["first_eff"] and p["eff"] < as_of - day(days=30)]
    first_pool = firsts[:]            # each first-term policy is used by at most one scenario
    rng.shuffle(first_pool)
    for _ in loop("genuine_hard_negative"):
        pol = first_pool.pop()
        inc = pol["eff"] + day(days=rng.randint(10, 85))
        v = pol["veh"]
        if v["make"] in ("HYUNDAI", "KIA") and 2011 <= v["year"] <= 2022 and rng.random() < 0.5:
            kw = dict(ctype="Vehicle Theft", sev="Total Loss", hour=rng.randint(0, 5), witnesses=0, lag=rng.randint(0, 1))
        elif rng.random() < 0.5:
            kw = dict(ctype="Single Vehicle Collision", sev=rng.choice(["Major Damage", "Total Loss"]),
                      hour=rng.randint(0, 4), witnesses=0, lag=rng.randint(0, 2))
        else:
            kw = dict(ctype="Multi-vehicle Collision", sev="Major Damage", witnesses=rng.randint(1, 3),
                      injury_p=.9, lag=rng.randint(0, 2))
        if add(pol, "genuine_hard_negative", incident=inc, **kw):
            made["genuine_hard_negative"] += 1
    # 2c. two of OUR insureds collide: both file, same incident -> linked but legitimate
    pair_no = 0
    for _ in loop("genuine_two_insured"):
        a = rng.choice(comp)
        d = a["eff"] + day(days=rng.randint(0, 300))
        if d > min(a["expiry"], as_of):
            continue
        pool = comp_by_borough[a["cust"]["borough"]]
        b = next((p for p in (rng.choice(pool) for _ in range(150))
                  if p["cust"]["customer_id"] != a["cust"]["customer_id"] and p["eff"] <= d <= min(p["expiry"], as_of)), None)
        if not b:
            continue
        pair_no += 1
        hr, wit = rng.choices(range(24), weights=HOUR_W["Multi-vehicle Collision"])[0], rng.choice([0, 1, 1, 2, 3])
        sev_a = pick(rng, {"Trivial Damage": 15, "Minor Damage": 40, "Major Damage": 35, "Total Loss": 10})
        sev_b = pick(rng, {"Trivial Damage": 15, "Minor Damage": 40, "Major Damage": 35, "Total Loss": 10})
        ca = add(a, "genuine_two_insured", f"PAIR-{pair_no}", incident=d, ctype="Multi-vehicle Collision", sev=sev_a,
                 collision="Rear Collision", nveh=2, hour=hr, witnesses=wit, lag=rng.randint(0, 3))
        cb = add(b, "genuine_two_insured", f"PAIR-{pair_no}", incident=d, ctype="Multi-vehicle Collision", sev=sev_b,
                 collision="Front Collision", nveh=2, hour=hr, witnesses=wit, lag=rng.randint(0, 4)) if ca else None
        if ca and cb:
            ca["_linked"], cb["_linked"] = cb["claim_no"], ca["claim_no"]
            made["genuine_two_insured"] += 2
        elif ca:
            claims.remove(ca)
            taken_by_policy[a["policy_no"]].remove(ca["date"])

    # ---- 3. FRAUD scenarios
    # 3a. inflated claims: real but small accident, claimed one or two severity levels higher (photo shows the truth)
    old_enough = [p for p in comp if p["eff"] < as_of - day(days=20)]
    for _ in loop("inflated_claim"):
        true_sev = pick(rng, {"Trivial Damage": 25, "Minor Damage": 55, "Major Damage": 20})
        sev = SEVERITIES[min(3, SEVERITIES.index(true_sev) + rng.choices([1, 2], [55, 45])[0])]
        if add(rng.choice(old_enough), "inflated_claim",
               ctype=pick(rng, {"Multi-vehicle Collision": 50, "Single Vehicle Collision": 25, "Parked Car": 25}),
               sev=sev, true_sev=true_sev, injury_p=0.65, injury_mult=2.0, lag=rng.randint(5, 30),
               witnesses=rng.choices([0, 1, 2], [60, 30, 10])[0]):
            made["inflated_claim"] += 1
    # 3b. soon after inception: big claim within weeks of the very first policy, slow reporting, no witnesses
    for _ in loop("soon_after_inception"):
        pol = first_pool.pop()
        ctype = pick(rng, {"Vehicle Theft": 35, "Single Vehicle Collision": 30, "Multi-vehicle Collision": 35})
        if add(pol, "soon_after_inception", incident=pol["eff"] + day(days=rng.randint(2, 75)), ctype=ctype,
               sev="Total Loss" if ctype == "Vehicle Theft" or rng.random() < 0.8 else "Major Damage",
               witnesses=rng.choices([0, 1, 2], [70, 20, 10])[0], lag=rng.randint(3, 15)):
            made["soon_after_inception"] += 1
    # 3c. staged accidents: a ring of insureds crash into each other at night, same story, big injuries
    ring_no = 0
    for _ in loop("staged_accident"):
        borough = rng.choice(list(comp_by_borough))
        pool = comp_by_borough[borough]
        d0 = rng.choice(pool)["eff"] + day(days=rng.randint(30, 250))
        if d0 > as_of - day(days=40):
            continue
        members = {}
        for p in (rng.choice(pool) for _ in range(500)):
            if p["eff"] <= d0 <= min(p["expiry"], as_of):
                members.setdefault(p["cust"]["customer_id"], p)
        if len(members) < 3:
            continue
        ring = rng.sample(list(members), rng.randint(3, min(5, len(members))))
        ring_no += 1
        d = d0
        for _acc in range(rng.randint(2, 3)):
            active = [(cid, policy_on(cid, d)) for cid in ring]
            active = [(cid, p) for cid, p in active if p]
            if len(active) < 2 or d > as_of - day(days=3):
                break
            part = rng.sample(active, min(len(active), rng.choice([2, 2, 3])))
            hr, sev = rng.choice([20, 21, 22, 23, 0, 1, 2, 3]), pick(rng, {"Trivial Damage": 5, "Minor Damage": 45, "Major Damage": 50})
            wit = rng.choice([2, 3, 3])
            for cid, pol in part:
                if add(pol, "staged_accident", f"RING-{ring_no}", incident=d, ctype="Multi-vehicle Collision", sev=sev,
                       hour=hr, witnesses=wit, nveh=len(part), injury_p=.85, injury_mult=1.5, lag=rng.randint(1, 3),
                       collision=rng.choice(["Rear Collision", "Side Collision"])):
                    made["staged_accident"] += 1
            d += day(days=rng.randint(35, 100))
    # 3d. theft fraud: valuable car "stolen" a few weeks after the term starts, at night, nobody saw, reported late
    valuable = [p for p in comp if p["value"] >= 12000 and p["eff"] < as_of - day(days=40)]
    for _ in loop("theft_fraud"):
        pol = rng.choice(valuable)
        if add(pol, "theft_fraud", incident=pol["eff"] + day(days=rng.randint(20, 150)), ctype="Vehicle Theft",
               sev="Total Loss" if rng.random() < 0.9 else "Major Damage", hour=rng.randint(0, 5), witnesses=0,
               lag=rng.randint(3, 10), collision=None, nveh=1):
            made["theft_fraud"] += 1
    # 3e. serial claimants: 4-7 small claims in ~2 years, mostly parked-car dings priced just over the deductible
    cust_pool = [cid for cid, ps in by_cust.items() if any(p["ptype"] == "COMP" and p["eff"] < as_of - day(days=60) for p in ps)]
    for _ in loop("serial_claimant"):
        cid = rng.choice(cust_pool)
        first = rng.choice([p for p in by_cust[cid] if p["ptype"] == "COMP" and p["eff"] < as_of - day(days=60)])
        d, group, grp = first["eff"] + day(days=rng.randint(10, 120)), f"SER-{cid}", []
        for _k in range(rng.randint(4, 7)):
            pol = policy_on(cid, d)
            if not pol:
                break
            cl = make_claim(rng, pol, as_of, taken_by_policy[pol["policy_no"]], incident=d,
                            ctype=pick(rng, {"Parked Car": 60, "Single Vehicle Collision": 25, "Multi-vehicle Collision": 15}),
                            sev=pick(rng, {"Trivial Damage": 45, "Minor Damage": 45, "Major Damage": 10}),
                            vehicle_exact=rnd10(pol["deductible"] * rng.uniform(1.05, 1.6) + 150), no_third_party=True,
                            witnesses=rng.choices([0, 1], [80, 20])[0], lag=rng.randint(3, 20))
            if cl:
                cl["_scenario"], cl["_group"] = "serial_claimant", group
                grp.append(cl)
                taken_by_policy[pol["policy_no"]].append(cl["date"])
            d += day(days=rng.randint(25, 110))
        if len(grp) >= 3:
            claims.extend(grp)
            made["serial_claimant"] += len(grp)
        else:
            for cl in grp:
                taken_by_policy[cl["policy_no"]].remove(cl["date"])
    # 3f. duplicate submissions: the same incident claimed again days later (double dipping)
    pol_by_no = {p["policy_no"]: p for p in policies}
    originals = [c for c in claims if c["_scenario"] == "genuine" and pol_by_no[c["policy_no"]]["ptype"] == "COMP"
                 and c["claim_date"] < as_of - day(days=30)]
    rng.shuffle(originals)            # each original is duplicated at most once
    for _ in loop("duplicate_submission"):
        if not originals:
            break
        o = originals.pop()
        cd = o["claim_date"] + day(days=rng.randint(2, 25))
        if cd > as_of:
            continue
        j = lambda x: rnd10(x * rng.uniform(0.9, 1.1)) if x else 0
        inj, prop, veh = j(o["injury"]), j(o["property"]), j(o["vehicle"])
        c = pol_by_no[o["policy_no"]]["cust"]
        dup = {**o, "claim_no": str(uuid.UUID(int=rng.getrandbits(128), version=4)), "claim_date": cd,
               "injury": inj, "property": prop, "vehicle": veh, "total": inj + prop + veh,
               "months_as_customer": int((cd - c["first_eff"]).days // 30.4375), "age": years_between(c["dob"], cd),
               "_scenario": "duplicate_submission", "_linked": o["claim_no"], "_group": f"DUP-{o['claim_no'][:8]}"}
        claims.append(dup)
        made["duplicate_submission"] += 1

    # ---- 4. investigator flag (noisy label) and damage photos
    for c in claims:
        c["suspicious_activity"] = rng.random() < FLAG_RATE[c["_scenario"]]
    assign_images(rng, claims)
    claims.sort(key=lambda c: (c["claim_date"], c["claim_no"]))
    return claims


def assign_images(rng, claims) -> None:
    """Each claim gets one of the 15 damage photos, chosen from its TRUE severity.
    Inflated claims therefore show a photo that is milder than the claimed severity; duplicate
    submissions reuse the original's photo; thefts have no damage photo (the car is gone)."""
    by_no: dict = {}
    for c in sorted(claims, key=lambda c: c["_scenario"] == "duplicate_submission"):   # originals first
        if c["_scenario"] == "duplicate_submission":
            c["_image"] = by_no[c["_linked"]]["_image"]
        elif c["type"] == "Vehicle Theft" or rng.random() < 0.03:
            c["_image"] = None
        else:
            cls, k = pick(rng, IMAGE_CLASS[c["_true_sev"]]), rng.randint(1, 5)
            c["_image"] = (f"{k}_{cls}.jpg", (k - 1) * 3 + IMAGE_CLASS_ID[cls])
        by_no[c["claim_no"]] = c


# ---------------------------------------------------------------- validation
def validate(customers, policies, claims, as_of: date) -> None:
    cust = {c["customer_id"]: c for c in customers}
    pol = {p["policy_no"]: p for p in policies}
    assert len(cust) == len(customers) and len(pol) == len(policies), "duplicate keys"
    assert len({c["claim_no"] for c in claims}) == len(claims), "duplicate claim_no"
    for p in policies:
        v, c = p["veh"], p["cust"]
        assert len(v["vin"]) == 17 and v["vin"][8] == vin_check_digit(v["vin"]), f"bad VIN check digit {v['vin']}"
        assert v["vin"][9] == VIN_YEAR[v["year"]], "VIN year code != model year"
        assert p["issue"] <= p["eff"] < p["expiry"] and p["issue"] <= as_of
        assert years_between(c["dob"], p["eff"]) >= 18, "driver under 18"
        assert c["license_date"] <= p["eff"] - timedelta(days=60) and years_between(c["dob"], c["license_date"]) >= 16
        assert p["premium"] > 0 and p["value"] >= 2500
    by_no = {c["claim_no"]: c for c in claims}
    for c in claims:
        p, sc = pol[c["policy_no"]], c["_scenario"]
        assert p["eff"] <= c["date"] <= p["expiry"], "incident outside policy term"
        assert c["date"] <= c["claim_date"] <= as_of, "claim reported before incident / in future"
        assert c["total"] == c["injury"] + c["property"] + c["vehicle"] and c["total"] > 0
        assert 0 <= c["hour"] <= 23 and c["age"] >= 18 and c["months_as_customer"] >= 0
        if p["ptype"] == "TP":
            assert c["vehicle"] == 0 and c["type"] == "Multi-vehicle Collision"
        if c["type"] == "Vehicle Theft":
            assert c["collision_type"] is None and c["number_of_vehicles_involved"] == 1 and c["_image"] is None
        if c["type"] == "Multi-vehicle Collision":
            assert c["number_of_vehicles_involved"] >= 2
        if sc in FRAUD_SHARE or sc == "genuine_hard_negative":
            assert p["ptype"] == "COMP"
        if sc == "duplicate_submission":
            o = by_no[c["_linked"]]
            assert o["policy_no"] == c["policy_no"] and o["date"] == c["date"] and c["claim_date"] > o["claim_date"]
        if sc == "genuine_two_insured":
            o = by_no[c["_linked"]]
            assert o["date"] == c["date"] and o["policy_no"] != c["policy_no"] and o["_linked"] == c["claim_no"]
    groups = Counter(c["_group"] for c in claims if c["_scenario"] in ("serial_claimant", "staged_accident"))
    assert all(n >= 2 for n in groups.values()), "fraud group with a single claim"


def summarize(customers, policies, claims) -> None:
    q = lambda xs, p: sorted(xs)[int(p * (len(xs) - 1))]
    print(f"\ncustomers={len(customers):,}  policies={len(policies):,}  claims={len(claims):,}  "
          f"total rows={len(customers) + len(policies) + len(claims):,}")
    print(f"vehicles={len({p['veh']['vin'] for p in policies}):,}  terms/vehicle="
          f"{len(policies) / len({p['veh']['vin'] for p in policies}):.2f}  claims per policy={len(claims) / len(policies):.3f}")
    prem = [p["premium"] for p in policies if p["ptype"] == "COMP"]
    print(f"COMP premium p10/p50/p90 = ${q(prem, .1):,}/${q(prem, .5):,}/${q(prem, .9):,}   "
          f"TP share={sum(p['ptype'] == 'TP' for p in policies) / len(policies):.1%}")
    print("claim types:", {k: f"{v / len(claims):.0%}" for k, v in Counter(c["type"] for c in claims).most_common()})

    print("\nscenario                 claims  share  flagged  avg total   <4mo   no-witness   median lag")
    for sc, n in Counter(c["_scenario"] for c in claims).most_common():
        s = [c for c in claims if c["_scenario"] == sc]
        lag = sorted((c["claim_date"] - c["date"]).days for c in s)[len(s) // 2]
        print(f"  {sc:22} {n:6} {n / len(claims):6.1%} {sum(c['suspicious_activity'] for c in s) / n:7.0%}  "
              f"${sum(c['total'] for c in s) // n:>8,} {sum(c['months_as_customer'] < 4 for c in s) / n:6.0%} "
              f"{sum(c['number_of_witnesses'] == 0 for c in s) / n:11.0%} {lag:11}d")
    fraud = [c for c in claims if c["_scenario"] in FRAUD_SHARE]
    flagged = [c for c in claims if c["suspicious_activity"]]
    tp = sum(c["_scenario"] in FRAUD_SHARE for c in flagged)
    print(f"\ntrue fraud rate={len(fraud) / len(claims):.1%}   flag rate={len(flagged) / len(claims):.1%}   "
          f"flag precision={tp / len(flagged):.0%}   flag recall={tp / len(fraud):.0%}  (label noise on purpose)")
    per_cust = Counter(c["_cust_id"] for c in claims)
    print("claims per customer:", dict(sorted(Counter(min(n, 6) for n in per_cust.values()).items())), "(6 = 6 or more)")
    with_img = [c for c in claims if c["_image"]]
    rank = {"Low": 0, "Medium": 1, "High": 2}
    mism = lambda c: (c["severity"] in ("Major Damage", "Total Loss")) and c["_image"][0].split("_")[1][:-4] == "Low"
    print(f"photo shows only a dent but claim says Major/Total Loss: inflated={sum(map(mism, [c for c in with_img if c['_scenario'] == 'inflated_claim'])) / max(1, sum(c['_scenario'] == 'inflated_claim' for c in with_img)):.0%}"
          f"  all-other={sum(map(mism, [c for c in with_img if c['_scenario'] != 'inflated_claim'])) / max(1, sum(c['_scenario'] != 'inflated_claim' for c in with_img)):.1%}")
    print(f"claims without a photo: {len(claims) - len(with_img):,} (thefts + 3% not yet submitted)")


# ---------------------------------------------------------------- output: flatten -> garbage -> CSV
CUSTOMER_COLS = ["customer_id", "date_of_birth", "borough", "neighborhood", "zip_code", "name"]
POLICY_COLS = ["POLICY_NO", "CUST_ID", "POLICYTYPE", "POL_ISSUE_DATE", "POL_EFF_DATE", "POL_EXPIRY_DATE", "MAKE", "MODEL",
               "MODEL_YEAR", "CHASSIS_NO", "USE_OF_VEHICLE", "PRODUCT", "SUM_INSURED", "PREMIUM", "DEDUCTABLE"]
CLAIM_COLS = ["claim_no", "policy_no", "claim_date", "months_as_customer", "injury", "property", "vehicle", "total",
              "collision_type", "number_of_vehicles_involved", "age", "insured_relationship", "license_issue_date",
              "date", "hour", "type", "severity", "number_of_witnesses", "suspicious_activity"]


def flatten(customers, policies, claims):
    """Turn the in-memory objects into flat CSV-ready rows (all values are strings)."""
    iso = lambda d: d.isoformat()
    gap = random.Random(7)   # ~0.8% of customers have no DOB on file, like real CRM data
    cust = [{"customer_id": str(c["customer_id"]), "date_of_birth": "" if gap.random() < 0.008 else iso(c["dob"]),
             "borough": c["borough"], "neighborhood": c["neighborhood"], "zip_code": c["zip_code"], "name": c["name"]}
            for c in customers]
    pol = [{"POLICY_NO": p["policy_no"], "CUST_ID": str(p["cust"]["customer_id"]), "POLICYTYPE": p["ptype"],
            "POL_ISSUE_DATE": iso(p["issue"]), "POL_EFF_DATE": iso(p["eff"]), "POL_EXPIRY_DATE": iso(p["expiry"]),
            "MAKE": p["veh"]["make"], "MODEL": p["veh"]["model"], "MODEL_YEAR": str(p["veh"]["year"]),
            "CHASSIS_NO": p["veh"]["vin"], "USE_OF_VEHICLE": p["veh"]["use"], "PRODUCT": p["product"],
            "SUM_INSURED": f"{p['value']}.00", "PREMIUM": f"{p['premium']}.00", "DEDUCTABLE": str(p["deductible"])}
           for p in policies]
    cl = [{"claim_no": c["claim_no"], "policy_no": c["policy_no"], "claim_date": iso(c["claim_date"]),
           "months_as_customer": str(c["months_as_customer"]), "injury": str(c["injury"]), "property": str(c["property"]),
           "vehicle": str(c["vehicle"]), "total": str(c["total"]), "collision_type": c["collision_type"] or "",
           "number_of_vehicles_involved": str(c["number_of_vehicles_involved"]), "age": str(c["age"]),
           "insured_relationship": c["insured_relationship"], "license_issue_date": iso(c["license_issue_date"]),
           "date": iso(c["date"]), "hour": str(c["hour"]), "type": c["type"], "severity": c["severity"],
           "number_of_witnesses": str(c["number_of_witnesses"]),
           "suspicious_activity": "true" if c["suspicious_activity"] else "false"}
          for c in claims]
    return cust, pol, cl


def inject_dirt(rng, cust, pol, cl, pol_ctx, geo, as_of: date, scale: float):
    """Mutate the flat rows in place. Returns (manifest rows, issue catalog rows).
    Rules: one issue per row; values must still load into the SQL columns (types, lengths,
    NOT NULL, CHECK constraints); NOT NULL text columns never get ''/'null'."""
    manifest: list = []
    counts: Counter = Counter()
    catalog: list = []
    touched: set = set()   # rows that already carry an issue
    d = date.fromisoformat
    all_makes = sorted({c[0] for c in CATALOG})
    zips_by_borough = {}
    for (b, _, z), _w in geo:
        zips_by_borough.setdefault(b, set()).add(z)

    def run(table, key, rows, issues):
        for name, rate, expect, _fn in issues:
            catalog.append([table, name, expect])
        for row in rows:
            if (table, row[key]) in touched:
                continue
            u, acc = rng.random(), 0.0
            for name, rate, _expect, fn in issues:
                acc += rate * scale
                if u < acc:
                    changes = fn(row)
                    if changes:
                        touched.add((table, row[key]))
                        for col, new in changes.items():
                            manifest.append([table, row[key], name, DB_COLUMN.get((table, col), col), row[col], new])
                            row[col] = new
                        counts[(table, name)] += 1
                    break

    # ---------------- customers
    def zip_mismatch(r):
        others = sorted(z for b, zs in zips_by_borough.items() if b != r["borough"] for z in zs)   # sorted: set order is not stable
        return {"zip_code": rng.choice(others)} if r["borough"] and others else None

    def name_garbage(r):
        last, _, first = r["name"].partition(", ")
        return {"name": rng.choice([f"{first} {last}", r["name"].upper(), f"  {r['name']} ", "TEST, USER",
                                    "UNKNOWN, UNKNOWN", "N/A"])}

    run("customers", "customer_id", cust, [
        ("geo_missing", .006, "borough IS NOT NULL AND zip_code IS NOT NULL",
         lambda r: {"borough": "", "neighborhood": "", "zip_code": ""}),
        ("borough_format", .004, "borough IN ('BRONX','BROOKLYN','MANHATTAN','QUEENS','STATEN ISLAND')",
         lambda r: {"borough": rng.choice([r["borough"].title(), r["borough"].lower(), r["borough"] + " "])}),
        ("zip_borough_mismatch", .004, "zip_code belongs to borough (check against a zip reference table)", zip_mismatch),
        ("zip_malformed", .004, "zip_code RLIKE '^[0-9]{5}$' AND zip_code BETWEEN '10001' AND '11697'",
         lambda r: {"zip_code": rng.choice(["00000", "99999", "1001 ", "N/A  ", "ABCDE"])}),
        ("dob_future", .002, "date_of_birth <= current_date()",
         lambda r: {"date_of_birth": (as_of + timedelta(days=rng.randint(200, 4000))).isoformat()}),
        ("dob_implausible", .004, "date_of_birth BETWEEN add_months(current_date(), -12*100) AND add_months(current_date(), -12*16)",
         lambda r: {"date_of_birth": rng.choice(["1900-01-01", "1899-12-31", "1850-06-15", "2023-05-17", "2021-11-02"])}),
        ("name_format", .006, "name RLIKE '^[^,]+, [^,]+$' AND name = trim(name)", name_garbage),
    ])

    # ---------------- policies
    def vin_garbage(r):
        v = r["CHASSIS_NO"]
        return {"CHASSIS_NO": rng.choice([v[:16], v + "X", v.lower(), v[:5] + "O" + v[6:], v + " ", "N/A",
                                          "REFER LIST", "0" * 17])}

    def vin_checksum(r):
        v = r["CHASSIS_NO"]
        if len(v) != 17 or v[8] != vin_check_digit(v):
            return None
        return {"CHASSIS_NO": v[:8] + rng.choice([c for c in "0123456789X" if c != v[8]]) + v[9:]}

    def term_wrong(r):
        eff = d(r["POL_EFF_DATE"])
        return {"POL_EXPIRY_DATE": rng.choice([add_years(eff, 5), eff + timedelta(days=30)]).isoformat()}

    run("policies", "POLICY_NO", pol, [
        ("premium_negative", .004, "PREMIUM > 0", lambda r: {"PREMIUM": "-" + r["PREMIUM"]}),
        ("premium_zero", .003, "PREMIUM > 0", lambda r: {"PREMIUM": "0.00"}),
        ("premium_absurd", .002, "PREMIUM < 50000",
         lambda r: {"PREMIUM": rng.choice(["9999999.00", f"{int(float(r['PREMIUM'])) * 100}.00"])}),
        ("sum_insured_bad", .003, "SUM_INSURED BETWEEN 2000 AND 500000",
         lambda r: {"SUM_INSURED": rng.choice(["0.00", "1.00", "99999999.00"])}),
        ("model_year_bad", .003, "MODEL_YEAR BETWEEN 1980 AND year(current_date()) + 1",
         lambda r: {"MODEL_YEAR": rng.choice(["1899", "2035", "2031", "1960"])}),
        ("vin_malformed", .006, "CHASSIS_NO RLIKE '^[A-HJ-NPR-Z0-9]{17}$'", vin_garbage),
        ("vin_bad_checksum", .003, "VIN check digit valid (needs a small UDF)", vin_checksum),
        ("issue_after_effective", .003, "POL_ISSUE_DATE <= POL_EFF_DATE",
         lambda r: {"POL_ISSUE_DATE": (d(r["POL_EFF_DATE"]) + timedelta(days=rng.randint(5, 60))).isoformat()}),
        ("issue_far_future", .001, "POL_ISSUE_DATE <= current_date()", lambda r: {"POL_ISSUE_DATE": "2099-12-31"}),
        ("term_length_wrong", .003, "datediff(POL_EXPIRY_DATE, POL_EFF_DATE) BETWEEN 360 AND 366", term_wrong),
        ("make_model_mismatch", .004, "(MAKE, MODEL) pair exists in a vehicle reference table",
         lambda r: {"MAKE": rng.choice([m for m in all_makes if m != r["MAKE"]])}),
        ("make_garbage", .003, "MAKE = upper(trim(MAKE)) AND MAKE IN (<known makes>)",
         lambda r: {"MAKE": rng.choice(["toyota ", "TOYOTTA", "Honda", "N/A", "UNKNOWN"])}),
        ("use_garbage", .004, "USE_OF_VEHICLE IN ('PRIVATE','COMMERCIAL','RENTACAR')",
         lambda r: {"USE_OF_VEHICLE": rng.choice(["private", "PRIVET", "PRIVATE ", "OTHER", "N/A", ""])}),
        ("deductible_bad", .004, "DEDUCTABLE IN (0, 500, 1000, 2000)",
         lambda r: {"DEDUCTABLE": rng.choice(["-500", "750", "99999", "250"])}),
    ])
    # business duplicates: same car, same term, different policy number
    catalog.append(["policies", "duplicate_policy", "one row per (CHASSIS_NO, POL_EFF_DATE) - dedupe or expect uniqueness"])
    next_no = max(int(r["POLICY_NO"]) for r in pol)
    dups = []
    for r in pol:
        if ("policies", r["POLICY_NO"]) in touched:   # only copy clean rows: keeps "one issue per row" true
            continue
        if rng.random() < .003 * scale:
            next_no += rng.randint(1, 5)
            dup = dict(r)
            dup["POLICY_NO"] = str(next_no)
            dups.append(dup)
            touched.add(("policies", dup["POLICY_NO"]))
            manifest.append(["policies", dup["POLICY_NO"], "duplicate_policy", "POLICY_NO", r["POLICY_NO"], dup["POLICY_NO"]])
            counts[("policies", "duplicate_policy")] += 1
    pol.extend(dups)

    # ---------------- claims
    def amounts(inj, prop, veh):
        return {"injury": str(inj), "property": str(prop), "vehicle": str(veh), "total": str(inj + prop + veh)}

    def incident_outside(r):
        ctx = pol_ctx[r["policy_no"]]
        room = (d(r["claim_date"]) - ctx["expiry"]).days
        if room >= 6 and rng.random() < 0.5:
            return {"date": (ctx["expiry"] + timedelta(days=rng.randint(5, room))).isoformat()}
        return {"date": (ctx["eff"] - timedelta(days=rng.randint(10, 200))).isoformat()}

    def category_garbage(r):
        col = rng.choice(["type", "severity", "collision_type", "insured_relationship"])
        v = r[col]
        opts = [v.lower() if v else "unknown", v.upper() if v else "N/A", "Unknown", "N/A"]
        if col == "collision_type":
            opts.append("Side Collison")
        if col == "severity":
            opts.append("Minor damage")
        return {col: rng.choice(opts)}

    def vehicles_mismatch(r):
        return {"number_of_vehicles_involved": "1" if r["type"].startswith("Multi") else "4"}

    def theft_collision(r):
        return {"collision_type": "Rear Collision"} if r["type"] == "Vehicle Theft" else None

    def amount_neg(r):
        return amounts(-rnd10(rng.uniform(500, 5000)), int(r["property"]), int(r["vehicle"]))

    def amount_outlier(r):
        return amounts(int(r["injury"]), int(r["property"]), rnd10(pol_ctx[r["policy_no"]]["value"] * rng.uniform(3, 10)))

    def tp_vehicle(r):
        if pol_ctx[r["policy_no"]]["ptype"] != "TP":
            return None
        return amounts(int(r["injury"]), int(r["property"]), rnd10(rng.uniform(3000, 20000)))

    run("claims", "claim_no", cl, [
        ("age_implausible", .004, "driver_age BETWEEN 18 AND 100", lambda r: {"age": rng.choice(["0", "5", "15", "117", "150", "220"])}),
        ("claim_before_incident", .006, "claim_date >= incident_date",
         lambda r: {"claim_date": (d(r["date"]) - timedelta(days=rng.randint(1, 60))).isoformat()}),
        ("claim_in_future", .002, "claim_date <= current_date()",
         lambda r: {"claim_date": (as_of + timedelta(days=rng.randint(30, 400))).isoformat()}),
        ("incident_outside_term", .005, "incident_date BETWEEN POL_EFF_DATE AND POL_EXPIRY_DATE (needs a join)", incident_outside),
        ("amount_negative", .004, "injury >= 0 AND property >= 0 AND vehicle >= 0", amount_neg),
        ("amount_zero", .003, "total > 0", lambda r: amounts(0, 0, 0)),
        ("amount_outlier", .003, "vehicle <= SUM_INSURED (needs a join)", amount_outlier),
        ("vehicles_out_of_range", .004, "number_of_vehicles_involved BETWEEN 1 AND 4",
         lambda r: {"number_of_vehicles_involved": rng.choice(["0", "9", "17"])}),
        ("vehicles_type_mismatch", .004, "incident_type LIKE 'Multi%' <=> number_of_vehicles_involved > 1", vehicles_mismatch),
        ("witnesses_outlier", .003, "number_of_witnesses BETWEEN 0 AND 5",
         lambda r: {"number_of_witnesses": rng.choice(["25", "99", "200"])}),
        ("category_garbage", .006, "incident_type / incident_severity / collision_type / insured_relationship IN (<allowed values>)", category_garbage),
        ("license_implausible", .004, "license_issue_date BETWEEN '1930-01-01' AND claim_date",
         lambda r: {"license_issue_date": rng.choice(["1900-01-01", "2099-12-31", (d(r["claim_date"]) + timedelta(days=365)).isoformat()])}),
        ("months_outlier", .003, "months_as_customer <= 60", lambda r: {"months_as_customer": rng.choice(["9999", "1200"])}),
    ])
    # rules that only make sense for a subset of rows get their own pass over just those rows
    run("claims", "claim_no", [r for r in cl if r["type"] == "Vehicle Theft"], [
        ("theft_with_collision", .02, "incident_type <> 'Vehicle Theft' OR collision_type IS NULL", theft_collision)])
    run("claims", "claim_no", [r for r in cl if pol_ctx[r["policy_no"]]["ptype"] == "TP"], [
        ("tp_vehicle_damage", .05, "NOT (policy is TP AND vehicle > 0) (needs a join)", tp_vehicle)])

    issue_rows = [[t, n, e, counts[(t, n)]] for t, n, e in
                  list(dict.fromkeys(map(tuple, catalog)))]
    return manifest, issue_rows


def write_csvs(cust, pol, cl, manifest, issue_rows) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    def dump(name, cols, rows):
        with open(OUT_DIR / name, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(cols)
            for r in rows:
                w.writerow([r[c] for c in cols] if isinstance(r, dict) else r)

    dump("customers.csv", CUSTOMER_COLS, cust)
    dump("policies.csv", POLICY_COLS, pol)
    dump("claims.csv", CLAIM_COLS, cl)
    dump("dirty_manifest.csv", ["table", "key", "issue", "column", "original_value", "injected_value"], manifest)
    dump("dirty_issues.csv", ["table", "issue", "suggested_expectation", "rows_injected"], issue_rows)
    print(f"\nwrote CSVs + answer keys to {OUT_DIR}")


def write_labels(claims, policies) -> None:
    """Answer keys for modelling: the true scenario behind every claim, and image_metadata.csv linking
    claims to damage photos (same layout as the original claims/metadata/image_metadata.csv)."""
    vin = {p["policy_no"]: p["veh"]["vin"] for p in policies}
    with open(OUT_DIR / "claims_ground_truth.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["claim_no", "is_fraud", "scenario", "group_id", "linked_claim_no", "claimed_severity",
                    "true_severity", "suspicious_activity_flag"])
        for c in claims:
            w.writerow([c["claim_no"], "true" if c["_scenario"] in FRAUD_SHARE else "false", c["_scenario"], c["_group"],
                        c["_linked"], c["severity"], c["_true_sev"], "true" if c["suspicious_activity"] else "false"])
    with open(OUT_DIR / "image_metadata.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["image_name", "image_id", "claim_no", "chassis_no"])
        for c in claims:
            if c["_image"]:
                w.writerow([c["_image"][0], c["_image"][1], c["claim_no"], vin[c["policy_no"]]])
    print("wrote claims_ground_truth.csv and image_metadata.csv")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--customers", type=int, default=23_000, help="~100k total rows at the default")
    ap.add_argument("--seed", type=int, default=2026)
    ap.add_argument("--as-of", type=date.fromisoformat, default=date.today(), help="'today' for the simulation")
    ap.add_argument("--fraud-rate", type=float, default=0.40, help="share of ALL claims that are truly fraudulent")
    ap.add_argument("--dirty-rate", type=float, default=1.0, help="garbage multiplier (0 = perfectly clean)")
    a = ap.parse_args()
    rng = random.Random(a.seed)
    geo = load_geography()
    customers, policies = build_policies(rng, a.customers, a.as_of, geo)
    claims = build_claims(rng, policies, a.as_of, a.fraud_rate)
    validate(customers, policies, claims, a.as_of)
    print("all consistency checks passed (on the clean data, before garbage is injected)")
    summarize(customers, policies, claims)

    pol_ctx = {p["policy_no"]: {"eff": p["eff"], "expiry": p["expiry"], "value": p["value"], "ptype": p["ptype"]}
               for p in policies}
    cust, pol, cl = flatten(customers, policies, claims)
    manifest, issue_rows = inject_dirt(random.Random(a.seed + 1), cust, pol, cl, pol_ctx, geo, a.as_of, a.dirty_rate)
    bad_rows = len({(m[0], m[1]) for m in manifest})
    total = len(cust) + len(pol) + len(cl)
    print(f"\ngarbage injected: {len(manifest)} values in {bad_rows} rows ({bad_rows / total:.1%} of {total:,} rows)")
    for t, n, _e, c in issue_rows:
        print(f"  {t:10} {n:24} {c:5}")
    write_csvs(cust, pol, cl, manifest, issue_rows)
    write_labels(claims, policies)


if __name__ == "__main__":
    main()
