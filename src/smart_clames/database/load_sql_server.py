"""Create the Azure SQL tables (sql/01_create_tables.sql) and load the three CSVs.

    uv run python src/smart_clames/load_sql_server.py

Re-runnable: the DDL drops and recreates the tables each time.
CSV -> SQL conversions are limited to what the column types force (see CONVERTERS);
business-level dirt (negative premiums, odd chassis numbers, ...) is loaded as-is on purpose.
"""
import csv
import re
import time
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import mssql_python
from dotenv import dotenv_values

ROOT = Path(__file__).resolve().parents[3]
CSV_DIR = ROOT / "data" / "sql_server"
DDL = ROOT / "sql" / "01_create_tables.sql"
NULL_TOKENS = {"", "null", "NULL", "None", "nan", "NaN"}
# CSV column -> database column, where the table uses a clearer name than the CSV header
DB_COLUMN = {("claims", "age"): "driver_age", ("claims", "date"): "incident_date", ("claims", "hour"): "incident_hour",
             ("claims", "type"): "incident_type", ("claims", "severity"): "incident_severity"}
BATCH = 1000

notes: list[str] = []  # data-handling decisions, reported at the end


# ------------------------------------------------------------------ converters
def _val(v: str):
    return None if v.strip() in NULL_TOKENS else v


def to_int(v: str):
    v = _val(v)
    if v is None:
        return None
    f = float(v)
    if f != int(f):
        raise ValueError(f"expected whole number, got {v!r}")
    return int(f)


def to_decimal(v: str):
    v = _val(v)
    if v is None:
        return None
    d = Decimal(v)
    q = d.quantize(Decimal("0.01"))  # money columns are DECIMAL(12,2)
    if d != q:
        notes.append(f"money value {v} has >2 decimals, rounded to {q}")
    return q


def to_date(v: str):
    v = _val(v)
    if v is None:
        return None
    for fmt in ("%Y-%m-%d", "%d-%m-%Y"):
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}" if fmt == "%Y-%m-%d" else r"\d{2}-\d{2}-\d{4}", v):
            return datetime.strptime(v, fmt).date()
    notes.append(f"unparseable date {v!r} loaded as NULL")
    return None


def to_bool(v: str):
    return {"true": 1, "false": 0}[v.strip().lower()]


def to_str(v: str):
    return _val(v)  # no trimming: whitespace is source dirt we want to see downstream


# (csv column, converter) in table column order
CUSTOMERS = [("customer_id", to_int), ("date_of_birth", to_date), ("borough", to_str),
             ("neighborhood", to_str), ("zip_code", to_str), ("name", to_str)]
POLICIES = [("POLICY_NO", to_str), ("CUST_ID", to_int), ("POLICYTYPE", to_str),
            ("POL_ISSUE_DATE", to_date), ("POL_EFF_DATE", to_date), ("POL_EXPIRY_DATE", to_date),
            ("MAKE", to_str), ("MODEL", to_str), ("MODEL_YEAR", to_int), ("CHASSIS_NO", to_str),
            ("USE_OF_VEHICLE", to_str), ("PRODUCT", to_str), ("SUM_INSURED", to_decimal),
            ("PREMIUM", to_decimal), ("DEDUCTABLE", to_int)]
CLAIMS = [("claim_no", to_str), ("policy_no", to_str), ("claim_date", to_date),
          ("months_as_customer", to_int), ("injury", to_int), ("property", to_int),
          ("vehicle", to_int), ("total", to_int), ("collision_type", to_str),
          ("number_of_vehicles_involved", to_int), ("age", to_int), ("insured_relationship", to_str),
          ("license_issue_date", to_date), ("date", to_date), ("hour", to_int), ("type", to_str),
          ("severity", to_str), ("number_of_witnesses", to_int), ("suspicious_activity", to_bool)]


def read_csv(name: str, spec, csv_dir: Path = CSV_DIR) -> list[tuple]:
    with open(csv_dir / f"{name}.csv", newline="", encoding="utf-8-sig") as f:
        return [tuple(conv(row[col]) for col, conv in spec) for row in csv.DictReader(f)]


def dedupe_policies(rows: list[tuple]) -> list[tuple]:
    """POLICY_NO must be unique. If it repeats, keep the row with the highest PREMIUM."""
    best: dict[str, tuple] = {}
    for r in rows:
        k = r[0]
        if k in best:
            keep = max(best[k], r, key=lambda x: x[13])
            notes.append(f"duplicate POLICY_NO {k}: kept PRODUCT={keep[11]!r} PREMIUM={keep[13]}, "
                         f"dropped the other row")
            best[k] = keep
        else:
            best[k] = r
    return list(best.values())


# ------------------------------------------------------------------ database
def connect():
    e = dotenv_values(ROOT / ".env")
    cs = (f"Server=tcp:{e['AZURE_SQL_HOST']},{e['AZURE_SQL_PORT']};Database={e['AZURE_SQL_DATABASE']};"
          f"Uid={e['AZURE_SQL_USER']};Pwd={{{e['AZURE_SQL_PASSWORD']}}};"
          "Encrypt=yes;TrustServerCertificate=no;")
    for attempt in range(1, 6):   # serverless Azure SQL auto-pauses; the first connection wakes it and may time out
        try:
            return mssql_python.connect(cs)
        except mssql_python.exceptions.OperationalError as e:
            if attempt == 5:
                raise
            print(f"connection attempt {attempt} failed ({str(e)[-60:].strip()}), database may be resuming - retrying")
            time.sleep(15)


def run_ddl(conn) -> None:
    cur = conn.cursor()
    for stmt in re.split(r"^\s*GO\s*$", DDL.read_text(), flags=re.M | re.I):
        if stmt.strip():
            cur.execute(stmt)
    conn.commit()


def insert_sql(table: str, cols: list[str]) -> str:
    quoted = ", ".join(f"[{DB_COLUMN.get((table, c), c)}]" for c in cols)
    return f"INSERT INTO dbo.{table} ({quoted}) VALUES ({', '.join('?' * len(cols))})"


def insert(conn, table: str, cols: list[str], rows: list[tuple]) -> None:
    sql = insert_sql(table, cols)
    cur = conn.cursor()
    for i in range(0, len(rows), BATCH):
        cur.executemany(sql, rows[i:i + BATCH])
    conn.commit()
    print(f"  loaded {len(rows):>6} rows into {table}")


def main() -> None:
    customers = read_csv("customers", CUSTOMERS)
    policies_raw = read_csv("policies", POLICIES)
    policies = dedupe_policies(policies_raw)
    claims = read_csv("claims", CLAIMS)
    print(f"read CSVs: customers={len(customers)} policies={len(policies_raw)} claims={len(claims)}")

    conn = connect()
    print("creating tables ...")
    run_ddl(conn)
    insert(conn, "customers", [c for c, _ in CUSTOMERS], customers)
    insert(conn, "policies", [c for c, _ in POLICIES], policies)
    insert(conn, "claims", [c for c, _ in CLAIMS], claims)

    cur = conn.cursor()
    print("verifying row counts in the database:")
    for t in ("customers", "policies", "claims"):
        cur.execute(f"SELECT COUNT(*) FROM dbo.{t}")
        print(f"  {t:<10} {cur.fetchone()[0]}")
    for n in dict.fromkeys(notes):
        print("note:", n)


if __name__ == "__main__":
    main()
