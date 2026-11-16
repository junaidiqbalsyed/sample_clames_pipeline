"""Append the synthetic CSVs (generate_synthetic.py) to the Azure SQL tables WITHOUT touching existing rows.

    uv run python src/smart_clames/load_synthetic.py            # first load
    uv run python src/smart_clames/load_synthetic.py --reload   # delete previously loaded synthetic rows, load again

Synthetic rows are recognisable by key range: customer_id >= 100000 and POLICY_NO starting with '3'
(original customers are < 15000 and original policy numbers start with '10').
NOTE: load_sql_server.py drops and recreates the tables, which also wipes the synthetic rows.
"""
import argparse
import time
from pathlib import Path

import load_sql_server as L

SYN = Path(__file__).resolve().parents[3] / "data" / "sql_server_synthetic"


def count(cur, sql: str) -> int:
    cur.execute(sql)
    return cur.fetchone()[0]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--reload", action="store_true", help="remove earlier synthetic rows first")
    args = ap.parse_args()

    customers = L.read_csv("customers", L.CUSTOMERS, SYN)
    policies = L.read_csv("policies", L.POLICIES, SYN)
    claims = L.read_csv("claims", L.CLAIMS, SYN)
    print(f"read synthetic CSVs: customers={len(customers)} policies={len(policies)} claims={len(claims)}")
    for n in dict.fromkeys(L.notes):
        print("note:", n)

    conn = L.connect()
    cur = conn.cursor()
    where_c, where_p = "customer_id >= 100000", "POLICY_NO LIKE '3%'"
    existing = count(cur, f"SELECT COUNT(*) FROM dbo.customers WHERE {where_c}")
    print(f"synthetic rows already in the database: {existing} customers")
    if existing:
        if not args.reload:
            raise SystemExit("synthetic data is already loaded - use --reload to replace it")
        cur.execute("DELETE FROM dbo.claims WHERE policy_no LIKE '3%'")
        cur.execute(f"DELETE FROM dbo.policies WHERE {where_p}")
        cur.execute(f"DELETE FROM dbo.customers WHERE {where_c}")
        conn.commit()
        print("removed previously loaded synthetic rows")

    t0 = time.time()
    L.insert(conn, "customers", [c for c, _ in L.CUSTOMERS], customers)
    L.insert(conn, "policies", [c for c, _ in L.POLICIES], policies)
    L.insert(conn, "claims", [c for c, _ in L.CLAIMS], claims)
    print(f"loaded in {time.time() - t0:.0f}s")

    print("database totals (original + synthetic):")
    for t in ("customers", "policies", "claims"):
        print(f"  {t:<10} {count(cur, f'SELECT COUNT(*) FROM dbo.{t}'):>7}")


if __name__ == "__main__":
    main()
