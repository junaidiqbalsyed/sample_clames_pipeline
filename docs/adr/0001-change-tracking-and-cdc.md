# 0001. Enable both Change Tracking and CDC on the source tables

**Status:** accepted · **Date:** 2026-11-14

**Context.** Lakeflow Connect for SQL Server needs Change Tracking (CT) or CDC. CT is lighter but only records which keys changed; CDC keeps full change history.
**Decision.** Enable both on `dbo.customers`, `policies`, `claims` (CT retention 14 days) so either ingestion mode can be tested.
**Consequences.** CDC blocks `sp_rename` on tracked columns and CHECK-constrained columns: disable CDC, drop the constraint, rename, re-create, re-enable. Re-running `load_sql_server.py` drops the tables and removes CT/CDC and trips the DDL audit trigger, so use `load_synthetic.py --reload` for data refreshes.
