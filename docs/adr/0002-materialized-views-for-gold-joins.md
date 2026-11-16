# 0002. Materialized views with batch joins in the gold layer

**Status:** accepted · **Date:** 2026-11-15

**Context.** Policies and customers are slowly changing reference data. A stream-stream join needs unbounded state and misses claims whose reference row arrives late.
**Decision.** Gold tables are materialized views over batch reads with LEFT joins driven from the claim; orphans are measured with warn-only expectations, not dropped. Geocoding lives in its own streaming table (`address_geocode`) acting as an exactly-once cache, so the final view stays incrementally refreshable.
**Consequences.** No join state to manage and claims never silently disappear. Python UDFs are kept out of materialized views.
