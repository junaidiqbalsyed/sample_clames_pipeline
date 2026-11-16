"""
GOLD | customer_claim_policy
=============================================================================
Grain      : one row per claim (claim_no)
Sources    : sample_clames.02_silver.claims
             sample_clames.02_silver.policies
             sample_clames.02_silver.customers
Consumers  : customer_claim_policy_telematics (next gold table)

Design decisions
----------------
1. MATERIALIZED VIEW with BATCH reads instead of a stream-stream join.
   Policy and customer are slowly-changing reference data. A stream-stream join
   needs unbounded state (no sensible watermark exists for reference data) and
   silently misses a claim whose policy/customer row arrives later than the
   state is kept. A batch join has no state, always sees the latest silver data,
   and is refreshed incrementally on serverless pipelines.

2. LEFT joins, driven from the claim.
   The previous inner joins made a claim vanish from gold if its policy or
   customer row was missing - in a claims-investigation pipeline that is the
   worst possible failure mode. Orphans are now kept and *measured* with
   expectations (visible in the pipeline event log). Downstream rules treat
   NULL policy fields as "needs investigation". To enforce referential
   integrity instead, switch the two `expect` entries to `expect_or_drop`.

3. Explicit column list (no SELECT *).
   Narrow projection = less data read/shuffled, and the gold schema is a
   deliberate contract: a silver schema change fails loudly here instead of
   leaking into dashboards.

Pipeline settings (optional): smart_claims.catalog = sample_clames
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("smart_claims.catalog", "sample_clames")
SILVER = f"{CATALOG}.02_silver"
GOLD = f"{CATALOG}.03_gold"


@dp.materialized_view(
    name=f"{GOLD}.customer_claim_policy",
    comment="Claims enriched with their policy and customer (one row per claim).",
    # claim_date  -> dashboard date-range filters
    # chassis_no  -> join key used by the telematics enrichment
    cluster_by=["claim_date", "chassis_no"],
    table_properties={"quality": "gold"},
)
@dp.expect_all(
    {
        # Hard identity check on the driving table.
        "valid_claim_no": "claim_no IS NOT NULL",
        # Referential-integrity monitors (warn only - see design note 2).
        "claim_has_policy": "chassis_no IS NOT NULL",
        "claim_has_customer": "customer_id IS NOT NULL",
    }
)
def customer_claim_policy():
    # ----------------------------------------------------------------------- #
    # 1. Read each silver table with a narrow projection.
    # ----------------------------------------------------------------------- #
    claim = spark.read.table(f"{SILVER}.claims").select(
        "claim_no",
        "policy_no",
        "claim_date",
        "months_as_customer",
        "injury",
        "property",
        "vehicle",
        "total",
        "collision_type",
        "number_of_vehicles_involved",
        "driver_age",
        "insured_relationship",
        "license_issue_date",
        "incident_date",
        "incident_hour",
        "incident_type",
        "incident_severity",
        "number_of_witnesses",
        "suspicious_activity",
    )

    # Silver policies keeps the source's UPPER-CASE column names; alias to
    # lower-case so gold has one consistent naming convention (sum_insured,
    # pol_eff_date, ... as the rule engine expects).
    policy_columns = [
        "policy_no", "cust_id", "policytype", "pol_issue_date", "pol_eff_date",
        "pol_expiry_date", "make", "model", "model_year", "chassis_no",
        "use_of_vehicle", "product", "sum_insured", "premium", "deductable",
    ]
    policy = spark.read.table(f"{SILVER}.policies").select(
        *[F.col(c).alias(c) for c in policy_columns]
    )

    customer = (
        spark.read.table(f"{SILVER}.customers")
        .select(
            "customer_id",
            "date_of_birth",
            "borough",
            "neighborhood",
            "zip_code",
            "firstname",
            "lastname",
        )
        # Silver does not carry an address column, so build it here:
        # "BROOKLYN, 11215". NULL when there is no borough (nothing to geocode).
        # NOTE: customer_claim_policy_telematics.py::address_geocode builds the
        # SAME expression - keep the two in sync.
        .withColumn(
            "address",
            F.when(F.col("borough").isNotNull(), F.concat_ws(", ", "borough", "zip_code")),
        )
    )

    # ----------------------------------------------------------------------- #
    # 2. Join. policy_no is joined by name (USING semantics) so it is not
    #    duplicated; cust_id / customer_id are both INT in silver, so a plain
    #    equality join is exact. Spark's adaptive
    #    query execution (AQE) automatically broadcasts whichever side turns
    #    out to be small at runtime, so no manual broadcast() hint is needed
    #    (a wrong hint on a growing table is a classic production regression).
    # ----------------------------------------------------------------------- #
    return claim.join(policy, on="policy_no", how="left").join(
        customer, on=F.col("cust_id") == F.col("customer_id"), how="left"
    )
