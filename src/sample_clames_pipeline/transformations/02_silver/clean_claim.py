from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql.functions import *

# ==============================================================================
# SILVER TABLE: CLEANED CLAIMS
# ==============================================================================
# Reads raw claims from Bronze, standardizes date formats, and enforces data
# quality expectations before writing to the Silver layer.
@dp.table(
    name="sample_clames.02_silver.claims",
    comment="claims table cleaned",
    # Enable Liquid Clustering on claim_date to optimize date-range queries
    cluster_by=["claim_year", "claim_month"],
    table_properties={
        "quality": "silver",
        "layer": "silver",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
    },
)
# Data quality expectations: automatically drop any records that violate these rules
@dp.expect_all_or_drop({
    # Ensure every claim has a primary identifier
    "valid_claim_id": "claim_no IS NOT NULL",
    # Ensure claim_date is present and not set in the future
    "valid_claim_date": "claim_date IS NOT NULL AND claim_date <= current_date()",
    # Ensure claim_amount is present and strictly positive
    "valid_claim_amount": "total IS NOT NULL AND total > 0",
    # Ensure incident_date is present and not set in the future
    "valid_incident_date": "incident_date IS NOT NULL AND incident_date <= current_date()",
    # Ensure incident_hour falls within a valid 24-hour clock (0–23)
    "valid_incident_hour": "incident_hour BETWEEN 0 AND 23",
    # Ensure Valid driver age
    "valid_driver_age": "driver_age BETWEEN 16 AND 85",
    # Ensure Valid driver license issue date
    "valid_license_issue_date": "license_issue_date <= current_date()"
})
def claims() -> DataFrame:
    return (
        # Stream incrementally from the Bronze claims table
        # (Note: verify catalog spelling 'sample_claims' vs 'sample_clames' above)
        spark.readStream.table("sample_clames.01_bronze.claims")
        # Parse raw MM-dd-yyyy strings into proper SQL Date types
        .withColumn("claim_date", to_date(col("claim_date"), "MM-dd-yyyy"))
        .withColumn("incident_date", to_date(col("incident_date"), "MM-dd-yyyy"))
        .withColumn("license_issue_date", to_date(col("license_issue_date"), "MM-dd-yyyy"))
        .withColumn("claim_year", year(col("claim_date")))
        .withColumn("claim_month", month(col("claim_date")))
    )