"""
GOLD | aggregated_telematics
=============================================================================
Grain      : one row per vehicle (chassis_no)
Source     : sample_clames.02_silver.telemetry_events   (cleaned telemetry events)
Consumers  : customer_claim_policy_telematics (joins on chassis_no)
             Rule_Engine.ipynb (speed_check uses telematics_speed)

Why a MATERIALIZED VIEW (and not a streaming table)?
  * This is a full-history GROUP BY. Streaming tables are append-only and never
    re-compute an aggregate when new rows arrive; a materialized view does.
  * On serverless pipelines, Lakeflow refreshes MVs *incrementally* (only the
    chassis_no groups touched by new events are re-computed) when the source
    Delta table has row tracking enabled (delta.enableRowTracking = true).

Pipeline settings this file relies on (all optional, defaults shown):
  smart_claims.catalog = sample_clames   (the pipeline's catalog)
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# --------------------------------------------------------------------------- #
# Configuration - read from pipeline settings so the same code promotes
# dev -> test -> prod without edits.
# --------------------------------------------------------------------------- #
CATALOG = spark.conf.get("smart_claims.catalog", "sample_clames")
SILVER = f"{CATALOG}.02_silver"
GOLD = f"{CATALOG}.03_gold"


@dp.materialized_view(
    name=f"{GOLD}.aggregated_telematics",
    comment=(
        "Per-vehicle (chassis_no) telematics summary: average speed and "
        "average position over all recorded events."
    ),
    # Downstream join key. chassis_no is a STRING => valid liquid-clustering key.
    cluster_by=["chassis_no"],
    table_properties={"quality": "gold"},
)
# Expectations are *warn-only*: violations are counted in the pipeline event
# log / data-quality UI but no row is dropped (the rule engine has its own
# "Invalid speed" bucket, so we want to keep these vehicles).
@dp.expect_all(
    {
        "non_negative_avg_speed": "telematics_speed IS NULL OR telematics_speed >= 0",
        "valid_avg_latitude": "telematics_latitude IS NULL OR telematics_latitude BETWEEN -90 AND 90",
        "valid_avg_longitude": "telematics_longitude IS NULL OR telematics_longitude BETWEEN -180 AND 180",
    }
)
def aggregated_telematics():
    return (
        # Batch read => materialized view. Fully-qualified name keeps the
        # dependency edge to the silver table defined in this same pipeline.
        spark.read.table(f"{SILVER}.telemetry_events")
        # --- Early projection + filter (optimisation) ------------------------
        # Silver already types speed/latitude/longitude as DOUBLE and drops invalid
        # coordinates, so no casting is needed here.
        # Only the 4 columns we need are read (Delta column pruning), and rows
        # without a chassis_no are removed BEFORE the shuffle. A NULL key can
        # never match a policy in the downstream join anyway, so it would only
        # create a useless "NULL" group.
        .select(
            "chassis_no",
            "speed",
            "latitude",
            "longitude",
        )
        .where(F.col("chassis_no").isNotNull())
        # --- Aggregation -----------------------------------------------------
        # avg() ignores NULLs, so a bad reading does not poison the whole vehicle.
        .groupBy("chassis_no")
        .agg(
            F.avg("speed").alias("telematics_speed"),
            F.avg("latitude").alias("telematics_latitude"),
            F.avg("longitude").alias("telematics_longitude"),
            # Extra, additive columns (do not affect existing consumers):
            F.max("speed").alias("telematics_max_speed"),
            F.count("*").alias("telematics_event_count"),  # how much evidence backs the averages
        )
    )
