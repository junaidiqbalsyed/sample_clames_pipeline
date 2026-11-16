from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_timestamp


@dp.table(
    name="sample_clames.02_silver.telemetry_events",
    comment="Cleaned telemetry events",
    # Clustering is disabled for now. Enable once the common filter columns are known.
    # cluster_by=["chassis_no", "event_timestamp"],
    table_properties={
        "quality": "silver",                        # Medallion architecture metadata tag
        "layer": "silver",                          # Governance/lineage layer tag
        "delta.enableChangeDataFeed": "true",       # Emits row-level change logs for downstream CDC/streaming reads
        "delta.autoOptimize.optimizeWrite": "true", # Coalesces small files on write
        "delta.autoOptimize.autoCompact": "true",   # Compacts small files after write
        "pipelines.reset.allowed": "false"          # Blocks accidental full refresh of this table
    }
)
# Data quality rules: rows failing ANY rule are dropped and counted in the pipeline event log.
# BETWEEN evaluates to NULL for null values, so null coordinates are dropped as well.
@dp.expect_all_or_drop({
    "valid_latitude": "latitude BETWEEN -90 AND 90",
    "valid_longitude": "longitude BETWEEN -180 AND 180",
    "valid_event_timestamp": "event_timestamp IS NOT NULL"  # Catches rows where timestamp parsing failed
})
def clean_telemetry_events() -> DataFrame:
    return (
        # Incremental read from Bronze (streaming table, only new data is processed)
        spark.readStream.table("sample_clames.01_bronze.telemetry_events")

        # Parse the string into a timestamp using the expected format.
        # Unparseable values become NULL and are dropped by the expectation above.
        .withColumn(
            "event_timestamp",
            to_timestamp(col("event_timestamp"), "yyyy-MM-dd HH:mm:ss")
        )
    )