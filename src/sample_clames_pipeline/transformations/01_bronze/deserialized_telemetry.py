from pyspark import pipelines as dp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql import DataFrame

# Define the expected structure of the incoming telemetry JSON payload.
telemetry_schema = StructType([
    StructField("chassis_no", StringType(), True),         # Unique vehicle identifier (VIN)
    StructField("latitude", DoubleType(), True),           # GPS latitude coordinate
    StructField("longitude", DoubleType(), True),          # GPS longitude coordinate
    StructField("event_timestamp", TimestampType(), True), # Device-side event generation time
    StructField("speed", DoubleType(), True)               # Vehicle speed at event time
])

@dp.table(
    name="sample_clames.01_bronze.telemetry_events",        # Fully qualified 3-level Unity Catalog target
    comment="Deserialized raw telemetry events",           # Table description visible in Catalog Explorer
    # cluster_by=["chassis_no", "event_timestamp"],          # Liquid Clustering: physically co-locates data by VIN and event time
    table_properties={
        "quality": "bronze",                               # Medallion architecture metadata tag
        "layer": "bronze",                                 # Governance/lineage layer tag
        "delta.enableChangeDataFeed": "true",              # Emits row-level change logs for downstream Silver CDC/streaming reads
        "delta.autoOptimize.optimizeWrite": "true",        # Coalesces small streaming files on write
        "delta.autoOptimize.autoCompact": "true",          # Automatically compacts small files post-write
        "pipelines.reset.allowed": "false"                 # Guardrail: blocks accidental Full Refresh wipes of Bronze history
    }
)
def deserialized_telemetry() -> DataFrame:
    df = (
        spark
        .readStream
        .table("sample_clames.00_landing.raw_telemetry_events")
        .withColumn("value", from_json(col("value").cast("string"), telemetry_schema))
        .select(
            "value.*",
            current_timestamp().alias("bronze_timestamp")
        )
    )
    return df