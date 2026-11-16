from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


@dp.table(
    name="sample_clames.02_silver.policies",
    comment="Cleaned motor policy data",
    table_properties={
        "quality": "silver",                        # Medallion architecture metadata tag
        "layer": "silver",                          # Governance/lineage layer tag
        "delta.enableChangeDataFeed": "true",       # Emits row-level change logs for downstream CDC/streaming reads
        "delta.autoOptimize.optimizeWrite": "true", # Coalesces small files on write
        "delta.autoOptimize.autoCompact": "true",   # Compacts small files after write
        "pipelines.reset.allowed": "false",         # Blocks accidental full refresh of this table
    },
)
# Data quality rule: rows without a policy number are dropped and counted in pipeline metrics
@dp.expect_or_drop("valid_policy_no", "policy_no IS NOT NULL")
def clean_policies() -> DataFrame:
    return (
        # Incremental read from bronze (streaming table)
        spark.readStream.table("sample_clames.01_bronze.policies")

        # 1. Fix negative premiums (e.g. reversals / sign errors) by taking the absolute value
        .withColumn("premium", F.abs(F.col("premium")))
    )