from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


@dp.table(
    name="sample_clames.02_silver.customers",
    comment="Cleaned customer data",
    table_properties={
        "quality": "silver",                        # Medallion architecture metadata tag
        "layer": "silver",                          # Governance/lineage layer tag
        "delta.enableChangeDataFeed": "true",       # Emits row-level change logs for downstream CDC/streaming reads
        "delta.autoOptimize.optimizeWrite": "true", # Coalesces small files on write
        "delta.autoOptimize.autoCompact": "true",   # Compacts small files after write
        "pipelines.reset.allowed": "false",         # Blocks accidental full refresh of this table
    },
)
# Data quality rules: rows failing ANY rule are dropped and counted in pipeline quality metrics.
# They are evaluated on the DataFrame returned below (after the transformations).
@dp.expect_all_or_drop({
    # Every customer must have a key
    "valid_customer_id": "customer_id IS NOT NULL",

    # DOB is mandatory (tracked separately so null DOBs show up as their own metric)
    "valid_date_of_birth": "date_of_birth IS NOT NULL",

    # Age > 16: DOB must be strictly earlier than (today - 16 years).
    # add_months(..., -192) = exactly 16 years back, so leap years are handled correctly
    # (a datediff > 16*365 check would drift by ~4 days).
    "valid_age_over_16": "date_of_birth < add_months(current_date(), -192)",
})
def clean_customer() -> DataFrame:
    # "Last, First" splits into exactly 2 parts on the comma; names without a comma give 1 part
    parts = F.split(F.col("name"), ",")

    return (
        # Incremental read from bronze (streaming table)
        spark.readStream.table("sample_clames.01_bronze.customers")

        # 1. Normalize the name:
        #    - 2 parts  -> flip "Last, First" into "First Last" (trim each part to remove stray spaces)
        #    - otherwise -> keep the name as is
        #    - trim the final string, then initcap (e.g. "marlene SCHMITT" -> "Marlene Schmitt")
        .withColumn(
            "name_norm",
            F.initcap(
                F.trim(
                    F.when(
                        F.size(parts) == 2,
                        F.concat_ws(" ", F.trim(parts[1]), F.trim(parts[0])),
                    ).otherwise(F.col("name"))
                )
            ),
        )

        # 2. Split the normalized name into firstname / lastname.
        #    limit=2 splits only on the first space, so multi-word last names stay together.
        #    lastname is null if the name is a single word.
        .withColumn("firstname", F.split("name_norm", " ", 2)[0])
        .withColumn("lastname", F.split("name_norm", " ", 2)[1])

        # 3. Cast DOB string -> DATE so the age expectation compares dates, not strings.
        #    Unparseable values become null and are dropped by valid_date_of_birth.
        .withColumn("date_of_birth", F.to_date("date_of_birth"))

        # 4. Drop the helper column; it's only needed to derive firstname/lastname
        .drop("name_norm")
    )