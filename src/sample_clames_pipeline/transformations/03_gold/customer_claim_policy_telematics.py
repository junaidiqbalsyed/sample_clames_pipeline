"""
GOLD | customer_claim_policy_telematics   (+ helper table address_geocode)
=============================================================================
Grain      : one row per claim (claim_no)
Sources    : gold.customer_claim_policy      (claim + policy + customer)
             gold.aggregated_telematics      (per-vehicle speed / position)
             silver.customers                (distinct addresses to geocode)
Consumers  : ML_Notebook / Rule_Engine.ipynb (joins USING claim_no),
             the "Claim - geo" dashboard dataset (reads lat_long)

What changed vs. the original 02_silver_to_gold.py, and why
-----------------------------------------------------------
* Geocode each DISTINCT address once, not once per claim row.
  The address is just "BOROUGH, ZIP_CODE", so there are only a few hundred
  distinct values across thousands of claims. Calling a rate-limited geocoding
  API per row was by far the slowest step of the old code.
* The geocode results live in their own streaming table (address_geocode).
  A streaming table with dropDuplicates() is an exactly-once CACHE: each address
  is looked up one time, ever. The final table below is then a plain join,
  so it stays incrementally refreshable (a python UDF in a materialized view
  would force a full recompute on every update).
* Real geocoding is the default. The old code returned RANDOM coordinates
  (uniform -90..90 / -180..180) which put claims in the ocean. A deterministic
  offline "demo" mode is available for dev / no-internet workspaces.
* Claims are no longer dropped when BOROUGH is NULL or no telematics exist;
  both gaps are measured with expectations instead (see customer_claim_policy.py).

Pipeline settings (all optional, defaults shown)
------------------------------------------------
  smart_claims.catalog              = sample_clames
  smart_claims.geocode_mode         = nominatim    # or "demo" (offline, deterministic)
  smart_claims.geocoder_user_agent  = smart-claims-pipeline
        -> Nominatim's usage policy requires an application-identifying
           User-Agent; set this to something with your contact info.
  Environment dependency: geopy   (already in this pipeline's environment;
        only needed when geocode_mode = nominatim.)

Nominatim (the free OpenStreetMap service) is limited to 1 request/second and
is not meant for bulk use. That is fine for a few hundred distinct addresses;
for larger volumes swap `_geocode_nominatim` for a commercial geocoder.
"""

from typing import Iterator

import pandas as pd
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import types as T

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
CATALOG = spark.conf.get("smart_claims.catalog", "sample_clames")
SILVER = f"{CATALOG}.02_silver"
GOLD = f"{CATALOG}.03_gold"

GEOCODE_MODE = spark.conf.get("smart_claims.geocode_mode", "nominatim").lower()
GEOCODER_USER_AGENT = spark.conf.get("smart_claims.geocoder_user_agent", "smart-claims-pipeline")

# Fail fast on a typo instead of silently producing a table full of NULLs.
if GEOCODE_MODE not in {"nominatim", "demo"}:
    raise ValueError(
        f"smart_claims.geocode_mode must be 'nominatim' or 'demo', got '{GEOCODE_MODE}'"
    )

# Appended to the "BOROUGH, ZIP" string to disambiguate it for the geocoder.
GEOCODE_COUNTRY_HINT = "USA"
# Circuit breaker: after this many consecutive *errors* (timeouts, blocked
# egress, HTTP 429) stop calling the API for the rest of the batch so one
# outage cannot stall the pipeline for hours. "No match" is not an error.
MAX_CONSECUTIVE_GEOCODE_ERRORS = 5

_COORDINATES = T.StructType(
    [
        T.StructField("latitude", T.DoubleType()),
        T.StructField("longitude", T.DoubleType()),
    ]
)


# --------------------------------------------------------------------------- #
# Geocoding implementations
# --------------------------------------------------------------------------- #
@F.pandas_udf(_COORDINATES)
def _geocode_nominatim(batches: Iterator[pd.Series]) -> Iterator[pd.DataFrame]:
    """
    Iterator-of-Series pandas UDF: the geocoder object is built ONCE per task
    (not once per row) and reused for every batch the task receives.

    The import is local so that `geopy` is only required on the workers when
    this mode is actually used.
    """
    from geopy.extra.rate_limiter import RateLimiter
    from geopy.geocoders import Nominatim

    geolocator = Nominatim(user_agent=GEOCODER_USER_AGENT, timeout=5, scheme="https")
    # Enforces Nominatim's 1 request/second policy and retries transient errors.
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1.1, max_retries=2, error_wait_seconds=2.0)

    consecutive_errors = 0
    for addresses in batches:
        latitudes, longitudes = [], []
        for address in addresses:
            lat = lon = None
            if address and consecutive_errors < MAX_CONSECUTIVE_GEOCODE_ERRORS:
                try:
                    location = geocode(f"{address}, {GEOCODE_COUNTRY_HINT}")
                    consecutive_errors = 0  # a clean round-trip, even if "not found"
                    if location is not None:
                        lat, lon = location.latitude, location.longitude
                except Exception as exc:  # noqa: BLE001 - network / HTTP / geopy errors
                    consecutive_errors += 1
                    print(f"Geocoding failed for '{address}': {exc}")
            latitudes.append(lat)
            longitudes.append(lon)
        yield pd.DataFrame({"latitude": latitudes, "longitude": longitudes})


def _demo_coordinates(address_col: str):
    """
    Offline stand-in for geocoding: a DETERMINISTIC pseudo-location inside the
    New York City bounding box, derived from a hash of the address.

    Unlike the old random() version, the same address always maps to the same
    point across runs/refreshes, and the points land on the map in NYC (the
    customers are all NYC boroughs). pmod() keeps the hash non-negative without
    abs(), which can overflow on Long.MinValue under ANSI mode.
    """
    lat_unit = F.pmod(F.xxhash64(F.col(address_col), F.lit("lat")), F.lit(1_000_000)) / 1_000_000
    lon_unit = F.pmod(F.xxhash64(F.col(address_col), F.lit("lon")), F.lit(1_000_000)) / 1_000_000
    return F.struct(
        (F.lit(40.55) + lat_unit * 0.35).alias("latitude"),   # 40.55 .. 40.90
        (F.lit(-74.25) + lon_unit * 0.55).alias("longitude"),  # -74.25 .. -73.70
    )


# --------------------------------------------------------------------------- #
# Helper table: one geocode per distinct address (exactly-once cache)
# --------------------------------------------------------------------------- #
@dp.table(
    name=f"{GOLD}.address_geocode",
    comment=(
        "Geocode cache: one row per distinct customer address (BOROUGH, ZIP). "
        "Each address is resolved exactly once."
    ),
    table_properties={"quality": "gold"},
)
@dp.expect("address_geocoded", "latitude IS NOT NULL AND longitude IS NOT NULL")
def address_geocode():
    addresses = (
        spark.readStream.table(f"{SILVER}.customers")
        # Same expression as customer_claim_policy.py - keep the two in sync.
        .select(
            F.when(
                F.col("borough").isNotNull(), F.concat_ws(", ", "borough", "zip_code")
            ).alias("address")
        )
        # NULL address == customer without a borough => nothing to look up.
        .where(F.col("address").isNotNull())
        # Streaming de-duplication keeps one tiny piece of state per distinct
        # address, so each address is geocoded a single time.
        .dropDuplicates(["address"])
    )

    if GEOCODE_MODE == "demo":
        coords = _demo_coordinates("address")
    else:
        # A single partition => a single task => one geocoder instance, so the
        # 1 req/s rate limit is respected globally. (N parallel tasks would
        # hit the API at N req/s.) Cheap because the data is only a few
        # hundred short strings.
        addresses = addresses.repartition(1)
        coords = _geocode_nominatim(F.col("address"))

    return (
        addresses.withColumn("coords", coords)
        .select(
            "address",
            F.col("coords.latitude").alias("latitude"),
            F.col("coords.longitude").alias("longitude"),
            F.lit(GEOCODE_MODE).alias("geocode_source"),  # audit: how was this resolved?
            F.current_timestamp().alias("geocoded_at"),
        )
    )


# --------------------------------------------------------------------------- #
# Final gold table
# --------------------------------------------------------------------------- #
@dp.materialized_view(
    name=f"{GOLD}.customer_claim_policy_telematics",
    comment=(
        "Claim + policy + customer enriched with the claimant's geolocation "
        "(lat_long) and the vehicle's aggregated telematics."
    ),
    cluster_by=["claim_date", "chassis_no"],
    table_properties={"quality": "gold"},
)
@dp.expect_all(
    {
        "valid_claim_no": "claim_no IS NOT NULL",
        "has_geolocation": "lat_long.latitude IS NOT NULL AND lat_long.longitude IS NOT NULL",
        "has_telematics": "telematics_speed IS NOT NULL",
        "valid_lat_long": (
            "lat_long.latitude IS NULL OR "
            "(lat_long.latitude BETWEEN -90 AND 90 AND lat_long.longitude BETWEEN -180 AND 180)"
        ),
    }
)
def customer_claim_policy_telematics():
    claims = spark.read.table(f"{GOLD}.customer_claim_policy")

    # Re-shape the cache into the struct the dashboard / rule engine expect.
    geolocation = spark.read.table(f"{GOLD}.address_geocode").select(
        "address",
        F.struct("latitude", "longitude").alias("lat_long"),
    )

    telematics = spark.read.table(f"{GOLD}.aggregated_telematics")
    telematics_columns = [c for c in telematics.columns if c != "chassis_no"]

    return (
        claims
        # Both enrichments are tiny, 1-row-per-key lookups -> they cannot
        # multiply claim rows, and AQE broadcasts them at runtime.
        .join(geolocation, on="address", how="left")
        .join(telematics, on="chassis_no", how="left")
        # USING-joins move the key columns to the front; restore the natural
        # claim-first column order so the schema stays stable for consumers.
        .select(*claims.columns, "lat_long", *telematics_columns)
    )
