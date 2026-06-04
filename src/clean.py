"""
Cleaning / pre-processing stage.

Order of operations (cheapest and most selective filters first, so that every
later, more expensive step sees as little data as possible):

  1. drop structurally invalid rows (null ts/mmsi, sentinel lat/lon)
  2. restrict to the December-2021 window
  3. cheap bounding-box geo pre-filter, then exact haversine <= 50 nm
  4. remove GPS jumps (per-vessel implied speed test)  -> protects against
     teleporting fixes being read as a collision
  5. split into a "moving" subset (SOG above threshold) for collision detection

We keep the full in-area cleaned set as well, because the trajectory plot needs
every fix of the two vessels (including any slow fixes near the impact), not
only the moving ones.
"""

from pyspark.sql import DataFrame, Window, functions as F

from . import config
from .spark_utils import haversine_km


def basic_validity(df: DataFrame) -> DataFrame:
    """Drop rows that cannot possibly be a usable moving-vessel position fix."""
    return df.filter(
        F.col("ts").isNotNull()
        & F.col("mmsi").isNotNull()
        & (F.col("mmsi") > 0)
        & F.col("lat").isNotNull()
        & F.col("lon").isNotNull()
        & F.col("lat").between(config.LAT_MIN, config.LAT_MAX)
        & F.col("lon").between(config.LON_MIN, config.LON_MAX)
        # AIS "not available" sentinels and the null island.
        & (F.col("lat") != 91.0)
        & (F.col("lon") != 181.0)
        & ~((F.col("lat") == 0.0) & (F.col("lon") == 0.0))
        # Keep only real vessel position reports (drop base stations / aids).
        & (F.col("mobile_type").isNull() | (F.col("mobile_type") == "Class A") | (F.col("mobile_type") == "Class B"))
    )


def filter_time(df: DataFrame) -> DataFrame:
    """Keep only the assignment window (inclusive of the whole end day)."""
    start = F.to_timestamp(F.lit(config.START_DATE + " 00:00:00"))
    end = F.to_timestamp(F.lit(config.END_DATE + " 23:59:59"))
    return df.filter((F.col("ts") >= start) & (F.col("ts") <= end))


def filter_area(df: DataFrame) -> DataFrame:
    """50 nm radius filter.

    A bounding box is applied first as a cheap pre-filter so that the more
    expensive haversine is only evaluated for the small set of rows that could
    plausibly be inside the circle.
    """
    import math

    dlat = config.RADIUS_KM / 111.32
    dlon = config.RADIUS_KM / (111.32 * math.cos(math.radians(config.CENTER_LAT)))

    boxed = df.filter(
        F.col("lat").between(config.CENTER_LAT - dlat, config.CENTER_LAT + dlat)
        & F.col("lon").between(config.CENTER_LON - dlon, config.CENTER_LON + dlon)
    )

    dist = haversine_km(
        F.col("lat"), F.col("lon"),
        F.lit(config.CENTER_LAT), F.lit(config.CENTER_LON),
    )
    return boxed.withColumn("dist_center_km", dist).filter(
        F.col("dist_center_km") <= config.RADIUS_KM
    )


def remove_gps_jumps(df: DataFrame) -> DataFrame:
    """Discard fixes that imply an impossible speed relative to the previous fix.

    For each MMSI, ordered by time, we compute the great-circle distance and the
    elapsed time to the previous fix and derive an implied speed. Fixes faster
    than `MAX_PLAUSIBLE_SPEED_KNOTS` are GPS noise / position jumps and removed.
    Implemented with a window function so it stays distributed.
    """
    w = Window.partitionBy("mmsi").orderBy("ts")

    enriched = (
        df
        .withColumn("prev_lat", F.lag("lat").over(w))
        .withColumn("prev_lon", F.lag("lon").over(w))
        .withColumn("prev_ts", F.lag("ts").over(w))
    )

    step_km = haversine_km(
        F.col("prev_lat"), F.col("prev_lon"), F.col("lat"), F.col("lon")
    )
    dt_sec = F.col("ts").cast("long") - F.col("prev_ts").cast("long")

    enriched = (
        enriched
        .withColumn("step_km", step_km)
        .withColumn("dt_sec", dt_sec)
        # knots = (km / 1.852) / (sec / 3600)
        .withColumn(
            "implied_knots",
            F.when(
                F.col("dt_sec") > 0,
                (F.col("step_km") / config.NM_TO_KM) / (F.col("dt_sec") / 3600.0),
            ).otherwise(F.lit(0.0)),
        )
    )

    cleaned = enriched.filter(
        F.col("prev_ts").isNull()  # first fix of a vessel: nothing to compare
        | (F.col("implied_knots") <= config.MAX_PLAUSIBLE_SPEED_KNOTS)
    )

    return cleaned.drop(
        "prev_lat", "prev_lon", "prev_ts", "step_km", "dt_sec", "implied_knots"
    )

def drop_service_vessels(df: DataFrame) -> DataFrame:
    """Remove rescue / coast-guard / tug vessels that swarm an accident scene."""
    name = F.upper(F.coalesce(F.col("name"), F.lit("")))
    return df.filter(
        ~name.contains("RESCUE")
        & ~name.contains("KBV")
        & ~name.contains("SVITZER")
        & ~name.contains("PILOT")
        & ~name.contains("SAR")
    )

def moving_only(df: DataFrame) -> DataFrame:
    """Keep fixes where the vessel is actually under way.

    This is the key defence against the "two ships docked next to each other"
    trap: stationary vessels report SOG ~ 0 and are dropped, so they can never
    form a (false) zero-distance pair.
    """
    return df.filter(
        F.col("sog").isNotNull() & (F.col("sog") >= config.MIN_SOG_KNOTS)
    )


def resolve_names(df: DataFrame) -> DataFrame:
    """One row per MMSI carrying the best-known vessel name."""
    named = df.filter(F.col("name").isNotNull() & (F.trim(F.col("name")) != ""))
    w = Window.partitionBy("mmsi").orderBy(F.col("ts").asc())
    return (
        named.withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .select("mmsi", F.trim(F.col("name")).alias("vessel_name"))
    )
