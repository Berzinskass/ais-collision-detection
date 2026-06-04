"""
Shared Spark utilities.

`haversine_km` is expressed purely with Spark SQL column functions (no Python
UDF). This keeps the great-circle distance computation inside the JVM / Catalyst
engine where it is vectorised, instead of paying the serialisation cost of a
row-by-row Python UDF.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Column

from . import config


def build_spark(app_name: str = "ais-collision-detection") -> SparkSession:
    """Create a SparkSession tuned for a single multi-core machine.

    The defaults below are sensible for a laptop / single container. On a real
    cluster these would come from spark-submit / the cluster manager instead.
    """
    import os

    driver_mem = os.environ.get("SPARK_DRIVER_MEMORY", "4g")

    return (
        SparkSession.builder.appName(app_name)
        # Give the single local JVM enough heap for a month of AIS data. Bump
        # SPARK_DRIVER_MEMORY (e.g. "8g") when processing all 31 daily files.
        .config("spark.driver.memory", driver_mem)
        # Adaptive Query Execution lets Spark coalesce shuffle partitions and
        # pick better join strategies at runtime - useful given how aggressively
        # the geo filter shrinks the data.
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        # The raw month is tens of GB; bump the shuffle partition count down once
        # filtered. AQE handles most of this but we set a reasonable ceiling.
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def haversine_km(lat1: Column, lon1: Column, lat2: Column, lon2: Column) -> Column:
    """Great-circle distance in kilometres between two (lat, lon) columns."""
    r = config.EARTH_RADIUS_KM
    dlat = F.radians(lat2 - lat1)
    dlon = F.radians(lon2 - lon1)
    a = (
        F.pow(F.sin(dlat / 2.0), 2)
        + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.pow(F.sin(dlon / 2.0), 2)
    )
    return F.lit(2.0 * r) * F.asin(F.sqrt(a))
