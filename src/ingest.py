"""
Ingestion stage.

Reads the raw Danish AIS CSV files (http://web.ais.dk/aisdata/), keeps only the
columns we need and normalises types. The Danish daily files share a fixed
header; we reference the original column names (which contain spaces) explicitly
so the loader is robust to extra trailing columns.
"""

from pyspark.sql import DataFrame, functions as F

from .spark_utils import build_spark  # noqa: F401  (re-exported for convenience)

# original AIS column name -> internal name
_COLUMNS = {
    "# Timestamp": "ts_raw",
    "Type of mobile": "mobile_type",
    "MMSI": "mmsi",
    "Latitude": "lat",
    "Longitude": "lon",
    "Navigational status": "nav_status",
    "SOG": "sog",
    "COG": "cog",
    "Heading": "heading",
    "Name": "name",
    "Ship type": "ship_type",
}

# Danish AIS timestamps look like "13/12/2021 03:27:05"
_TS_FORMAT = "dd/MM/yyyy HH:mm:ss"


def load_ais(spark, path_glob: str) -> DataFrame:
    """Load one or many AIS CSV files into a typed DataFrame.

    `path_glob` may be a single file or a glob such as ``data/aisdk-2021-12-*.csv``.
    """
    raw = (
        spark.read.option("header", True)
        .option("mode", "PERMISSIVE")
        .csv(path_glob)
    )

    selected = raw.select(
        [F.col(f"`{src}`").alias(dst) for src, dst in _COLUMNS.items()]
    )

    typed = (
        selected
        .withColumn("ts", F.to_timestamp("ts_raw", _TS_FORMAT))
        .withColumn("mmsi", F.col("mmsi").cast("long"))
        .withColumn("lat", F.col("lat").cast("double"))
        .withColumn("lon", F.col("lon").cast("double"))
        .withColumn("sog", F.col("sog").cast("double"))
        .withColumn("cog", F.col("cog").cast("double"))
        .withColumn("heading", F.col("heading").cast("double"))
        .drop("ts_raw")
    )
    return typed
