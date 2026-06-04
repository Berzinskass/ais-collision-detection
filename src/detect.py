"""
Collision-detection stage.

The naive way to find two close vessels is an all-pairs (Cartesian) self-join:
for N fixes that is O(N^2) distance computations and is infeasible on a month of
AIS data. Instead we bucket every fix into a (time_bin, grid_cell) key and only
compare fixes that share a bucket. Because two vessels that physically touch
must be in the same small space at the same instant, this loses no true
collision while reducing the comparison count to roughly linear in N.

To avoid missing a pair that straddles a cell or bin boundary, we expand each
fix on the *left* side of the join into its 3x3 neighbouring cells and 3
neighbouring time bins, and join against the *right* side's single home bucket.
Any pair within one cell / one bin of each other is therefore guaranteed to meet.
"""

from pyspark.sql import DataFrame, Window, functions as F

from . import config
from .spark_utils import haversine_km


def _bucketize(df: DataFrame) -> DataFrame:
    """Attach integer time-bin and grid-cell keys to every fix."""
    return (
        df
        .withColumn(
            "time_bin",
            (F.col("ts").cast("long") / config.TIME_BIN_SECONDS).cast("long"),
        )
        .withColumn(
            "cell_lat",
            F.floor((F.col("lat") - config.CENTER_LAT) / config.GRID_CELL_DEG).cast("int"),
        )
        .withColumn(
            "cell_lon",
            F.floor((F.col("lon") - config.CENTER_LON) / config.GRID_CELL_DEG).cast("int"),
        )
    )


def find_candidate_pairs(moving: DataFrame) -> DataFrame:
    """Return per-bucket close-approach observations between distinct vessels.

    Output columns describe a single near-simultaneous observation of a pair:
    mmsi_a, mmsi_b (with mmsi_a < mmsi_b), the two positions, both timestamps,
    the time gap and the separation distance in metres.
    """
    bucketed = _bucketize(moving).select(
        "mmsi", "ts", "lat", "lon", "sog", "time_bin", "cell_lat", "cell_lon"
    )

    # LEFT side: explode into neighbouring time bins and cells so boundary
    # crossings are still matched. 3 bins x 3 x 3 cells = 27 replicas per fix,
    # a small constant blow-up that guarantees completeness.
    bin_offsets = F.array([F.lit(-1), F.lit(0), F.lit(1)])
    cell_offsets = F.array([F.lit(-1), F.lit(0), F.lit(1)])

    left = (
        bucketed
        .withColumn("dbin", F.explode(bin_offsets))
        .withColumn("dlat", F.explode(cell_offsets))
        .withColumn("dlon", F.explode(cell_offsets))
        .withColumn("k_bin", F.col("time_bin") + F.col("dbin"))
        .withColumn("k_lat", F.col("cell_lat") + F.col("dlat"))
        .withColumn("k_lon", F.col("cell_lon") + F.col("dlon"))
        .select(
            F.col("mmsi").alias("mmsi_a"),
            F.col("ts").alias("ts_a"),
            F.col("lat").alias("lat_a"),
            F.col("lon").alias("lon_a"),
            "k_bin", "k_lat", "k_lon",
        )
    )

    right = bucketed.select(
        F.col("mmsi").alias("mmsi_b"),
        F.col("ts").alias("ts_b"),
        F.col("lat").alias("lat_b"),
        F.col("lon").alias("lon_b"),
        F.col("time_bin").alias("k_bin"),
        F.col("cell_lat").alias("k_lat"),
        F.col("cell_lon").alias("k_lon"),
    )

    joined = (
        left.join(right, on=["k_bin", "k_lat", "k_lon"], how="inner")
        # keep each unordered pair once and drop self-matches
        .filter(F.col("mmsi_a") < F.col("mmsi_b"))
        # only compare near-simultaneous fixes
        .withColumn(
            "dt_sec",
            F.abs(F.col("ts_a").cast("long") - F.col("ts_b").cast("long")),
        )
        .filter(F.col("dt_sec") <= config.MAX_TIME_DIFF_SECONDS)
    )

    with_dist = joined.withColumn(
        "dist_m",
        haversine_km(
            F.col("lat_a"), F.col("lon_a"), F.col("lat_b"), F.col("lon_b")
        ) * 1000.0,
    )

    # A fix may have matched via several neighbour replicas; collapse to the
    # single closest observation per (pair, instant).
    return with_dist.select(
        "mmsi_a", "mmsi_b", "ts_a", "ts_b",
        "lat_a", "lon_a", "lat_b", "lon_b", "dt_sec", "dist_m",
    ).dropDuplicates(["mmsi_a", "mmsi_b", "ts_a", "ts_b"])


def closest_approach_per_pair(pairs: DataFrame) -> DataFrame:
    """For every vessel pair, keep only their single closest-approach record."""
    w = Window.partitionBy("mmsi_a", "mmsi_b").orderBy(F.col("dist_m").asc())
    return (
        pairs.withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )


def detect(moving: DataFrame):
    """Run the full detection and return (best_pair_row, ranked_dataframe).

    The collision is the pair with the global minimum closest-approach distance.
    The collision timestamp/position is taken as the mid-point of the two
    vessels at that instant.
    """
    pairs = find_candidate_pairs(moving)
    cpa = closest_approach_per_pair(pairs).filter(
        F.col("dist_m") <= config.COLLISION_DISTANCE_M
    )

    ranked = (
        cpa
        .withColumn("collision_lat", (F.col("lat_a") + F.col("lat_b")) / 2.0)
        .withColumn("collision_lon", (F.col("lon_a") + F.col("lon_b")) / 2.0)
        # the impact time is the earlier of the two near-simultaneous fixes
        .withColumn(
            "collision_ts",
            F.when(F.col("ts_a") <= F.col("ts_b"), F.col("ts_a")).otherwise(F.col("ts_b")),
        )
        .orderBy(F.col("dist_m").asc())
    )

    best = ranked.limit(1).collect()
    best_row = best[0] if best else None
    return best_row, ranked
