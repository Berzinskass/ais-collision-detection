"""
Visualisation stage.

Spark does the heavy lifting; by the time we plot we are dealing with at most a
few hundred fixes (two vessels over a 20-minute window), so we collect that tiny
slice to the driver and render it with matplotlib. The big-data constraint
applies to *processing*, not to plotting a handful of points.
"""

import os
from datetime import timedelta

import matplotlib

matplotlib.use("Agg")  # headless / container-safe backend
import matplotlib.pyplot as plt  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from . import config


def extract_window(cleaned_in_area, mmsi_a, mmsi_b, collision_ts):
    """Return a small pandas DataFrame of both vessels' fixes around impact."""
    lo = collision_ts - timedelta(minutes=config.WINDOW_MINUTES)
    hi = collision_ts + timedelta(minutes=config.WINDOW_MINUTES)

    slice_df = (
        cleaned_in_area
        .filter(F.col("mmsi").isin([mmsi_a, mmsi_b]))
        .filter((F.col("ts") >= F.lit(lo)) & (F.col("ts") <= F.lit(hi)))
        .select("mmsi", "ts", "lat", "lon", "sog")
        .orderBy("mmsi", "ts")
    )
    return slice_df.toPandas()


def plot_collision(track_pdf, mmsi_a, name_a, mmsi_b, name_b,
                   collision_ts, collision_lat, collision_lon, out_path):
    """Render the two trajectories and mark the collision point."""
    fig, ax = plt.subplots(figsize=(11, 9))

    colors = {mmsi_a: "#1f77b4", mmsi_b: "#d62728"}
    labels = {mmsi_a: f"{name_a} ({mmsi_a})", mmsi_b: f"{name_b} ({mmsi_b})"}

    for mmsi in (mmsi_a, mmsi_b):
        g = track_pdf[track_pdf["mmsi"] == mmsi].sort_values("ts")
        if g.empty:
            continue
        ax.plot(g["lon"], g["lat"], "-o", ms=3, lw=1.4,
                color=colors[mmsi], label=labels[mmsi])
        # mark the start of each track
        ax.scatter(g["lon"].iloc[0], g["lat"].iloc[0],
                   color=colors[mmsi], marker="s", s=70, zorder=5,
                   edgecolor="black")
        ax.annotate("start", (g["lon"].iloc[0], g["lat"].iloc[0]),
                    textcoords="offset points", xytext=(6, 6), fontsize=8)

    # the collision itself
    ax.scatter(collision_lon, collision_lat, marker="X", s=260,
               color="black", zorder=6, label="collision")
    ax.annotate(
        f"COLLISION\n{collision_ts:%Y-%m-%d %H:%M:%S} UTC\n"
        f"{collision_lat:.5f}, {collision_lon:.5f}",
        (collision_lon, collision_lat),
        textcoords="offset points", xytext=(10, -40), fontsize=9,
        bbox=dict(boxstyle="round,pad=0.4", fc="wheat", ec="black", alpha=0.9),
    )

    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    ax.set_title(
        f"Vessel trajectories +/- {config.WINDOW_MINUTES} min around collision\n"
        f"{name_a} vs {name_b}"
    )
    ax.legend(loc="best")
    ax.grid(True, alpha=0.3)
    ax.set_aspect(1.0 / 0.57)  # rough lon/lat aspect correction at ~55N

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    return out_path
