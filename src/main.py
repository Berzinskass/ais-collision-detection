"""
Entry point: orchestrates ingest -> clean -> detect -> visualise and writes the
results file and trajectory plot to the output directory.

Run inside the container as:
    python -m src.main
Configurable via environment variables:
    AIS_DATA_GLOB   (default: data/*.csv)
    AIS_OUTPUT_DIR  (default: output)
"""

import os
import sys

from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel

from . import clean, config, detect, visualize
from .ingest import load_ais
from .spark_utils import build_spark


def main() -> int:
    data_glob = os.environ.get("AIS_DATA_GLOB", "data/*.csv")
    out_dir = os.environ.get("AIS_OUTPUT_DIR", "output")
    os.makedirs(out_dir, exist_ok=True)

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    print(f"[1/6] Loading AIS data from: {data_glob}")
    raw = load_ais(spark, data_glob)

    print("[2/6] Cleaning: validity -> time -> area -> GPS-jump removal")
    valid = clean.basic_validity(raw)
    in_window = clean.filter_time(valid)
    in_area = clean.filter_area(in_window)
    deglitched = clean.remove_gps_jumps(in_area)

    # Cleaned-in-area data is reused twice (names + trajectory plot), so cache it.
    deglitched = deglitched.persist(StorageLevel.MEMORY_AND_DISK)
    n_clean = deglitched.count()
    print(f"      cleaned fixes inside {config.RADIUS_NM:.0f} nm: {n_clean:,}")

    print("[3/6] Filtering to moving vessels (excludes anchored/docked)")
    moving = clean.moving_only(clean.drop_service_vessels(deglitched))

    print("[4/6] Detecting collision via bucketed spatial-temporal self-join")
    best, ranked = detect.detect(moving)

    if best is None:
        print("No vessel pair came within the collision threshold "
              f"({config.COLLISION_DISTANCE_M:.0f} m). Nothing to report.")
        spark.stop()
        return 1

    # Attach human-readable names.
    names = clean.resolve_names(deglitched)
    name_map = {r["mmsi"]: r["vessel_name"] for r in names.collect()}
    name_a = name_map.get(best["mmsi_a"], "UNKNOWN")
    name_b = name_map.get(best["mmsi_b"], "UNKNOWN")

    collision_ts = best["collision_ts"]
    collision_lat = best["collision_lat"]
    collision_lon = best["collision_lon"]

    # ---- results file -----------------------------------------------------
    lines = []
    lines.append("=" * 70)
    lines.append("COLLISION DETECTION RESULT")
    lines.append("=" * 70)
    lines.append(f"Vessel A : {name_a}  (MMSI {best['mmsi_a']})")
    lines.append(f"Vessel B : {name_b}  (MMSI {best['mmsi_b']})")
    lines.append(f"Time     : {collision_ts} UTC")
    lines.append(f"Position : lat {collision_lat:.6f}, lon {collision_lon:.6f}")
    lines.append(f"Closest approach (CPA): {best['dist_m']:.1f} m")
    lines.append("")
    lines.append(f"Top {config.TOP_N_CANDIDATES} closest pairs (sanity check / noise separation):")
    lines.append("-" * 70)
    top = ranked.limit(config.TOP_N_CANDIDATES).collect()
    for r in top:
        a = name_map.get(r["mmsi_a"], "?")
        b = name_map.get(r["mmsi_b"], "?")
        lines.append(
            f"  {r['dist_m']:8.1f} m  {r['collision_ts']}  "
            f"{r['mmsi_a']} ({a}) <-> {r['mmsi_b']} ({b})"
        )
    report = "\n".join(lines)
    print("\n" + report + "\n")

    results_path = os.path.join(out_dir, "results.txt")
    with open(results_path, "w") as fh:
        fh.write(report + "\n")
    print(f"[5/6] Wrote {results_path}")

    # ---- visualisation ----------------------------------------------------
    print("[6/6] Extracting +/-10 min tracks and plotting")
    track_pdf = visualize.extract_window(
        deglitched, best["mmsi_a"], best["mmsi_b"], collision_ts
    )
    plot_path = os.path.join(out_dir, "trajectory.png")
    visualize.plot_collision(
        track_pdf, best["mmsi_a"], name_a, best["mmsi_b"], name_b,
        collision_ts, collision_lat, collision_lon, plot_path,
    )
    print(f"      Wrote {plot_path}")

    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
