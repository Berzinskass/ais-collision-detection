"""
Generate a small synthetic AIS file in the Danish CSV format for testing the
pipeline without downloading the real (tens of GB) dataset.

It deliberately plants every situation the pipeline must handle:
  * VESSEL_ALPHA + VESSEL_BETA   -> a genuine collision (CPA ~ 0 m, both moving)
  * DOCK_ONE + DOCK_TWO          -> two vessels moored ~30 m apart (SOG 0)
                                     => must NOT be reported (stationary trap)
  * GHOST                        -> a moving vessel with one GPS-jump fix that
                                     teleports next to ALPHA
                                     => the jump must be filtered as noise
  * FARAWAY                      -> moving vessel well outside 50 nm
                                     => removed by the geo filter

Run:  python -m tests.make_synthetic data/synthetic.csv
"""

import csv
import math
import os
import sys
from datetime import datetime, timedelta

CENTER_LAT = 55.225000
CENTER_LON = 14.245000

HEADER = [
    "Timestamp", "Type of mobile", "MMSI", "Latitude", "Longitude",
    "Navigational status", "ROT", "SOG", "COG", "Heading", "IMO", "Callsign",
    "Name", "Ship type", "Cargo type", "Width", "Length",
    "Type of position fixing device", "Draught", "Destination", "ETA",
    "Data source type", "A", "B", "C", "D",
]

KM_PER_DEG_LAT = 111.32
KM_PER_DEG_LON = 111.32 * math.cos(math.radians(CENTER_LAT))


def row(ts, mmsi, lat, lon, sog, name, status="Under way using engine"):
    return {
        "Timestamp": ts.strftime("%d/%m/%Y %H:%M:%S"),
        "Type of mobile": "Class A",
        "MMSI": mmsi,
        "Latitude": f"{lat:.6f}",
        "Longitude": f"{lon:.6f}",
        "Navigational status": status,
        "ROT": "0", "SOG": f"{sog:.1f}", "COG": "0", "Heading": "0",
        "IMO": "", "Callsign": "", "Name": name, "Ship type": "Cargo",
        "Cargo type": "", "Width": "10", "Length": "55",
        "Type of position fixing device": "GPS", "Draught": "3",
        "Destination": "", "ETA": "", "Data source type": "AIS",
        "A": "", "B": "", "C": "", "D": "",
    }


def km_offset(lat, lon, dnorth_km, deast_km):
    return lat + dnorth_km / KM_PER_DEG_LAT, lon + deast_km / KM_PER_DEG_LON


def main(out_path):
    rows = []
    t0 = datetime(2021, 12, 13, 3, 14, 0)  # 10 min before the planted impact

    # --- genuine collision: two vessels converging to the same point at t=10min
    # ALPHA travels west-to-east, BETA travels north-to-south, they meet at centre.
    for s in range(0, 21 * 30):  # 21 min, a fix every 2 s
        t = t0 + timedelta(seconds=2 * s)
        frac = (2 * s) / (10 * 60.0)  # 0 -> 1 at impact (600 s)
        # ALPHA: from 3 km west of centre, heading east
        a_lat, a_lon = km_offset(CENTER_LAT, CENTER_LON, 0.0, -3.0 + 3.0 * frac)
        rows.append(row(t, 111111111, a_lat, a_lon, 9.0, "VESSEL_ALPHA"))
        # BETA: from 3 km north of centre, heading south
        b_lat, b_lon = km_offset(CENTER_LAT, CENTER_LON, 3.0 - 3.0 * frac, 0.0)
        rows.append(row(t, 222222222, b_lat, b_lon, 9.0, "VESSEL_BETA"))

    # --- stationary trap: two moored vessels ~30 m apart, SOG 0, for 21 minutes
    for s in range(0, 21):
        t = t0 + timedelta(minutes=s)
        d_lat, d_lon = km_offset(CENTER_LAT, CENTER_LON, 5.0, 5.0)
        rows.append(row(t, 333333333, d_lat, d_lon, 0.0, "DOCK_ONE", "Moored"))
        d2_lat, d2_lon = km_offset(d_lat, d_lon, 0.03, 0.0)  # 30 m north
        rows.append(row(t, 444444444, d2_lat, d2_lon, 0.0, "DOCK_TWO", "Moored"))

    # --- GPS-jump trap: GHOST sits 8 km away but one fix teleports onto centre
    for s in range(0, 21):
        t = t0 + timedelta(minutes=s)
        g_lat, g_lon = km_offset(CENTER_LAT, CENTER_LON, -8.0, -8.0)
        if s == 5:  # single anomalous fix jumps right onto the collision point
            g_lat, g_lon = CENTER_LAT, CENTER_LON
        rows.append(row(t, 555555555, g_lat, g_lon, 8.0, "GHOST"))

    # --- far-away moving vessel, well outside 50 nm (~120 km north)
    for s in range(0, 21):
        t = t0 + timedelta(minutes=s)
        f_lat, f_lon = km_offset(CENTER_LAT, CENTER_LON, 120.0, 0.0)
        rows.append(row(t, 666666666, f_lat, f_lon, 12.0, "FARAWAY"))

    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    with open(out_path, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=HEADER)
        w.writeheader()
        w.writerows(rows)
    print(f"wrote {len(rows)} rows -> {out_path}")


if __name__ == "__main__":
    out = sys.argv[1] if len(sys.argv) > 1 else "data/synthetic.csv"
    main(out)
