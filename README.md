# AIS Collision Detection (Danish AIS, December 2021)

Detects the pair of vessels that collided (closest physical approach) inside a
**50 nm radius of 55.225000 N, 14.245000 E** during **December 2021**, using
Danish AIS data and **Apache Spark (PySpark)**, then plots both trajectories
for the 20-minute window around the impact (±10 min).

The processing (loading, cleaning, geo/time filtering, noise removal and the
proximity search) is done entirely in Spark. Pandas is used only to render the
final ~few-hundred-point plot.

---

## 1. What it produces

When the container runs it writes two files to `./output/`:

* **`results.txt`** — the MMSI numbers, vessel names, exact timestamp and
  coordinates of the collision, plus the top-N closest pairs as a sanity check.
* **`trajectory.png`** — the two vessels' trajectories ±10 minutes around the
  collision, with start markers and the impact point.

---

## 2. Configuration

All tunable thresholds live in `src/config.py` (radius, date window, minimum
speed for "moving", GPS-jump speed ceiling, grid/time bucket sizes, collision
distance threshold, plot window). Environment variables:

| Variable | Default | Meaning |
|---|---|---|
| `AIS_DATA_GLOB` | `data/*.csv` | which CSV files to read |
| `AIS_OUTPUT_DIR` | `output` | where results + plot are written |
| `SPARK_DRIVER_MEMORY` | `4g` | JVM heap for the Spark driver |

---

## 3. Project layout

```
src/
  config.py        # all thresholds / parameters
  spark_utils.py   # SparkSession builder + vectorised haversine column
  ingest.py        # read Danish AIS CSVs, normalise schema/types
  clean.py         # validity, time, geo, GPS-jump and stationary filters
  detect.py        # bucketed spatial-temporal self-join + CPA detection
  visualize.py     # extract ±10 min tracks and render the plot
  main.py          # orchestration + output
tests/
  make_synthetic.py  # generates a test CSV with a planted collision + noise
report.md          # methodology and findings
Dockerfile
docker-compose.yml
download_data.sh
requirements.txt
```

See `report.md` for the methodology, the noise-handling rationale and the
computational-efficiency discussion.
