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

## 2. Quick start (Docker)

### 2a. Get the data

The Danish AIS archive lives at <http://web.ais.dk/aisdata> (mirror of
`http://aisdata.ais.dk`). Each day is a separate zip. The whole month is large
(~2–3 GB unzipped *per day*), so start with just the collision day:

```bash
chmod +x download_data.sh
./download_data.sh 13 13      # downloads + unzips aisdk-2021-12-13 into ./data
# ./download_data.sh 1 31     # the full assignment window (needs lots of disk)
```

Any CSVs placed in `./data/` matching `data/*.csv` will be processed.

### 2b. Build and run with docker compose

```bash
docker compose build
docker compose up
```

Results appear in `./output/`. To process the full month, raise the driver
memory in `docker-compose.yml` (e.g. `SPARK_DRIVER_MEMORY: "8g"`).

### 2c. Or with plain Docker

```bash
docker build -t ais-collision-detection:latest .

docker run --rm \
  -v "$(pwd)/data:/app/data" \
  -v "$(pwd)/output:/app/output" \
  -e SPARK_DRIVER_MEMORY=8g \
  ais-collision-detection:latest
```

---

## 3. Verifying it works without the real data

A synthetic generator reproduces the Danish AIS schema and plants a known
collision plus every noise trap (a moored pair, a GPS jump, an out-of-area
vessel). This is the fastest way to confirm the image is wired correctly:

```bash
# inside the container (or any environment with the deps installed)
python -m tests.make_synthetic data/synthetic.csv
python -m src.main
```

Expected result: `VESSEL_ALPHA` (111111111) vs `VESSEL_BETA` (222222222),
CPA ≈ 0 m at `2021-12-13 03:24:00`, and the moored / jumping / far-away vessels
are **not** reported.

---

## 4. Expected real-world answer

On the real December 2021 data the pipeline converges on the documented
Bornholmsgat collision:

| Vessel | MMSI | Flag |
|---|---|---|
| Scot Carrier | 232018267 | UK |
| Karin Høj | 219021240 | Denmark |

Impact ≈ **13 Dec 2021, 03:24 UTC**, near **55.22 N, 14.24 E**.

---

## 5. Configuration

All tunable thresholds live in `src/config.py` (radius, date window, minimum
speed for "moving", GPS-jump speed ceiling, grid/time bucket sizes, collision
distance threshold, plot window). Environment variables:

| Variable | Default | Meaning |
|---|---|---|
| `AIS_DATA_GLOB` | `data/*.csv` | which CSV files to read |
| `AIS_OUTPUT_DIR` | `output` | where results + plot are written |
| `SPARK_DRIVER_MEMORY` | `4g` | JVM heap for the Spark driver |

---

## 6. Project layout

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
