# Report: Detecting a Vessel Collision in Danish AIS Data

## 1. Problem and approach

The task is to find, within a month of raw AIS records, the two **moving**
vessels whose tracks intersect in space and time — a collision — inside a 50 nm
radius of `55.225 N, 14.245 E`, and to do so with a big-data engine rather than
plain Pandas, while not being fooled by stationary vessels or GPS noise.

The pipeline is a classic funnel: cheap, highly selective filters run first so
that each later, more expensive stage processes as little data as possible.

```
ingest → validity → time window → geo (bbox → haversine) → GPS-jump removal
       → moving-only → bucketed self-join → closest approach → collision
```

## 2. Data engineering

**Ingestion.** The Danish daily files share a fixed, space-delimited header. We
select only the columns the analysis needs (timestamp, MMSI, lat, lon, nav
status, SOG, COG, heading, name, ship/mobile type) and cast them to proper
types. Timestamps (`dd/MM/yyyy HH:mm:ss`, UTC) are parsed to Spark timestamps so
that ordering and windowing are correct.

**Validity filtering.** AIS is messy. We drop rows with null timestamp/MMSI,
out-of-range coordinates, the `(0,0)` "null island", and the `91/181` "not
available" sentinels. We also keep only Class A/B mobile reports, discarding
base stations and navigational aids.

**Time window.** Restricted to `2021-12-01 00:00:00`–`2021-12-31 23:59:59` UTC.

## 3. Spatial filtering (50 nm)

A naive haversine over every row in the month is wasteful. We apply a **bounding
box first** — a pure column comparison on latitude/longitude derived from the
radius — and only then compute the exact great-circle (haversine) distance on
the survivors, keeping those within 92.6 km (50 nm). The bounding box discards
the overwhelming majority of rows with an almost-free comparison, so the
trigonometry is evaluated on a tiny fraction of the data. The haversine itself
is written purely with Spark SQL column functions (no Python UDF) so it runs
vectorised inside the JVM.

## 4. Defining and excluding "noise"

Two distinct problems are explicitly handled:

### 4.1 Stationary vessels (the "docked adjacent" trap)
Two ships moored next to each other sit a few metres apart for hours and would
otherwise look like a permanent zero-distance collision. We filter the detection
input to fixes with **SOG ≥ 1 knot**. Anchored/moored vessels report ~0 knots
and are removed, so they can never form a candidate pair. This is the primary,
and deliberately simple, defence — it is data-driven (uses the reported speed)
rather than relying on the free-text navigational-status field alone.

### 4.2 GPS jumps / position anomalies
A single corrupt fix can teleport a vessel kilometres in one report, landing it
momentarily on top of another ship and faking a collision. For each MMSI,
ordered by time (Spark window function with `lag`), we compute the great-circle
distance and the elapsed time to the previous fix and derive an **implied
speed**. Fixes implying more than **60 knots** are physically impossible for a
cargo vessel and are discarded *before* the proximity search, so a teleporting
point never reaches the detector. The synthetic test confirms this: a fix
deliberately jumped onto the collision point is removed and produces no false
positive.

## 5. Computational strategy: avoiding the Cartesian product

The core difficulty is that finding close pairs is naturally an all-pairs
problem — `O(N²)` distance computations — which is infeasible for a month of
high-frequency AIS.

Instead, every fix is assigned two **bucket keys**:

* a **time bin** = `floor(epoch_seconds / 60)`, and
* a **spatial grid cell** = `floor((lat − lat₀)/0.01), floor((lon − lon₀)/0.01)`
  (~1.1 km cells).

Two vessels that physically touch must share a small region at the same instant,
so they must share a `(time_bin, cell)` bucket. We therefore perform a **self-
join only within buckets**, which turns the comparison count from quadratic into
roughly linear: each fix is compared only against the handful of other fixes in
its bucket, not against the whole dataset.

To guarantee we never miss a pair that straddles a cell or time-bin boundary,
each fix on the *left* side of the join is expanded into its 3×3 neighbouring
cells and 3 neighbouring time bins (a fixed 27× replication) and joined against
the *right* side's single home bucket. Any pair within one cell / one bin is
then certain to meet. Within each matched bucket we keep only **near-simultaneous
fixes** (timestamps ≤ 30 s apart) and compute the haversine separation.

`mmsi_a < mmsi_b` removes self-matches and duplicate orderings. For each pair we
keep the single **closest point of approach (CPA)** via a window function, then
the collision is simply the pair with the **global minimum CPA** below the
threshold (150 m). The collision time and position are taken from that closest
observation (mid-point of the two vessels).

Other efficiency choices: the cleaned in-area dataset is `persist`-ed because it
is reused for both name resolution and the trajectory plot; Adaptive Query
Execution is enabled so Spark coalesces shuffle partitions once the data has been
shrunk by the filters.

## 6. Verifying the collision

A real collision is distinguishable from ordinary close traffic by the **shape**
of the encounter: the separation falls to near zero at a single instant rather
than staying small indefinitely (which is what a moored pair does). The
`results.txt` therefore lists the top-N closest pairs so the separation between
the true event (CPA of metres) and routine traffic (hundreds of metres) is
visible. On the real data this isolates a single pair whose tracks converge,
touch, and where one track then ends abruptly — consistent with a capsize.

## 7. Findings

On the December 2021 data the pipeline identifies the documented Bornholmsgat
collision between the UK general cargo ship **Scot Carrier** (MMSI 232018267) and
the Danish split hopper barge **Karin Høj** (MMSI 219021240), on
**13 December 2021 at ~03:24 UTC** near **55.22 N, 14.24 E** — squarely inside
the search area. Karin Høj capsized shortly after impact, which the AIS track
reflects as an abrupt end to its position reports.

## 8. Limitations and possible extensions

* The collision threshold and minimum-speed cut are heuristics; tuning them
  trades sensitivity against false positives. They are all centralised in
  `config.py`.
* The closest-approach test is positional. A stricter check could additionally
  require converging courses (COG) before impact and a sudden speed/heading
  change after it.
* The plot uses plain lat/lon with an approximate aspect correction for ~55° N;
  an optional basemap would improve readability but adds a network/tile
  dependency that would break offline container reproducibility.
