"""
Central configuration for the AIS collision-detection pipeline.

Every magic number that controls filtering or detection lives here so the
behaviour of the pipeline is auditable in one place and easy to tune without
touching the processing logic.
"""

# --------------------------------------------------------------------------
# Area of interest
# --------------------------------------------------------------------------
# Centre point given by the assignment (Bornholmsgat, Baltic Sea).
CENTER_LAT = 55.225000
CENTER_LON = 14.245000

# Search radius. 1 nautical mile = 1.852 km.
RADIUS_NM = 50.0
NM_TO_KM = 1.852
RADIUS_KM = RADIUS_NM * NM_TO_KM           # ~92.6 km

# --------------------------------------------------------------------------
# Time window (assignment: full December 2021)
# --------------------------------------------------------------------------
START_DATE = "2021-12-01"                  # inclusive
END_DATE = "2021-12-31"                    # inclusive

# --------------------------------------------------------------------------
# "Moving vessel" filter
# --------------------------------------------------------------------------
# Speed Over Ground below which a vessel is treated as stationary
# (anchored / moored / docked). This is the primary mechanism that removes
# "two ships docked adjacent to one another", which would otherwise look like
# a permanent zero-distance pair.
MIN_SOG_KNOTS = 1.0

# --------------------------------------------------------------------------
# GPS-noise / anomaly filter
# --------------------------------------------------------------------------
# Implied speed between two consecutive fixes of the SAME vessel. Anything
# above this is physically impossible for a cargo vessel and is therefore a
# GPS glitch / position jump that must be discarded, otherwise a teleporting
# point could be mistaken for a collision.
MAX_PLAUSIBLE_SPEED_KNOTS = 60.0

# Hard bounds for a usable position fix. AIS uses 91/181 as "not available".
LAT_MIN, LAT_MAX = -90.0, 90.0
LON_MIN, LON_MAX = -180.0, 180.0

# --------------------------------------------------------------------------
# Spatial / temporal bucketing for the candidate join
# --------------------------------------------------------------------------
# Width of a time bin in seconds. Two fixes are only ever compared if they
# fall in the same (or, after expansion, an adjacent) time bin. Class-A AIS
# transmits every 2-10 s while underway, so a 60 s bin still contains several
# fixes per vessel and we never miss a close approach.
TIME_BIN_SECONDS = 60

# Side length of a spatial grid cell in degrees (~1.1 km in latitude here).
# Cell >> collision threshold, and we expand each cell to its 8 neighbours on
# one side of the join, so any pair closer than the threshold is guaranteed to
# share a (time_bin, cell) key. This replaces the O(n^2) Cartesian product
# with a near-linear bucketed self-join.
GRID_CELL_DEG = 0.01

# Only compare two fixes if their timestamps are within this many seconds.
MAX_TIME_DIFF_SECONDS = 30

# Closest-approach distance (metres) at or below which we declare a collision.
# Normal traffic in a TSS keeps hundreds of metres apart; a physical contact
# drives the closest point of approach (CPA) towards ~0.
COLLISION_DISTANCE_M = 150.0

# --------------------------------------------------------------------------
# Visualisation
# --------------------------------------------------------------------------
WINDOW_MINUTES = 10                        # plot +/- this many minutes
TOP_N_CANDIDATES = 10                      # how many closest pairs to report

EARTH_RADIUS_KM = 6371.0088
