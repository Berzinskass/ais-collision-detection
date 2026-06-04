#!/usr/bin/env bash
#
# Download and unzip Danish AIS daily files for December 2021 into ./data
#
# The full month is large (~300-500 MB zipped, ~2-3 GB unzipped PER DAY).
# For a first test, download a single day (the 13th, which contains the
# collision) rather than all 31:
#
#     ./download_data.sh 13 13          # just 2021-12-13
#     ./download_data.sh 1 31           # the whole month (default)
#
set -euo pipefail

START_DAY="${1:-1}"
END_DAY="${2:-31}"
BASE_URL="http://web.ais.dk/aisdata"     # mirror of http://aisdata.ais.dk
DEST="data"

mkdir -p "$DEST"

for d in $(seq -w "$START_DAY" "$END_DAY"); do
    name="aisdk-2021-12-${d}"
    zip="${name}.zip"
    if [[ -f "${DEST}/${name}.csv" ]]; then
        echo "[skip] ${name}.csv already present"
        continue
    fi
    echo "[get ] ${zip}"
    curl -fSL "${BASE_URL}/${zip}" -o "${DEST}/${zip}"
    echo "[unzip] ${zip}"
    unzip -o "${DEST}/${zip}" -d "${DEST}" >/dev/null
    rm -f "${DEST}/${zip}"
done

echo "Done. CSV files in ./${DEST}:"
ls -lh "${DEST}"/*.csv
