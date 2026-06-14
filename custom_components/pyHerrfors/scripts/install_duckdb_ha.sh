#!/bin/sh
# One-time duckdb install for Home Assistant OS / Alpine (musl).
# Required on Python 3.14+ where no musllinux wheel exists for duckdb>=1.4.2.
# Run inside the Home Assistant container (SSH / Terminal add-on):
#   sh /config/custom_components/pyHerrfors/scripts/install_duckdb_ha.sh

set -eu

PY_MINOR="$(python3 -c 'import sys; print(sys.version_info.minor)')"
echo "Python 3.${PY_MINOR} detected"

if [ "$PY_MINOR" -lt 14 ]; then
    echo "Installing duckdb==1.3.2 (prebuilt musllinux wheel)..."
    python3 -m pip install --no-cache-dir "duckdb==1.3.2"
else
    echo "Installing duckdb>=1.4.2 (compiling from source, may take 20-40 minutes)..."
    apk add --no-cache cmake g++ gcc linux-headers musl-dev python3-dev
    python3 -m pip install --no-cache-dir "duckdb>=1.4.2"
    apk del cmake g++ gcc linux-headers musl-dev python3-dev || true
fi

python3 -c "import duckdb; print('duckdb', duckdb.__version__, 'OK')"
echo "Done. Restart Home Assistant Core."
