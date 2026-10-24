#!/bin/sh
# One-time duckdb install for Home Assistant OS / Alpine (musl).
# Required on Python 3.14+ where no musllinux wheel exists for duckdb>=1.4.2.
# Run inside the Home Assistant container (SSH / Terminal add-on):
#   sh /config/custom_components/pyHerrfors/scripts/install_duckdb_ha.sh

set -eu

# Home Assistant Core ships its own interpreter under /usr/local. Prefer that
# over Alpine's python3, which apk may also install.
if [ -x /usr/local/bin/python3 ]; then
    PYTHON=/usr/local/bin/python3
else
    PYTHON="$(command -v python3)"
fi

PY_MINOR="$("$PYTHON" -c 'import sys; print(sys.version_info.minor)')"
echo "Using ${PYTHON} (Python 3.${PY_MINOR})"

if [ "$PY_MINOR" -lt 14 ]; then
    echo "Installing duckdb==1.3.2 (prebuilt musllinux wheel)..."
    "$PYTHON" -m pip install --no-cache-dir "duckdb==1.3.2"
else
    echo "Installing duckdb>=1.4.2 (compiling from source, may take 20-40 minutes)..."
    # Do not apk-add python3 / python3-dev: those pull Alpine's Python (e.g.
    # 3.14.8) and CMake then fails to match Home Assistant's 3.14.9 headers.
    apk add --no-cache cmake g++ gcc linux-headers musl-dev

    # DuckDB 2.x requires ninja >= 1.10 (Alpine samurai is 1.2) and NumPy
    # headers at configure time. Install both into HA's Python so CMake can
    # see them. Isolated builds look at this interpreter, not the overlay.
    "$PYTHON" -m pip install --no-cache-dir \
        "ninja>=1.10" \
        "numpy>=2.0" \
        "nanobind>=3.0" \
        "scikit-build-core>=0.11.4" \
        "cmake>=3.29"

    NINJA_BIN="$("$PYTHON" -c 'from ninja import BIN_DIR; print(BIN_DIR)')"
    export PATH="${NINJA_BIN}:${PATH}"

    PY_INCLUDE="$("$PYTHON" -c 'import sysconfig; print(sysconfig.get_path("include"))')"
    if [ ! -f "${PY_INCLUDE}/Python.h" ]; then
        echo "Python.h not in ${PY_INCLUDE}; trying Alpine python3-dev headers..."
        apk add --no-cache python3-dev
        if [ -f "/usr/include/python3.${PY_MINOR}/Python.h" ]; then
            PY_INCLUDE="/usr/include/python3.${PY_MINOR}"
        fi
    fi
    if [ ! -f "${PY_INCLUDE}/Python.h" ]; then
        echo "Could not find Python.h. Cannot compile duckdb against ${PYTHON}."
        exit 1
    fi

    NP_INCLUDE="$("$PYTHON" -c 'import numpy; print(numpy.get_include())')"
    echo "Python headers: ${PY_INCLUDE}"
    echo "NumPy headers:  ${NP_INCLUDE}"

    # --no-build-isolation so CMake uses HA's Python (with numpy) instead of
    # an isolated env that cannot see these headers.
    "$PYTHON" -m pip install --no-cache-dir --no-build-isolation \
        --config-settings="cmake.define.Python_EXECUTABLE=${PYTHON}" \
        --config-settings="cmake.define.Python_INCLUDE_DIR=${PY_INCLUDE}" \
        --config-settings="cmake.define.Python_NumPy_INCLUDE_DIR=${NP_INCLUDE}" \
        "duckdb>=1.4.2"

    apk del cmake g++ gcc linux-headers musl-dev python3-dev || true
fi

"$PYTHON" -c "import duckdb; print('duckdb', duckdb.__version__, 'OK')"
echo "Done. Restart Home Assistant Core."
