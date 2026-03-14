#!/bin/bash
# Run ./build/release/duckdb inside the spalm Docker environment.
# Usage:
#   ./run_duckdb.sh                  # interactive DuckDB shell
#   ./run_duckdb.sh file.sql         # run one SQL file
#   ./run_duckdb.sh a.sql b.sql ...  # run multiple SQL files in order

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SPALM_IMAGE="spalm-release"
DUCKDB_IMAGE="duckdb-spalm"

# Build the spalm-release base image only if it doesn't exist
if ! docker image inspect "$SPALM_IMAGE" > /dev/null 2>&1; then
    echo "[1/2] Building base image: $SPALM_IMAGE"
    docker build -t "$SPALM_IMAGE" "$SCRIPT_DIR/spalm-release"
else
    echo "[1/2] Base image '$SPALM_IMAGE' already exists, skipping build."
fi

# Build the duckdb-spalm image only if it doesn't exist
if ! docker image inspect "$DUCKDB_IMAGE" > /dev/null 2>&1; then
    echo "[2/2] Building DuckDB image: $DUCKDB_IMAGE"
    docker build -t "$DUCKDB_IMAGE" "$SCRIPT_DIR"
else
    echo "[2/2] DuckDB image '$DUCKDB_IMAGE' already exists, skipping build."
fi

DUCKDB_BIN="./build/release/duckdb"

if [ "$#" -gt 0 ]; then
    # Pipe all provided .sql files to DuckDB (concatenated in order)
    echo "Running SQL file(s): $*"
    cat "$@" | docker run --rm -i \
        --user "$(id -u):$(id -g)" \
        -v "$SCRIPT_DIR:/workspace" \
        -v "$SCRIPT_DIR/test_data:/test_data:ro" \
        -w /workspace \
        "$DUCKDB_IMAGE" \
        "$DUCKDB_BIN"
else
    # No files given — start an interactive session
    docker run --rm -it \
        --user "$(id -u):$(id -g)" \
        -v "$SCRIPT_DIR:/workspace" \
        -v "$SCRIPT_DIR/test_data:/test_data:ro" \
        -w /workspace \
        "$DUCKDB_IMAGE" \
        "$DUCKDB_BIN"
fi
