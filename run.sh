#!/bin/sh
set -eu
DATA_DIR="${1:-${DATA_DIR:-/data}}"
OUTPUT_DIR="${2:-${OUTPUT_DIR:-/output}}"
SCRATCH_DIR="${SCRATCH_DIR:-/scratch}"

export DATA_DIR OUTPUT_DIR SCRATCH_DIR
export TMPDIR="$SCRATCH_DIR" HOME="$SCRATCH_DIR"

if [ -f /app/engine ]; then
    exec /app/engine "$DATA_DIR" "$OUTPUT_DIR"
elif [ -f "$(dirname "$0")/engine/target/release/engine" ]; then
    exec "$(dirname "$0")/engine/target/release/engine" "$DATA_DIR" "$OUTPUT_DIR"
else
    exec python3 "$(dirname "$0")/pipeline/pipeline.py" "$DATA_DIR" "$OUTPUT_DIR"
fi
