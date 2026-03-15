#!/usr/bin/env bash
# Start the long-running scheduled harvest loop with a configurable YAML file.

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_PATH="${BGP_HARVEST_CONFIG:-$PROJECT_ROOT/config.example.yaml}"

exec env PYTHONPATH="$PROJECT_ROOT/src${PYTHONPATH:+:$PYTHONPATH}" \
    python -m bgp_harvest --config "$CONFIG_PATH" harvest schedule "$@"
