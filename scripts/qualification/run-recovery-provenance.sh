#!/usr/bin/env bash
# Isolated supplemental real-process cases 9, 10 and 16.
set -euo pipefail
HARNESS_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_DIR=$(cd "$HARNESS_DIR/../.." && pwd)
export HARNESS_DIR REPO_DIR
. "$HARNESS_DIR/fleet.sh"
QUAL_PRIVATE_HTTPS=https://github.com/iryzhkov/citadel.git
export QUAL_PRIVATE_HTTPS
trap 'fleet_stop_all; printf "evidence kept: %s/evidence\n" "${ROOT:-uninitialized}"' EXIT
fleet_setup "$REPO_DIR"
python3 "$HARNESS_DIR/recovery_provenance.py" "$ROOT" "$T3_PORT"
