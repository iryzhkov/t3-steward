#!/usr/bin/env bash
# Explicit opt-in, read-only GitHub SSH probes from two disposable workers.
set -euo pipefail
: "${QUAL_GITHUB_KEY_PATH:?set an existing authorized SSH key path}"
: "${QUAL_GITHUB_KNOWN_HOSTS_PATH:?set an existing verified known-hosts path}"
[ -f "$QUAL_GITHUB_KEY_PATH" ] && [ -f "$QUAL_GITHUB_KNOWN_HOSTS_PATH" ]
export QUAL_GITHUB_KEY_PATH QUAL_GITHUB_KNOWN_HOSTS_PATH
HARNESS_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_DIR=$(cd "$HARNESS_DIR/../.." && pwd)
export HARNESS_DIR REPO_DIR
. "$HARNESS_DIR/fleet.sh"
QUAL_PRIVATE_HTTPS=https://github.com/iryzhkov/UpKeeper.git
export QUAL_PRIVATE_HTTPS
unset T3_STEWARD_CREDENTIAL_QUAL_GITHUB_SSH
trap 'fleet_stop_all; printf "evidence kept: %s/evidence\n" "${ROOT:-uninitialized}"' EXIT
fleet_require
fleet_init
fleet_build "$REPO_DIR"
fleet_ssh_wrapper
fleet_keys
fleet_repositories
fleet_t3_stub
fleet_sshd
fleet_secrets
fleet_coordinator_config
fleet_worker_config worker-a repo-private
fleet_worker_config worker-b repo-unauthorized
fleet_client_config
fleet_forced_commands
python3 "$HARNESS_DIR/private_github.py" setup "$ROOT"
fleet_worker_bootstrap worker-b
fleet_provider_cache worker-b
fleet_start_workers
fleet_start_coordinator
fleet_enroll_workers
fleet_await_workers
CAMPAIGN=$(fleet_campaign case4-real-private private)
export CAMPAIGN
[ "$(fleet_workflow_count)" = 0 ]
fleet_client_cli main campaign check "$CAMPAIGN" --json >"$ROOT/evidence/private-github-one.json"
python3 "$HARNESS_DIR/private_github.py" verify "$ROOT" one
# Readiness observations are process-local cached evidence. Restart the querying
# coordinator and persistent worker so phase two observes the newly supplied
# credential; repository/ref/catalog bindings remain byte-for-byte identical.
python3 "$HARNESS_DIR/private_github.py" both "$ROOT"
export T3_STEWARD_CREDENTIAL_QUAL_GITHUB_SSH="$QUAL_GITHUB_KEY_PATH"
fleet_restart_worker worker-b
unset T3_STEWARD_CREDENTIAL_QUAL_GITHUB_SSH
kill "$COORDINATOR_PID"
wait "$COORDINATOR_PID" || true
mv "$ROOT/evidence/coordinator.log" "$ROOT/evidence/coordinator-phase-one.log"
fleet_start_coordinator
fleet_await_workers
fleet_client_cli main campaign check "$CAMPAIGN" --json >"$ROOT/evidence/private-github-both.json"
python3 "$HARNESS_DIR/private_github.py" verify "$ROOT" both
[ "$(fleet_workflow_count)" = 0 ]
printf '%s\n' '{"case":4,"result":"PASS","repository":"git@github.com:iryzhkov/UpKeeper.git","phases":["worker-a","both"],"workflowRuns":0}' >"$ROOT/evidence/private-github-result.json"
