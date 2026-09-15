#!/usr/bin/env bash
# Real worker-home CLI and worker-local polling across restricted SSH.
# Separate synthetic T3 servers deliberately prevent shared-provider false proof.
set -euo pipefail
HARNESS_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_DIR=$(cd "$HARNESS_DIR/../.." && pwd)
export HARNESS_DIR REPO_DIR
QUAL_PRIVATE_HTTPS=https://127.0.0.1:9/disposable.git
export QUAL_PRIVATE_HTTPS
. "$HARNESS_DIR/fleet.sh"
. "$HARNESS_DIR/cases.sh"
. "$HARNESS_DIR/lifecycle.sh"

remote_t3() {
  local host=$1
  LAST_T3_PORT=$(fleet_free_port)
  mkdir -p "$ROOT/turns/$host"
  python3 "$HARNESS_DIR/t3_stub.py" "$LAST_T3_PORT" "$ROOT/evidence/t3-$host.jsonl" "$ROOT/turns/$host" "$QUAL_T3_PROJECTS" >"$ROOT/evidence/t3-$host.log" 2>&1 &
  fleet_track "$!"
  local deadline=$((SECONDS + 15))
  until curl -sf "http://127.0.0.1:$LAST_T3_PORT/health" >/dev/null; do
    [ "$SECONDS" -lt "$deadline" ] || fleet_fail "$host T3 unavailable"
    sleep 0.2
  done
}
finish() {
  local code=$?
  trap - EXIT
  fleet_stop_all
  printf 'remote wait qualification exit=%s root=%s\n' "$code" "$ROOT"
  exit "$code"
}

fleet_require
fleet_init
trap finish EXIT
fleet_build "${REMOTE_WAIT_SOURCE:-$REPO_DIR}"
git -C "${REMOTE_WAIT_SOURCE:-$REPO_DIR}" rev-parse HEAD >"$ROOT/evidence/source-commit.txt"
git -C "${REMOTE_WAIT_SOURCE:-$REPO_DIR}" status --short >"$ROOT/evidence/source-status.txt"
sha256sum "$STEWARD" >"$ROOT/evidence/binary-sha256.txt"
fleet_ssh_wrapper
fleet_keys
fleet_repositories
remote_t3 coordinator
COORDINATOR_T3_PORT=$LAST_T3_PORT
remote_t3 worker-a
WORKER_A_T3_PORT=$LAST_T3_PORT
remote_t3 worker-b
WORKER_B_T3_PORT=$LAST_T3_PORT
fleet_sshd
fleet_secrets
T3_PORT=$COORDINATOR_T3_PORT
fleet_coordinator_config
fleet_client_config
T3_PORT=$WORKER_A_T3_PORT
fleet_worker_config worker-a repo-private
T3_PORT=$WORKER_B_T3_PORT
fleet_worker_config worker-b repo-unauthorized
python3 "$HARNESS_DIR/remote_wait.py" configure "$ROOT" "$COORDINATOR_T3_PORT" "$WORKER_A_T3_PORT" "$WORKER_B_T3_PORT"
fleet_forced_commands
fleet_worker_bootstrap worker-b
fleet_provider_cache worker-b
fleet_provider_cache worker-a
fleet_start_workers
fleet_wrapper "$ROOT/bin/worker-a-run" "$ROOT/worker-a/home" "$ROOT/worker-a/ssh_config" "$STEWARD" run --config "$ROOT/worker-a/config.yaml"
"$ROOT/bin/worker-a-run" >"$ROOT/evidence/worker-a-run.log" 2>&1 &
WAIT_RUNNER_PID=$!
fleet_track "$WAIT_RUNNER_PID"
fleet_start_coordinator
fleet_enroll_workers
fleet_await_workers
mkdir -p "$ROOT/turns/worker-a/qual-park"
cat >"$ROOT/turns/worker-a/qual-park/turn1.sh" <<EOF
#!/bin/sh
set -u
$(fleet_task_env)
unset COORDINATOR_CONFIG
. ./.t3-steward/task.env
printf '%s\n' "\$T3_STEWARD_ATTEMPT_ID" >"$ROOT/evidence/attempt-id.txt"
"$STEWARD" wait add --config "$ROOT/worker-a/config.yaml" --task current --name remote-worker-condition --every 30s --max-every 30s --timeout 4m -- python3 "$HARNESS_DIR/remote_wait.py" condition "$ROOT" >"$ROOT/evidence/register.txt" 2>&1
status=\$?
printf '%s\n' "\$status" >"$ROOT/evidence/register-exit.txt"
[ "\$status" -eq 0 ] || { echo "backlog status: failed remote registration failed"; exit 0; }
echo "parked through worker-home restricted SSH"
EOF
cat >"$ROOT/turns/worker-a/qual-park/turn2.sh" <<EOF
#!/bin/sh
set -eu
test -f .t3-steward/task.env
printf 'worker resumed after its condition\n' > result.txt
EOF
chmod 0755 "$ROOT/turns/worker-a/qual-park/"*.sh
directory=$(fleet_executable_campaign remote-wait park)
fleet_submit_local "$directory" "qual-remote-wait-$$" "$ROOT/evidence/submit.json"
run=$(run_of "$ROOT/evidence/submit.json")
printf '%s\n' "$run" >"$ROOT/evidence/run-id.txt"
deadline=$((SECONDS + 180))
until [ -f "$ROOT/evidence/register-exit.txt" ]; do
  kill -0 "$WAIT_RUNNER_PID" || fleet_fail "worker wait runner exited"
  [ "$SECONDS" -lt "$deadline" ] || fleet_fail "no real CLI registration result"
  sleep 1
done
[ "$(cat "$ROOT/evidence/register-exit.txt")" = 0 ] || fleet_fail "real worker-home CLI registration failed: $(cat "$ROOT/evidence/register.txt")"
for sample in 1 2 3; do
  fleet_log "checking parked sample $sample"
  sleep 3
  python3 "$HARNESS_DIR/remote_wait.py" parked "$ROOT" "$run"
done
touch "$ROOT/worker-a/home/remote-condition-ready"
await_run "$run" 180 succeeded
show_run "$run" "$ROOT/evidence/final.json" >/dev/null
python3 "$HARNESS_DIR/remote_wait.py" verdict "$ROOT" "$run"
