#!/usr/bin/env bash
# Case 5: authored UpKeeper catalog reaches two real persistent worker processes.
set -euo pipefail
HARNESS_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_DIR=$(cd "$HARNESS_DIR/../.." && pwd)
export HARNESS_DIR REPO_DIR
. "$HARNESS_DIR/fleet.sh"
. "$HARNESS_DIR/cases.sh"
. "$HARNESS_DIR/lifecycle.sh"
QUAL_PRIVATE_HTTPS=https://github.com/iryzhkov/citadel.git
export QUAL_PRIVATE_HTTPS
FAILURES=0
record() {
  printf '[case] %s %s %s\n' "$2" "$1" "$3" | tee -a "$ROOT/evidence/case5-verdicts.txt"
  [ "$2" = PASS ] || FAILURES=$((FAILURES + 1))
}
trap 'fleet_stop_all; printf "evidence: %s/evidence\n" "${ROOT:-unset}"' EXIT

# Dedicated topology: both workers use the fleet's persistent enrollment path.
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
fleet_worker_config worker-b repo-private
fleet_client_config
fleet_forced_commands
python3 - "$ROOT" <<'PY'
import pathlib,sys
root=pathlib.Path(sys.argv[1])
for host in ("coordinator","worker-a","worker-b"):
    p=root/host/"config.yaml"
    p.write_text(p.read_text().replace("      # worker-a is the one-shot SSH transport", "      connection: persistent-ssh"))
PY
fleet_wrapper "$ROOT/forced/worker-a-exchange" "$ROOT/worker-a/home" "$ROOT/worker-a/ssh_config" \
  "$STEWARD" worker --config "$ROOT/worker-a/config.yaml" bridge
for worker in worker-a worker-b; do
  fleet_worker_bootstrap "$worker"
  fleet_provider_cache "$worker"
done
fleet_start_workers
fleet_restart_worker worker-a
fleet_start_coordinator
sleep 4
for worker in worker-a worker-b; do
  digest=$(fleet_coordinator_cli backlog workers --json | reading worker-catalog "$worker")
  fleet_coordinator_cli worker enroll "$worker" --request-id "case5-initial-$worker" \
    --catalog-revision "$digest" --expected-revision 0 --reason 'disposable initial enrollment' \
    >"$ROOT/evidence/case5-initial-enroll-$worker.out" 2>&1
done
fleet_await_workers
fleet_coordinator_cli backlog workers --json >"$ROOT/evidence/case5-initial-workers.json"
python3 "$HARNESS_DIR/case5_runtime.py" prepare "$ROOT" "${UPKEEPER_SOURCE:?set UPKEEPER_SOURCE to validated source}"
# Applying intent must not have touched the existing worker enrollment.
fleet_coordinator_cli backlog workers --json >"$ROOT/evidence/case5-after-apply-before-restart.json"
kill "$COORDINATOR_PID"
wait "$COORDINATOR_PID" || true
mv "$ROOT/evidence/coordinator.log" "$ROOT/evidence/case5-before-restart.log"
fleet_start_coordinator
sleep 8
fleet_coordinator_cli backlog workers --json >"$ROOT/evidence/case5-missing-observation.json"
for worker in worker-a worker-b; do
  digest=$(reading worker-catalog "$worker" <"$ROOT/evidence/case5-missing-observation.json")
  fleet_coordinator_cli worker enroll "$worker" --request-id "case5-missing-$worker" \
    --catalog-revision "$digest" --expected-revision 1 --reason 'missing Opus observation must refuse' \
    >"$ROOT/evidence/case5-missing-enroll-$worker.out" 2>&1 && code=0 || code=$?
  printf '%s' "$code" >"$ROOT/evidence/case5-missing-enroll-$worker.exit"
done
python3 "$HARNESS_DIR/case5_runtime.py" advertise "$ROOT"
sleep 8
fleet_coordinator_cli backlog workers --json >"$ROOT/evidence/case5-observed-before-enroll.json"
for worker in worker-a worker-b; do
  digest=$(reading worker-catalog "$worker" <"$ROOT/evidence/case5-observed-before-enroll.json")
  fleet_coordinator_cli worker enroll "$worker" --request-id "case5-opus-$worker" \
    --catalog-revision "$digest" --expected-revision 1 --reason 'explicit qualification Opus enrollment' \
    >"$ROOT/evidence/case5-opus-enroll-$worker.out" 2>&1 && code=0 || code=$?
  printf '%s' "$code" >"$ROOT/evidence/case5-opus-enroll-$worker.exit"
done
sleep 8
fleet_coordinator_cli backlog workers --json >"$ROOT/evidence/case5-effective-workers.json"
python3 "$HARNESS_DIR/case5_runtime.py" verify "$ROOT" || FAILURES=$((FAILURES + 1))
for item in 'worker-a plain' 'worker-b beside'; do
  read -r worker project <<<"$item"
  fleet_turn_script "qual-$project" 1 <<'EOF'
#!/bin/sh
printf 'Opus route actually dispatched\n' >result.txt
EOF
  dir=$(fleet_executable_campaign "case5-$worker" "$project")
  python3 - "$dir/workflow.yaml" <<'PY'
import pathlib,sys
p=pathlib.Path(sys.argv[1]);p.write_text(p.read_text().replace("model: synthetic-model","model: claude-opus-4-5"))
PY
  out="$ROOT/evidence/case5-submit-$worker.json"
  if fleet_submit_local "$dir" "case5-opus-task-$worker" "$out"; then
    run=$(run_of "$out")
    if state=$(await_task "$run" 180 succeeded); then
      record "case5-opus-execution-$worker" PASS "$run $state"
    else
      record "case5-opus-execution-$worker" FAIL "$run $state"
    fi
    show_run "$run" "$ROOT/evidence/case5-run-$worker.json" >/dev/null
  else
    record "case5-opus-execution-$worker" FAIL "submission failed"
  fi
done
exit "$((FAILURES > 0))"
