#!/usr/bin/env bash
# Supplemental disposable qualification. Never uses installed binaries or live state.
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
  printf '[case] %s %s %s\n' "$2" "$1" "$3" | tee -a "$ROOT/evidence/supplemental-verdicts.txt"
  [ "$2" = PASS ] || FAILURES=$((FAILURES + 1))
}
trap 'fleet_stop_all; printf "evidence: %s/evidence\n" "${ROOT:-unset}"' EXIT
fleet_setup "$REPO_DIR"

# Change only the coordinator-authored catalog. Do not silently enroll the new one.
cp "$ROOT/coordinator/config.yaml" "$ROOT/evidence/case6-before-config.yaml"
fleet_coordinator_cli backlog workers --json >"$ROOT/evidence/case6-before.json"
kill "$COORDINATOR_PID"
wait "$COORDINATOR_PID" || true
python3 - "$ROOT/coordinator/config.yaml" <<'PY'
import pathlib,sys
p=pathlib.Path(sys.argv[1])
p.write_text(p.read_text().replace("models: [synthetic-model]", "models: [synthetic-model, synthetic-added]"))
PY
mv "$ROOT/evidence/coordinator.log" "$ROOT/evidence/coordinator-before-drift.log"
fleet_start_coordinator
sleep 8
fleet_coordinator_cli backlog workers --json >"$ROOT/evidence/case6-drift.json"
dir=$(fleet_campaign case6 beside)
fleet_client_cli main campaign check "$dir" --json >"$ROOT/evidence/case6-check.json" 2>"$ROOT/evidence/case6-check.err" || true

# Provider observation is independent of desired authorization. Advertise the added
# model before attempting a valid enrollment; otherwise the safety gate correctly
# refuses even the right revision because the route is unavailable.
python3 - "$ROOT/worker-b/t3/caches/synthetic.json" <<'PY'
import pathlib,json,sys
p=pathlib.Path(sys.argv[1]); d=json.loads(p.read_text())
d["models"].append({"slug":"synthetic-added"})
p.write_text(json.dumps(d))
PY
sleep 8
# Evidence-driven enrollment attempts: stale catalog, stale enrollment revision, correct fence.
old=$(python3 "$HARNESS_DIR/inspect.py" worker-catalog worker-b <"$ROOT/evidence/case6-before.json")
new=$(python3 "$HARNESS_DIR/inspect.py" worker-catalog worker-b <"$ROOT/evidence/case6-drift.json")
fleet_coordinator_cli worker enroll worker-b --request-id qual-drift-stale-catalog --catalog-revision "$old" --reason qualification --expected-revision 1 >"$ROOT/evidence/case6-stale-catalog.txt" 2>&1 && old_code=0 || old_code=$?
fleet_coordinator_cli worker enroll worker-b --request-id qual-drift-stale-revision --catalog-revision "$new" --reason qualification --expected-revision 0 >"$ROOT/evidence/case6-stale-revision.txt" 2>&1 && rev_code=0 || rev_code=$?
fleet_coordinator_cli worker enroll worker-b --request-id qual-drift-correct --catalog-revision "$new" --reason qualification --expected-revision 1 >"$ROOT/evidence/case6-correct.txt" 2>&1 && correct_code=0 || correct_code=$?
printf '%s %s %s\n' "$old_code" "$rev_code" "$correct_code" >"$ROOT/evidence/case6-codes.txt"
if [ "$old" != "$new" ] && [ "$old_code" != 0 ] && [ "$rev_code" != 0 ] && [ "$correct_code" = 0 ]; then
  record case6-fences PASS "catalog changed; stale catalog and stale enrollment revision refused; current fence accepted"
else
  record case6-fences FAIL "old=$old new=$new exit codes=$old_code/$rev_code/$correct_code"
fi
sleep 8
fleet_coordinator_cli backlog workers --json >"$ROOT/evidence/case6-after.json"
fleet_turn_script qual-beside 1 <<EOF
#!/bin/sh
printf 'one dispatch after drift\n' > result.txt
EOF
dir=$(fleet_executable_campaign case6-execution beside)
out="$ROOT/evidence/case6-submit.json"
if fleet_submit_local "$dir" qual-drift-execution "$out"; then
  run=$(run_of "$out")
  if state=$(await_task "$run" 180 succeeded); then
    show_run "$run" "$ROOT/evidence/case6-run.json" >/dev/null
    record case6-execution PASS "$run $state"
  else
    show_run "$run" "$ROOT/evidence/case6-run.json" >/dev/null
    record case6-execution FAIL "$run $state"
  fi
else
  record case6-execution FAIL "submission failed"
fi

python3 "$HARNESS_DIR/fleet_limits.py" "$ROOT" "${UPKEEPER_SOURCE:-/home/igor/Work/wt-b-fleet}" || FAILURES=$((FAILURES + 1))
exit "$((FAILURES > 0))"
