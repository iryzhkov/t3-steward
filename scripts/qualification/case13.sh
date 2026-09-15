#!/usr/bin/env bash
# Case 13: restart the coordinator and the worker while a task is waiting.
#
# A park is durable state on both sides, and a restart is where duplicate wakes
# come from. Exactly one wake, one resumed turn and one verification must
# survive both processes being replaced.

set -euo pipefail

# Case 7 lives here too: it needs the same disposable fleet and nothing else.

case_thirteen() {
  local signal=case13 project=qual-park-restart
  park_turn_scripts "$project" "$signal"
  rm -f "$ROOT/signals/$signal"

  local directory submit run
  directory=$(fleet_executable_campaign parkrestart park-restart)
  submit=$(evidence_path case13-submit.json)
  if ! fleet_submit_local "$directory" "qual-case13-$$" "$submit"; then
    record case13 FAIL "submission failed: $(tail -n 3 "$submit.err")"
    return
  fi
  run=$(run_of "$submit")
  local state
  if ! state=$(await_task "$run" 300 waiting-external); then
    record case13 FAIL "the task never parked: \"$state\"; see $ROOT/evidence/$signal-register.txt"
    return
  fi
  local thread
  thread=$(python3 -c 'import json,sys
needle = sys.argv[1]
for line in sys.stdin:
    try:
        entry = json.loads(line)
    except Exception:
        continue
    if entry.get("event") == "turn-script" and needle in (entry.get("script") or ""):
        print(entry.get("thread") or "")
        raise SystemExit
print("")' "$project/turn1.sh" <"$ROOT/evidence/t3-stub.jsonl")

  # Both processes are replaced while the attempt is parked.
  fleet_restart_worker worker-b
  fleet_restart_coordinator clean
  sleep 5
  local afterRestart
  afterRestart=$(fleet_coordinator_cli backlog show "$run" --json 2>/dev/null | reading task-state)
  case "$afterRestart" in
    "waiting-external "*) ;;
    *)
      record case13 FAIL "the park did not survive the restarts: \"$afterRestart\""
      return
      ;;
  esac

  : >"$ROOT/signals/$signal"
  local woken
  if ! woken=$(await_task "$run" 420 succeeded failed); then
    show_run "$run" "$(evidence_path case13-woken.json)" >/dev/null
    record case13 FAIL "after the restarts the parked attempt never settled: \"$woken\""
    return
  fi
  local final starts verification outputs
  final=$(show_run "$run" "$(evidence_path case13-woken.json)")
  starts=$(turn_starts "$thread")
  verification=$(reading verification <"$final")
  outputs=$(reading outputs <"$final")
  if [ "$starts" != 2 ]; then
    record case13 FAIL "thread $thread received $starts turn starts across the restarts, expected exactly 2"
    return
  fi
  if [ "$verification" != 1 ]; then
    record case13 FAIL "verification ran $verification time(s) across the restarts, expected exactly 1"
    return
  fi
  if [ "${woken%% *}" != succeeded ] || [ "$outputs" != 1 ]; then
    record case13 FAIL "after the restarts the task ended \"$woken\" with $outputs output(s)"
    return
  fi
  record case13 PASS "park survived a worker and a coordinator restart; one wake, $starts turn starts, $verification verification, task $woken"
}

# ---------------------------------------------------------------------------
# Case 7: three preparation failures.
#
# Each attempt at preparing the workspace must keep its own immutable log, and
# the terminal reason must quote the first causal failure rather than only the
# last one.
# ---------------------------------------------------------------------------
case_seven() {
  local directory submit run
  directory=$(fleet_executable_campaign badsetup bad-setup)
  submit=$(evidence_path case7-submit.json)
  if ! fleet_submit_local "$directory" "qual-case7-$$" "$submit"; then
    record case7 FAIL "submission failed: $(tail -n 3 "$submit.err")"
    return
  fi
  run=$(run_of "$submit")
  local state
  if ! state=$(await_task "$run" 420 failed); then
    record case7 FAIL "the task with a failing setup profile did not fail: \"$state\""
    return
  fi
  local final failure logs
  final=$(show_run "$run" "$(evidence_path case7-run.json)")
  failure=$(reading task-field failure <"$final")
  logs=$(find "$ROOT/worker-a" -name '*.preparation.*.log' 2>/dev/null | sort)
  printf '%s\n' "$logs" >"$(evidence_path case7-preparation-logs.txt)"
  local count
  count=$(printf '%s\n' "$logs" | grep -c . || true)
  if [ "${count:-0}" -lt 3 ]; then
    record case7 FAIL "found $count preparation log(s), expected three distinct ones: $logs"
    return
  fi
  local distinct
  distinct=$(printf '%s\n' "$logs" | sed 's/.*\.preparation\.//' | sort -u | grep -c . || true)
  if [ "${distinct:-0}" -lt 3 ]; then
    record case7 FAIL "the preparation logs are not distinct ordinals: $logs"
    return
  fi
  case "$failure" in
    *"first error"*)
      record case7 PASS "$count immutable preparation logs, terminal reason keeps the first cause: $(printf '%s' "$failure" | head -c 200)"
      ;;
    *)
      record case7 FAIL "the terminal reason does not quote the first causal failure: $(printf '%s' "$failure" | head -c 200)"
      ;;
  esac
}
