#!/usr/bin/env bash
# Wake semantics: two waits on one attempt, and the difference between each and
# all.
#
# These are only possible now that the fence is a liveness check: the first
# registration advances the attempt revision, so under the old equality fence a
# second wait could never be registered at all.

set -euo pipefail

# park_two_waits installs a first turn that registers two waits on the same
# attempt, the second with the given wake mode.
park_two_waits() {
  local project=$1 signal=$2 second=$3 wake=$4
  rm -f "$ROOT/signals/$signal" "$ROOT/signals/$second"
  fleet_turn_script "$project" 1 <<EOF
#!/bin/sh
$(fleet_task_env)
export STEWARD COORDINATOR_CONFIG SIGNALS EVIDENCE
export QUAL_SIGNAL=$signal
export QUAL_SECOND_WAIT=$second
export QUAL_WAKE=$wake
export QUAL_FIRST_WAKE=$wake
export QUAL_INSPECT=$HARNESS_DIR/inspect.py
exec /bin/sh $HARNESS_DIR/turn_park.sh
EOF
  fleet_turn_script "$project" 2 <<EOF
#!/bin/sh
$(fleet_task_env)
printf 'written after the wake\n' > result.txt
echo "wrote result.txt after the wake"
EOF
}

# case_multi_wait proves that a second wait can be registered at all, and that
# with the default each mode settling one of the two wakes the attempt.
case_multi_wait() {
  local signal=wake-each second=wake-each-2 project=qual-wake-each
  park_two_waits "$project" "$signal" "$second" each
  local directory submit run
  directory=$(fleet_executable_campaign wakeeach wake-each)
  submit=$(evidence_path case16-each-submit.json)
  if ! fleet_submit_local "$directory" "qual-case16-each-$$" "$submit"; then
    record case16-multi-wait FAIL "submission failed: $(tail -n 3 "$submit.err")"
    return
  fi
  run=$(run_of "$submit")
  local state
  if ! state=$(await_task "$run" 300 waiting-external); then
    record case16-multi-wait FAIL "the attempt never parked: \"$state\"; see $ROOT/evidence/$signal-register.txt"
    return
  fi
  local secondOut="$ROOT/evidence/$signal-register-second.txt"
  local deadline=$((SECONDS + 60))
  until [ -s "$secondOut" ]; do
    [ "$SECONDS" -lt "$deadline" ] && sleep 2 && continue
    record case16-multi-wait FAIL 'the turn never attempted a second registration'
    return
  done
  if ! grep -q 'exit=0' "$secondOut"; then
    record case16-multi-wait FAIL "the second wait on the same attempt was refused: $(head -c 220 "$secondOut" | tr '\n' ' ')"
    return
  fi
  record case16-multi-wait PASS "two waits registered on one attempt; the second exited 0 while the first was live"

  # each: settling one of the two must wake the attempt.
  : >"$ROOT/signals/$signal"
  local woken
  if ! woken=$(await_task "$run" 300 succeeded failed); then
    record case16-wake-each FAIL "settling one of two each waits did not wake the attempt: \"$woken\""
    return
  fi
  if [ "${woken%% *}" != succeeded ]; then
    record case16-wake-each FAIL "the woken attempt ended \"$woken\""
    return
  fi
  record case16-wake-each PASS "one of two each waits settled and the attempt resumed and $woken"
}

# Native coordinator records prove settlement; elapsed time alone cannot.
all_wait_phase() {
  local run=$1 phase=$2 evidence=$3
  # The CLI native list currently exposes node waits only. Read coordinator
  # task-wait records in a read-only SQLite snapshot for this assertion.
  python3 - "$ROOT/coordinator/state.db" "$evidence" <<'PY'
import json, sqlite3, sys
with sqlite3.connect("file:" + sys.argv[1] + "?mode=ro", uri=True) as db:
    rows = [json.loads(row[0]) for row in db.execute("SELECT record FROM coordinator_task_waits")]
with open(sys.argv[2], "w") as output:
    json.dump({"taskWaits": rows}, output)
PY
  python3 - "$run" "$phase" "$evidence" <<'PY'
import json, sys
rows = [w for w in json.load(open(sys.argv[3])).get("taskWaits", [])
        if w.get("workflowRunId") == sys.argv[1]]
assert len(rows) == 2 and all(w["wake"] == "all" for w in rows), rows
first = next(w for w in rows if w["name"] == "wake-all")
second = next(w for w in rows if w["name"] == "wake-all-second")
if sys.argv[2] == "registered":
    assert all(not w.get("settledAt") for w in rows), rows
elif sys.argv[2] == "held":
    assert (first.get("result") or {}).get("outcome") == "met", rows
    assert not second.get("settledAt"), rows
    assert all(not w.get("wokenAt") for w in rows), rows
else:
    assert all((w.get("result") or {}).get("outcome") == "met" for w in rows), rows
    assert all(w.get("delivery") == "delivered" for w in rows), rows
assert len({w["threadId"] for w in rows}) == 1, rows
print(first["threadId"])
PY
}

case_wake_all() {
  local signal=wake-all second=wake-all-2 project=qual-wake-all
  park_two_waits "$project" "$signal" "$second" all
  local directory submit run state thread
  directory=$(fleet_executable_campaign wakeall wake-all)
  submit=$(evidence_path case16-all-submit.json)
  if ! fleet_submit_local "$directory" "qual-case16-all-$$" "$submit"; then
    record case16-wake-all FAIL "submission failed"
    return
  fi
  run=$(run_of "$submit")
  if ! state=$(await_task "$run" 300 waiting-external); then
    record case16-wake-all FAIL "the attempt never parked: $state"
    return
  fi
  local deadline=$((SECONDS + 60))
  until thread=$(all_wait_phase "$run" registered "$(evidence_path case16-all-registered.json)" 2>/dev/null); do
    if [ "$SECONDS" -ge "$deadline" ]; then
      record case16-wake-all FAIL "two live all waits were not registered"
      return
    fi
    sleep 1
  done
  : >"$ROOT/signals/$signal"
  deadline=$((SECONDS + 90))
  until all_wait_phase "$run" held "$(evidence_path case16-all-held.json)" >/dev/null 2>&1; do
    if [ "$SECONDS" -ge "$deadline" ]; then
      record case16-wake-all FAIL "first all wait did not settle while second remained pending"
      return
    fi
    sleep 1
  done
  # Observe another coordinator cycle after confirmed settlement.
  sleep 5
  local held
  held=$(show_run "$run" "$(evidence_path case16-all-held-run.json)")
  if [ "$(reading task-state <"$held" | cut -d' ' -f1)" != waiting-external ] ||
     [ "$(turn_starts "$thread")" != 1 ] ||
     [ "$(reading outputs <"$held")" != 0 ] ||
     [ "$(reading verification <"$held")" != 0 ]; then
    record case16-wake-all FAIL "confirmed first settlement resumed or collected before remaining wait"
    return
  fi
  : >"$ROOT/signals/$second"
  if ! state=$(await_task "$run" 300 succeeded failed) || [ "${state%% *}" != succeeded ]; then
    record case16-wake-all FAIL "all-set resumption did not succeed: $state"
    return
  fi
  local final finalThread
  final=$(show_run "$run" "$(evidence_path case16-all-final.json)")
  finalThread=$(reading task-field threadId <"$final")
  if ! all_wait_phase "$run" delivered "$(evidence_path case16-all-delivered.json)" >/dev/null ||
     [ "$finalThread" != "$thread" ] || [ "$(turn_starts "$thread")" != 2 ]; then
    record case16-wake-all FAIL "waits not delivered once to the original thread"
    return
  fi
  record case16-wake-all PASS "two pure all waits: first demonstrably met while second pending and no collection; remaining met; same thread $thread resumed exactly once and succeeded"
}
