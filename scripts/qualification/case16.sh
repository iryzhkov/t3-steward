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

# case_wake_all proves that an all set does not wake until every wait settles.
case_wake_all() {
  local signal=wake-all second=wake-all-2 project=qual-wake-all
  park_two_waits "$project" "$signal" "$second" all
  local directory submit run
  directory=$(fleet_executable_campaign wakeall wake-all)
  submit=$(evidence_path case16-all-submit.json)
  if ! fleet_submit_local "$directory" "qual-case16-all-$$" "$submit"; then
    record case16-wake-all FAIL "submission failed: $(tail -n 3 "$submit.err")"
    return
  fi
  run=$(run_of "$submit")
  local state
  if ! state=$(await_task "$run" 300 waiting-external); then
    record case16-wake-all FAIL "the attempt never parked: \"$state\""
    return
  fi
  # Settle only the all wait. The attempt must stay parked: the other wait of
  # the set is still outstanding.
  : >"$ROOT/signals/$second"
  local held deadline=$((SECONDS + 120))
  while [ "$SECONDS" -lt "$deadline" ]; do
    held=$(fleet_coordinator_cli backlog show "$run" --json 2>/dev/null | reading task-state)
    case "$held" in
      "waiting-external "*) ;;
      *)
        record case16-wake-all FAIL "an all wait woke the attempt before the rest of its set settled: \"$held\""
        return
        ;;
    esac
    sleep 5
  done
  # Now settle the remaining wait; the attempt must resume.
  : >"$ROOT/signals/$signal"
  local woken
  if ! woken=$(await_task "$run" 300 succeeded failed); then
    record case16-wake-all FAIL "the attempt did not resume after every wait settled: \"$woken\""
    return
  fi
  record case16-wake-all PASS "an all wait held the park for 120s while its set was incomplete, then the attempt resumed and $woken"
}
