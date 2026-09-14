#!/usr/bin/env bash
# Case 11: a task-bound wait parks an attempt, and nothing happens to it until
# the condition settles.
#
# This is the case the whole effort exists for, so each claim is asserted
# separately and each failure says which claim failed. The scripted turn is the
# agent: turn one registers the wait and stops without writing anything, turn
# two writes the declared output after the steward's own wake message starts it.

set -euo pipefail

# turn_starts counts thread.turn.start commands the synthetic provider received
# for one thread. It is how "the same thread resumed" is evidenced rather than
# assumed.
turn_starts() {
  local thread=$1
  python3 -c 'import json,sys
thread = sys.argv[1]
starts = 0
for line in sys.stdin:
    try:
        entry = json.loads(line)
    except Exception:
        continue
    command = entry.get("command") or {}
    if command.get("type") == "thread.turn.start" and command.get("threadId") == thread:
        starts += 1
print(starts)' "$thread" <"$ROOT/evidence/t3-stub.jsonl"
}

case_eleven() {
  local signal=case11 project=qual-park
  park_turn_scripts "$project" "$signal"
  rm -f "$ROOT/signals/$signal"

  local directory submit run
  directory=$(fleet_executable_campaign park park)
  submit=$(evidence_path case11-submit.json)
  if ! fleet_submit_local "$directory" "qual-case11-$$" "$submit"; then
    record case11-park FAIL "submission failed: $(tail -n 3 "$submit.err")"
    return
  fi
  run=$(run_of "$submit")

  # 1. The attempt reaches waiting-external.
  local state
  if ! state=$(await_task "$run" 240 waiting-external); then
    show_run "$run" "$(evidence_path case11-parked.json)" >/dev/null
    record case11-park FAIL "attempt never parked; state is \"$state\" (see $(evidence_path case11-parked.json), $ROOT/evidence/$signal-register.txt)"
    return
  fi
  local parked control
  parked=$(show_run "$run" "$(evidence_path case11-parked.json)")
  control=$(printf '%s' "$state" | cut -d' ' -f2)
  if [ "$control" != waiting-external ]; then
    record case11-park FAIL "progress is waiting-external but control is \"$control\""
    return
  fi
  # The parked attempt does not report its thread, so the thread is read from
  # the provider journal: it is the thread whose first turn ran this scenario's
  # park script.
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
  record case11-park PASS "attempt parked: $state thread=$thread"

  # The execution identity the product handed the task is judged separately
  # from the lifecycle it enables. The turn script recorded both revisions; if
  # they differ, the fence the product would have sent was stale and the
  # unmodified path could not have parked anything.
  local revisions
  revisions=$(cat "$ROOT/evidence/$signal-revision.txt" 2>/dev/null || echo 'missing')
  case "$revisions" in
    *"workspace="*)
      if [ "${revisions#*workspace=}" = "${revisions#*coordinator=}" ]; then
        record case11-identity-revision PASS "the workspace identity named the current attempt revision ($revisions)"
      else
        record case11-identity-revision FAIL "the workspace execution identity is stale: $revisions; the unmodified path is refused with \"$(head -n 1 "$ROOT/evidence/$signal-register-unpatched.txt" 2>/dev/null || echo 'stale attempt revision')\""
      fi
      ;;
    *) record case11-identity-revision FAIL "the turn recorded no revision comparison" ;;
  esac

  # 2. Nothing is collected and nothing is verified while it waits.
  local outputs verification runstate
  outputs=$(reading outputs <"$parked")
  verification=$(reading verification <"$parked")
  runstate=$(reading run-state <"$parked")
  if [ "$outputs" != 0 ] || [ "$verification" != 0 ]; then
    record case11-quiet FAIL "a parked attempt already has $outputs output(s) and $verification verification(s)"
  elif [ "${runstate%%/*}" = succeeded ] || [ "${runstate%%/*}" = failed ]; then
    record case11-quiet FAIL "the campaign is already terminal ($runstate) while its only task is parked"
  else
    record case11-quiet PASS "parked: 0 outputs, 0 verifications, campaign $runstate"
  fi

  # 3. Executor and provider capacity is released.
  #
  # The quota pool admits one assignment at a time. A second campaign that runs
  # to completion while the first is parked can only have been given the slot
  # the parked attempt would otherwise still hold.
  fleet_turn_script qual-beside 1 <<EOF
#!/bin/sh
$(fleet_task_env)
printf 'ran beside a parked task\n' > result.txt
echo "wrote result.txt"
EOF
  local neighbour neighbourOut neighbourRun neighbourState
  # The neighbour is pinned to the other worker, so what it demonstrates is the
  # quota pool slot rather than the parked worker's own executor: the pool
  # admits one assignment at a time across the fleet.
  neighbour=$(fleet_executable_campaign beside beside)
  neighbourOut=$(evidence_path case11-neighbour.json)
  if ! fleet_submit_local "$neighbour" "qual-case11-beside-$$" "$neighbourOut"; then
    record case11-capacity FAIL "the neighbour campaign could not be submitted: $(tail -n 3 "$neighbourOut.err")"
  else
    neighbourRun=$(run_of "$neighbourOut")
    if ! neighbourState=$(await_task "$neighbourRun" 240 succeeded); then
      local neighbourTask
      neighbourTask=$(fleet_coordinator_cli backlog show "$neighbourRun" --json 2>/dev/null \
        | reading task-field taskId)
      fleet_coordinator_cli backlog explain "$neighbourRun/$neighbourTask" \
        >"$(evidence_path case11-capacity-explain.txt)" 2>&1 || true
      fleet_coordinator_cli backlog quota --json \
        >"$(evidence_path case11-capacity-quota.json)" 2>&1 || true
      fleet_coordinator_cli backlog reservations --json \
        >"$(evidence_path case11-capacity-reservations.json)" 2>&1 || true
      record case11-capacity FAIL "the parked attempt still holds its provider slot: a second campaign on the other worker stayed \"$neighbourState\" for the whole park, on a pool whose limit is one. The coordinator's own explanation says the task is eligible, so the refusal is pool concurrency rather than placement. Evidence: $(evidence_path case11-capacity-explain.txt)"
    else
      record case11-capacity PASS "a second campaign ran to $neighbourState on the one-slot pool while the first stayed parked"
    fi
  fi

  # The parked attempt must still be parked after all that.
  local still
  still=$(fleet_coordinator_cli backlog show "$run" --json 2>/dev/null | reading task-state)
  case "$still" in
    "waiting-external "*) ;;
    *) record case11-quiet FAIL "the parked attempt changed to \"$still\" while nothing had settled" ;;
  esac

  # 4. The condition settles; the same thread resumes and the work completes.
  : >"$ROOT/signals/$signal"
  local woken
  if ! woken=$(await_task "$run" 300 succeeded failed); then
    show_run "$run" "$(evidence_path case11-woken.json)" >/dev/null
    record case11-wake FAIL "the parked attempt never settled; state is \"$woken\" (see $(evidence_path case11-woken.json))"
    return
  fi
  local final finalThread starts
  final=$(show_run "$run" "$(evidence_path case11-woken.json)")
  finalThread=$(reading task-field threadId <"$final")
  starts=$(turn_starts "$thread")
  outputs=$(reading outputs <"$final")
  verification=$(reading verification <"$final")
  runstate=$(reading run-state <"$final")

  if [ "$finalThread" != "$thread" ]; then
    record case11-wake FAIL "the attempt resumed on thread $finalThread, not the parked thread $thread"
    return
  fi
  if [ "$starts" != 2 ]; then
    record case11-wake FAIL "thread $thread received $starts turn starts, expected exactly 2 (one dispatch, one wake)"
    return
  fi
  record case11-wake PASS "same thread $thread resumed: $starts turn starts, task $woken"

  if [ "${woken%% *}" != succeeded ]; then
    record case11-complete FAIL "the woken task ended $woken; failure=$(reading task-field failure <"$final")"
    return
  fi
  if [ "$outputs" != 1 ]; then
    record case11-complete FAIL "the woken task collected $outputs output(s), expected 1"
    return
  fi
  if [ "$verification" != 1 ]; then
    record case11-complete FAIL "verification ran $verification time(s), expected exactly 1"
    return
  fi
  # The campaign settles one cycle after its last task, so it is waited for
  # rather than read at the moment the task finished.
  local settled
  if ! settled=$(await_run "$run" 120); then
    record case11-complete FAIL "the task succeeded but the campaign stayed $settled (last read $runstate)"
    return
  fi
  record case11-complete PASS "task $woken, campaign $settled, $outputs output collected, verification ran $verification time"
}
