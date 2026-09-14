#!/usr/bin/env bash
# Two properties the unit tests cannot reach, attacked rather than demonstrated.
#
# 1. A wait request id names one park. Re-registering it after the wait settled
#    must refuse, not print the end-your-turn message.
# 2. Nothing may collect outputs or run verification for a parked attempt
#    through any path.

set -euo pipefail

# ---------------------------------------------------------------------------
# attack-replayed-request-id
#
# Turn one parks under a fixed request id. The wake starts turn two, which
# re-registers the same request id. The attempt is running again by then, so the
# registration must be refused. A success there would tell the agent to end a
# turn nothing will resume, and the worker would then collect and verify a
# workspace with no outputs in it.
# ---------------------------------------------------------------------------
case_attack_replay() {
  local signal=replay project=qual-race request=qual-replayed-request
  rm -f "$ROOT/signals/$signal"
  local turn
  for turn in 1 2; do
    fleet_turn_script "$project" "$turn" <<EOF
#!/bin/sh
$(fleet_task_env)
export STEWARD COORDINATOR_CONFIG SIGNALS EVIDENCE
export QUAL_SIGNAL=$signal-$turn
export QUAL_REQUEST_ID=$request
export QUAL_TURN=$turn
export QUAL_INSPECT=$HARNESS_DIR/inspect.py
exec /bin/sh $HARNESS_DIR/turn_replay.sh
EOF
  done
  local directory submit run
  directory=$(fleet_executable_campaign replay race)
  submit=$(evidence_path attack-replay-submit.json)
  if ! fleet_submit_local "$directory" "qual-attack-replay-$$" "$submit"; then
    record attack-replayed-request-id FAIL "submission failed: $(tail -n 3 "$submit.err")"
    return
  fi
  run=$(run_of "$submit")
  local state
  if ! state=$(await_task "$run" 240 waiting-external); then
    record attack-replayed-request-id FAIL "the first turn never parked: state \"$state\"; see $ROOT/evidence/$signal-1-register.txt"
    return
  fi
  # Settle the first wait so the wake starts turn two.
  : >"$ROOT/signals/$signal-1"
  local settled
  if ! settled=$(await_task "$run" 300 succeeded failed waiting-external); then
    record attack-replayed-request-id FAIL "the attempt never left its park: \"$settled\""
    return
  fi
  # The resumed turn can only replay the request id if it can still name itself.
  # Whether the worker-written identity record survived the park is judged
  # separately, because it is a different claim with a different owner.
  local workspace="$ROOT/evidence/$signal-2-workspace.txt"
  local second="$ROOT/evidence/$signal-2-register.txt"
  local deadline=$((SECONDS + 180))
  until [ -s "$second" ] || [ -s "$workspace" ]; do
    if [ "$SECONDS" -ge "$deadline" ]; then
      record identity-survives-the-park FAIL 'the resumed turn never ran'
      record attack-replayed-request-id FAIL 'the resumed turn never re-registered the request id'
      return
    fi
    sleep 2
  done
  if grep -q 'task.env' "$workspace" 2>/dev/null; then
    record identity-survives-the-park PASS 'the worker-written execution identity was still in the workspace for the resumed turn'
  else
    record identity-survives-the-park FAIL "the execution identity is gone from the workspace the resumed turn ran in, so a woken task cannot name itself; workspace listing in $workspace"
    record attack-replayed-request-id FAIL 'not reached: the resumed turn could not name its own attempt, so no request id could be replayed'
    return
  fi
  deadline=$((SECONDS + 120))
  until [ -s "$second" ]; do
    if [ "$SECONDS" -ge "$deadline" ]; then
      record attack-replayed-request-id FAIL 'the resumed turn never re-registered the request id'
      return
    fi
    sleep 2
  done
  if grep -qiE 'already settled|not parked|refus|error' "$second"; then
    record attack-replayed-request-id PASS "the replayed request id was refused: $(head -c 200 "$second" | tr '\n' ' ')"
    return
  fi
  if grep -qi 'end this turn' "$second"; then
    record attack-replayed-request-id FAIL "a settled request id was replayed and the agent was told to end its turn: $(head -c 200 "$second" | tr '\n' ' ')"
    return
  fi
  record attack-replayed-request-id FAIL "the replay produced neither a refusal nor a park: $(head -c 200 "$second" | tr '\n' ' ')"
}

# ---------------------------------------------------------------------------
# Case 14: registering a task-bound wait after the attempt is terminal.
#
# The identity of a finished attempt is used deliberately, which is what an
# agent holding a stale workspace would do. The refusal must be explicit, and
# nothing may be left holding the thread afterwards.
# ---------------------------------------------------------------------------
case_fourteen() {
  if [ -z "${EXECUTION_RUN:-}" ]; then
    record case14 FAIL 'no finished run to borrow a terminal attempt from'
    return
  fi
  local detail state attempt task thread revision assignment
  detail=$(show_run "$EXECUTION_RUN" "$(evidence_path case14-source.json)")
  state=$(reading task-state <"$detail")
  case "$state" in
    succeeded*|failed*) ;;
    *) record case14 FAIL "the borrowed attempt is not terminal: $state" ; return ;;
  esac
  attempt=$(reading task-field id <"$detail")
  task=$(reading task-field taskId <"$detail")
  thread=$(reading task-field threadId <"$detail")
  revision=$(printf '%s' "$state" | cut -d' ' -f3)
  assignment=$(reading task-field assignmentId <"$detail")

  local out status=0
  out=$(evidence_path case14-register.txt)
  env -i \
    PATH="$ROOT/bin:/usr/bin:/bin" \
    HOME="$ROOT/worker-a/home" \
    XDG_CONFIG_HOME="$ROOT/worker-a/home/.config" \
    XDG_STATE_HOME="$ROOT/worker-a/home/.local/state" \
    T3_STEWARD_WORKFLOW_RUN_ID="$EXECUTION_RUN" \
    T3_STEWARD_TASK_ID="$task" \
    T3_STEWARD_ATTEMPT_ID="$attempt" \
    T3_STEWARD_ATTEMPT_REVISION="$revision" \
    T3_STEWARD_ASSIGNMENT_ID="$assignment" \
    T3_STEWARD_THREAD_ID="$thread" \
    "$STEWARD" wait add --config "$ROOT/coordinator/config.yaml" --task current \
      --name case14 --every 30s --max-every 30s --timeout 10m \
      -- test -f "$ROOT/signals/case14" >"$out" 2>&1 || status=$?
  if [ "$status" -eq 0 ]; then
    record case14 FAIL "a task-bound wait was accepted for terminal attempt $attempt"
    return
  fi
  if ! grep -qiE 'terminal|refus' "$out"; then
    record case14 FAIL "the refusal did not name the terminal attempt: $(head -c 200 "$out" | tr '\n' ' ')"
    return
  fi
  # No orphan authority: the attempt must be untouched and no local check may
  # have been left polling for a park that does not exist.
  local after waits
  after=$(fleet_coordinator_cli backlog show "$EXECUTION_RUN" --json 2>/dev/null | reading task-state)
  waits=$(fleet_coordinator_cli wait list 2>/dev/null | grep -c case14 || true)
  if [ "$after" != "$state" ]; then
    record case14 FAIL "the refused registration changed the attempt from \"$state\" to \"$after\""
    return
  fi
  if [ "${waits:-0}" != 0 ]; then
    record case14 FAIL "the refused registration left $waits local wait(s) behind"
    return
  fi
  record case14 PASS "refused with exit $status: $(head -c 160 "$out" | tr '\n' ' '); attempt still $after, no local wait left"
}
