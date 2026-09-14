#!/usr/bin/env bash
# The wait-aware lifecycle, recovery and intake cases.
#
# These need a task that really executes: the coordinator plans and dispatches
# an assignment, the worker prepares a workspace and creates a thread, and the
# synthetic provider runs a scripted turn in that workspace. The script stands
# in for the agent; everything it calls is the real product.

set -euo pipefail

# reading runs one named reading of a coordinator JSON document.
reading() {
  python3 "$HARNESS_DIR/inspect.py" "$@"
}

# fleet_submit_local submits over the coordinator's own socket. These cases are
# about the lifecycle rather than the remote carrier, and the remote carrier
# cannot run the readiness check at all (see the remote-viability case).
fleet_submit_local() {
  local directory=$1 key=$2 out=$3
  fleet_coordinator_cli campaign submit "$directory" --idempotency-key "$key" --json \
    >"$out" 2>"$out.err"
}

# run_of reads the run id out of a submission answer.
run_of() { json_after_preamble runId <"$1"; }

# show_run writes the run detail to an evidence file and echoes its path.
show_run() {
  local run=$1 path=$2
  fleet_coordinator_cli backlog show "$run" --json >"$path" 2>/dev/null || true
  printf '%s' "$path"
}

# await_run waits for the campaign itself to settle, which happens a cycle after
# its last task does.
await_run() {
  local run=$1 seconds=$2
  local deadline=$((SECONDS + seconds)) state
  while :; do
    state=$(fleet_coordinator_cli backlog show "$run" --json 2>/dev/null | reading run-state)
    case "$state" in
      succeeded/*|*/succeeded|failed/*|*/failed) printf '%s' "$state"; return 0 ;;
    esac
    if [ "$SECONDS" -ge "$deadline" ]; then
      printf '%s' "$state"
      return 1
    fi
    sleep 1
  done
}

# await_task waits until the task's progress matches one of the given states.
# It returns 1 on timeout, and the caller decides what that means: this helper
# never treats "never arrived" as the expected state.
await_task() {
  local run=$1 seconds=$2
  shift 2
  local deadline=$((SECONDS + seconds)) state wanted
  while :; do
    state=$(fleet_coordinator_cli backlog show "$run" --json 2>/dev/null | reading task-state)
    for wanted in "$@"; do
      case "$state" in
        "$wanted "*) printf '%s' "$state"; return 0 ;;
      esac
    done
    if [ "$SECONDS" -ge "$deadline" ]; then
      printf '%s' "$state"
      return 1
    fi
    sleep 1
  done
}

# park_turn_scripts installs the two scripted turns of a parking scenario: the
# first registers a task-bound wait and stops without writing anything, the
# second writes the declared output after the wake.
park_turn_scripts() {
  local project=$1 signal=$2
  fleet_turn_script "$project" 1 <<EOF
#!/bin/sh
$(fleet_task_env)
export STEWARD COORDINATOR_CONFIG SIGNALS EVIDENCE
export QUAL_SIGNAL=$signal
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

# ---------------------------------------------------------------------------
# Case 8: a legacy intake file that can never be accepted.
#
# The drop directory is read-only to the coordinator: it re-reads it on every
# cycle and never drains it. A file naming a project no alias maps must
# therefore be reported once and skipped silently afterwards, across restarts,
# and the quarantine view must show it with its reason.
# ---------------------------------------------------------------------------
case_eight() {
  local drop="$ROOT/coordinator/drop/qualification-quarantine.md"
  cat >"$drop" <<'EOF'
---
project: no-such-project-anywhere
title: qualification quarantine fixture
importance: 3
difficulty: 3
max_turns: 2
enabled: true
---
This file names a project the coordinator has no alias for, so it can never be
accepted. It exists to be quarantined.
EOF
  # Give the running coordinator a few cycles, then restart it twice. A report
  # that repeats is the failure this case looks for.
  sleep 8
  fleet_restart_coordinator clean
  sleep 8
  fleet_restart_coordinator clean
  sleep 8

  local reports
  reports=$(cat "$ROOT/evidence/coordinator"*.log 2>/dev/null \
    | grep -c 'no-such-project-anywhere' || true)
  local view
  view=$(evidence_path case8-quarantine.json)
  fleet_coordinator_cli backlog quarantine --json >"$view" 2>"$view.err" || true
  local rows
  rows=$(reading quarantine <"$view")
  if [ -z "$rows" ] || [ "$rows" = unreadable ]; then
    record case8 FAIL "the quarantine view showed nothing: $(head -c 200 "$view$([ -s "$view" ] || echo .err)" | tr '\n' ' ')"
    return
  fi
  local count
  count=$(printf '%s\n' "$rows" | grep -c . || true)
  if [ "${count:-0}" != 1 ]; then
    record case8 FAIL "the quarantine view lists $count entries, expected exactly 1: $rows"
    return
  fi
  if [ "${reports:-0}" -gt 3 ]; then
    record case8 FAIL "the refusal was logged $reports times across three coordinator lifetimes, which is repeated reporting"
    return
  fi
  record case8 PASS "one durable quarantine record across three coordinator lifetimes, $reports log line(s): $rows"
}

# ---------------------------------------------------------------------------
# execution-path: the base every lifecycle case stands on.
#
# It is a case in its own right because a lifecycle assertion made on a fleet
# that never dispatched anything would pass for the wrong reason.
# ---------------------------------------------------------------------------
case_execution_path() {
  fleet_turn_script qual-plain 1 <<EOF
#!/bin/sh
$(fleet_task_env)
printf 'plain synthetic turn\n' > result.txt
echo "wrote result.txt"
EOF
  local directory out run state
  directory=$(fleet_executable_campaign plain plain)
  out=$(evidence_path execution-path-submit.json)
  if ! fleet_submit_local "$directory" "qual-plain-$$" "$out"; then
    record execution-path FAIL "submission failed: $(tail -n 3 "$out.err")"
    return 1
  fi
  run=$(run_of "$out")
  EXECUTION_RUN=$run
  export EXECUTION_RUN
  if ! state=$(await_task "$run" 180 succeeded); then
    show_run "$run" "$(evidence_path execution-path-run.json)" >/dev/null
    record execution-path FAIL "run $run task state is \"$state\" after 180s; see $(evidence_path execution-path-run.json) and the coordinator log"
    return 1
  fi
  local detail artifacts
  detail=$(show_run "$run" "$(evidence_path execution-path-run.json)")
  artifacts=$(reading artifacts <"$detail")
  record execution-path PASS "run=$run task=$state artifacts=[$artifacts]"
  return 0
}
