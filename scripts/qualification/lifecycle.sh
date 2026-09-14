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
