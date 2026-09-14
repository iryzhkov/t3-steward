#!/usr/bin/env bash
# Case 12: race wait registration against agent-turn completion.
#
# The attempt must never be both waiting and terminal. The race is biased both
# ways and run enough times to mean something: the report says how many
# iterations ran and how many of each ordering were actually observed, because
# an iteration that did not reach the ordering it intended proves nothing about
# it.

set -euo pipefail

# QUAL_RACE_ITERATIONS is per bias, so the default is six runs in total. Each is
# a real campaign, so raising it costs about a minute an iteration.
QUAL_RACE_ITERATIONS=${QUAL_RACE_ITERATIONS:-3}

# race_iteration runs one campaign under one bias and returns its observation as
# "<bias> <registration> <final-state>".
race_iteration() {
  local bias=$1 index=$2
  local signal="race-$bias-$index" project=qual-race delay=0
  # The completion-first bias delays the registration past the point where the
  # worker has reconciled the finished turn, which is the ordering that has to
  # be reached rather than assumed.
  if [ "$bias" = completion-first ]; then
    delay=8
  fi
  rm -f "$ROOT/signals/$signal"
  fleet_turn_script "$project" 1 <<EOF
#!/bin/sh
$(fleet_task_env)
export STEWARD COORDINATOR_CONFIG SIGNALS EVIDENCE
export QUAL_SIGNAL=$signal
export QUAL_BIAS=$bias
export QUAL_DELAY=$delay
export QUAL_INSPECT=$HARNESS_DIR/inspect.py
exec /bin/sh $HARNESS_DIR/turn_race.sh
EOF
  fleet_turn_script "$project" 2 <<EOF
#!/bin/sh
$(fleet_task_env)
printf 'written after the wake\n' > result.txt
echo "wrote result.txt after the wake"
EOF

  local directory submit run
  directory=$(fleet_executable_campaign "race-$bias-$index" race)
  submit=$(evidence_path "case12-$signal-submit.json")
  if ! fleet_submit_local "$directory" "qual-case12-$signal-$$" "$submit"; then
    printf '%s submission-failed submission-failed' "$bias"
    return 1
  fi
  run=$(run_of "$submit")

  # Sample the attempt while the race resolves. The invariant is checked on
  # every sample rather than only at the end, because "both at once" is a
  # transient state if it happens at all.
  local deadline=$((SECONDS + 150)) state violation="" seen=""
  while [ "$SECONDS" -lt "$deadline" ]; do
    state=$(fleet_coordinator_cli backlog show "$run" --json 2>/dev/null | reading task-state)
    case "$state" in
      "waiting-external "*)
        seen=waiting
        # A parked attempt must not also carry a terminal control state.
        case "$state" in
          *" stopped "*|*" completed "*) violation="parked attempt carries a terminal control: $state" ;;
        esac
        ;;
      succeeded*|failed*)
        # Terminal. If a local check is still polling for this task's park, the
        # attempt is terminal and waiting at the same time.
        if fleet_coordinator_cli wait list --all 2>/dev/null | grep -q "$signal"; then
          if fleet_coordinator_cli wait list --all 2>/dev/null | grep "$signal" | grep -qi waiting; then
            violation="attempt is $state while a task-bound wait for it is still waiting"
          fi
        fi
        break
        ;;
    esac
    sleep 1
  done
  if [ -n "$seen" ]; then
    : >"$ROOT/signals/$signal"
    await_task "$run" 240 succeeded failed >/dev/null || true
    state=$(fleet_coordinator_cli backlog show "$run" --json 2>/dev/null | reading task-state)
  fi
  local registration=none
  if [ -s "$ROOT/evidence/$signal-register.txt" ]; then
    if grep -q 'This task is now parked' "$ROOT/evidence/$signal-register.txt"; then
      registration=parked
    else
      registration=refused
    fi
  fi
  # The ordering that was actually reached, rather than the one that was
  # intended: an iteration that never parked did reach completion first.
  local ordering=registration-before-completion
  if [ "$registration" != parked ]; then
    ordering=completion-before-registration
  fi
  printf '%s %s %s %s %s' "$bias" "$ordering" "$registration" "${state%% *}" "${violation:-none}"
}

case_twelve() {
  local bias index observation
  local iterations=0 parked=0 refused=0 none=0 violations=0
  local before=0 after=0
  local -a rows=()
  for bias in registration-first completion-first; do
    index=1
    while [ "$index" -le "$QUAL_RACE_ITERATIONS" ]; do
      observation=$(race_iteration "$bias" "$index") || true
      rows+=("$observation")
      iterations=$((iterations + 1))
      case "$observation" in
        *" parked "*) parked=$((parked + 1)) ;;
        *" refused "*) refused=$((refused + 1)) ;;
        *" none "*) none=$((none + 1)) ;;
      esac
      case "$observation" in
        *" none") ;;
        *) violations=$((violations + 1)) ;;
      esac
      case "$observation" in
        *" registration-before-completion "*) before=$((before + 1)) ;;
        *" completion-before-registration "*) after=$((after + 1)) ;;
      esac
      index=$((index + 1))
    done
  done
  printf '%s\n' "${rows[@]}" >"$(evidence_path case12-observations.txt)"
  if [ "$violations" -ne 0 ]; then
    record case12 FAIL "$violations of $iterations iterations saw an attempt both waiting and terminal; see $(evidence_path case12-observations.txt)"
    return
  fi
  record case12 PASS "$iterations iterations ($QUAL_RACE_ITERATIONS per bias): orderings observed were $before registration-before-completion and $after completion-before-registration; $parked parked, $refused refused, $none without a registration record; no iteration saw an attempt both waiting and terminal. Observations in $(evidence_path case12-observations.txt)"
}
