#!/usr/bin/env bash
# Case 12 proves both sides of registration versus completion with real tasks.
set -euo pipefail
QUAL_RACE_ITERATIONS=${QUAL_RACE_ITERATIONS:-3}

race_iteration() {
  local bias=$1 index=$2 signal="race-$1-$2" project=qual-race
  rm -f "$ROOT/signals/$signal" "$ROOT/signals/$signal-allow-registration"
  fleet_turn_script "$project" 1 <<EOF
#!/bin/sh
$(fleet_task_env)
export STEWARD COORDINATOR_CONFIG SIGNALS EVIDENCE
export QUAL_SIGNAL=$signal
export QUAL_BIAS=$bias
exec /bin/sh $HARNESS_DIR/turn_race.sh
EOF
  fleet_turn_script "$project" 2 <<EOF
#!/bin/sh
$(fleet_task_env)
printf 'written after the wake\n' > result.txt
echo "wrote result.txt after the wake"
EOF
  local directory submit run state stopped samples=0 released=0
  directory=$(fleet_executable_campaign "race-$bias-$index" race)
  submit=$(evidence_path "case12-$signal-submit.json")
  fleet_submit_local "$directory" "qual-case12-$signal-$$" "$submit" || return 1
  run=$(run_of "$submit")
  local deadline=$((SECONDS + 300))
  while [ "$SECONDS" -lt "$deadline" ]; do
    read -r state stopped < <(python3 "$HARNESS_DIR/race_observe.py" sample "$ROOT/coordinator/state.db" "$run" "$ROOT/evidence/$signal-samples.jsonl")
    if [ "$bias" = registration-first ] && [ "$state" = waiting-external ] && [ "$stopped" = true ] &&
       grep -q 'exit=0' "$ROOT/evidence/$signal-register.txt" 2>/dev/null; then
      samples=$((samples + 1))
      if [ "$samples" -ge 3 ]; then
        : >"$ROOT/signals/$signal"
      fi
    fi
    case "$state" in
      succeeded|failed)
        if [ "$bias" = completion-first ] && [ "$released" = 0 ]; then
          # The first check is already in flight, but cannot return until the
          # coordinator has committed this terminal outcome.
          printf '{"run":"%s","progress":"%s"}\n' "$run" "$state" >"$ROOT/evidence/$signal-release.json"
          : >"$ROOT/signals/$signal-allow-registration"
          released=1
        fi
        if grep -q '^exit=' "$ROOT/evidence/$signal-register.txt" 2>/dev/null; then
          break
        fi
        ;;
    esac
    sleep 5
  done
  fleet_coordinator_cli backlog show "$run" --json >"$ROOT/evidence/$signal-final.json"
  python3 "$HARNESS_DIR/race_observe.py" verdict "$ROOT/evidence" "$signal" "$bias" "$run"
}

case_twelve() {
  local bias index observation failures=0
  : >"$(evidence_path case12-observations.txt)"
  for bias in registration-first completion-first; do
    for ((index=1; index<=QUAL_RACE_ITERATIONS; index++)); do
      if observation=$(race_iteration "$bias" "$index"); then
        printf '%s\n' "$observation" >>"$(evidence_path case12-observations.txt)"
      else
        failures=$((failures + 1))
        printf '%s\n' "${observation:-unobserved iteration: $bias $index}" >>"$(evidence_path case12-observations.txt)"
      fi
    done
  done
  if [ "$QUAL_RACE_ITERATIONS" -lt 1 ] || [ "$failures" -ne 0 ]; then
    record case12 FAIL "$failures incomplete/invalid race iterations; both observed orderings and successful terminal outcomes required; see $(evidence_path case12-observations.txt)"
  else
    record case12 PASS "$QUAL_RACE_ITERATIONS parked and $QUAL_RACE_ITERATIONS terminally refused registrations, both orderings proven, every task succeeded with output/verification evidence; see $(evidence_path case12-observations.txt)"
  fi
}
