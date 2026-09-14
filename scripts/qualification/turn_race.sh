#!/bin/sh
# The scripted first turn of the race scenario.
#
# QUAL_BIAS selects which way the race is biased:
#   registration-first  register the wait, then end the turn
#   completion-first    start the registration detached and end the turn at once,
#                       so the provider reports the turn complete while the
#                       registration is still in flight
set -u

if [ ! -f ./.t3-steward/task.env ]; then
  echo "backlog status: failed no execution identity in the workspace"
  exit 0
fi
. ./.t3-steward/task.env

CURRENT=$("$STEWARD" backlog show --config "$COORDINATOR_CONFIG" "$T3_STEWARD_WORKFLOW_RUN_ID" --json 2>/dev/null | python3 "$QUAL_INSPECT" task-state | cut -d' ' -f3)
export T3_STEWARD_WORKFLOW_RUN_ID T3_STEWARD_TASK_ID T3_STEWARD_ATTEMPT_ID
export T3_STEWARD_ASSIGNMENT_ID T3_STEWARD_THREAD_ID
if [ -n "${CURRENT:-}" ] && [ "$CURRENT" != 0 ]; then
  T3_STEWARD_ATTEMPT_REVISION=$CURRENT
fi
export T3_STEWARD_ATTEMPT_REVISION

printf '%s\n' "$T3_STEWARD_ATTEMPT_ID" >"$EVIDENCE/$QUAL_SIGNAL-attempt.txt"

# The check is run once before the attempt is parked, so a check that takes time
# delays the registration by that much. That is how the completion-first
# ordering is reached: the turn has ended and the worker has already reconciled
# the terminal thread by the time the registration arrives.
register() {
  "$STEWARD" wait add --config "$COORDINATOR_CONFIG" --task current --name "$QUAL_SIGNAL" --every 30s --max-every 30s --timeout 10m -- /bin/sh -c "sleep ${QUAL_DELAY:-0}; test -f '$SIGNALS/$QUAL_SIGNAL'" >"$EVIDENCE/$QUAL_SIGNAL-register.txt" 2>&1
  printf 'exit=%s\n' "$?" >>"$EVIDENCE/$QUAL_SIGNAL-register.txt"
}

if [ "$QUAL_BIAS" = completion-first ]; then
  # Detached, with its own descriptors, so ending this turn does not wait for it
  # and the provider reports the turn complete while the registration is still
  # in flight.
  # Its descriptors are redirected away from the ones the provider is reading,
  # so the turn is reported complete without waiting for the registration.
  ( register </dev/null >/dev/null 2>&1 ) &
  echo "race: turn ended while the registration was in flight"
  exit 0
fi

register
echo "race: registered before ending the turn"
