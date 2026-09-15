#!/bin/sh
# The scripted first turn of a parking scenario: register a task-bound wait and
# stop without writing anything.
#
# The synthetic provider runs this in place of an agent, with the prepared
# workspace as the working directory. Everything it calls is the real product,
# and nothing about the execution identity is corrected: the task parks itself
# with exactly what it was given, which is the property the case exists for.
# The caller exports STEWARD, COORDINATOR_CONFIG, SIGNALS, EVIDENCE,
# QUAL_SIGNAL and QUAL_INSPECT.
set -u

# The worker writes the execution identity into the workspace before dispatch,
# and that is where a task is meant to read it from.
if [ ! -f ./.t3-steward/task.env ]; then
  echo "backlog status: failed no execution identity in the workspace"
  exit 0
fi
. ./.t3-steward/task.env

# The revision the workspace record names is compared with the revision the
# coordinator holds, and recorded. It is evidence only: the registration below
# uses the workspace value unchanged, so a mismatch that still parks is the fix
# working, and a mismatch that refuses is the defect returning.
CURRENT=$("$STEWARD" backlog show --config "$COORDINATOR_CONFIG" "$T3_STEWARD_WORKFLOW_RUN_ID" --json 2>/dev/null | python3 "$QUAL_INSPECT" task-state | cut -d' ' -f3)
printf 'workspace=%s coordinator=%s\n' "$T3_STEWARD_ATTEMPT_REVISION" "${CURRENT:-unknown}" >"$EVIDENCE/$QUAL_SIGNAL-revision.txt"

if ! "$STEWARD" wait add --config "$COORDINATOR_CONFIG" --task current --name "$QUAL_SIGNAL" --every 30s --max-every 30s --timeout 10m -- test -f "$SIGNALS/$QUAL_SIGNAL" >"$EVIDENCE/$QUAL_SIGNAL-register.txt" 2>&1; then
  # Say so in the final message rather than ending quietly. A turn that failed
  # to park and then stopped would otherwise look like an ordinary empty turn,
  # and the attempt would be collected and verified against nothing.
  echo "backlog status: failed task-bound wait registration failed"
  exit 0
fi

# A second wait on the same attempt, registered while the first is live. The
# first registration advances the attempt revision, so this is exactly the
# registration that was impossible while the fence was an equality test. It is
# optional: a scenario that does not want it does not set QUAL_SECOND_WAIT.
if [ -n "${QUAL_SECOND_WAIT:-}" ]; then
  "$STEWARD" wait add --config "$COORDINATOR_CONFIG" --task current --name "$QUAL_SIGNAL-second" --wake "${QUAL_WAKE:-each}" --every 30s --max-every 30s --timeout 10m -- test -f "$SIGNALS/$QUAL_SECOND_WAIT" >"$EVIDENCE/$QUAL_SIGNAL-register-second.txt" 2>&1
  printf 'exit=%s\n' "$?" >>"$EVIDENCE/$QUAL_SIGNAL-register-second.txt"
fi

# The registration printed the instruction to end the turn. Ending here is the
# whole point of the case: nothing is written, so a collection that happened
# anyway would have nothing to collect and verification would fail.
echo "parked on $QUAL_SIGNAL"
