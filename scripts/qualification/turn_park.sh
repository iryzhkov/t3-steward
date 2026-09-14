#!/bin/sh
# The scripted first turn of a parking scenario: register a task-bound wait and
# stop without writing anything.
#
# The synthetic provider runs this in place of an agent, with the prepared
# workspace as the working directory. Everything it calls is the real product.
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

# The revision in that record is compared with the revision the coordinator
# actually holds. They disagree: the execution package carries the attempt
# revision as of its construction, and the attempt has advanced by the time this
# turn runs, so the fence the product sends is stale. The comparison is recorded
# for the case11-identity-revision case, and the registration below proceeds
# with the current revision so that the rest of the lifecycle can be exercised.
CURRENT=$("$STEWARD" backlog show --config "$COORDINATOR_CONFIG" "$T3_STEWARD_WORKFLOW_RUN_ID" --json 2>/dev/null | python3 "$QUAL_INSPECT" task-state | cut -d' ' -f3)
printf 'workspace=%s coordinator=%s\n' "$T3_STEWARD_ATTEMPT_REVISION" "${CURRENT:-unknown}" >"$EVIDENCE/$QUAL_SIGNAL-revision.txt"

export T3_STEWARD_WORKFLOW_RUN_ID T3_STEWARD_TASK_ID T3_STEWARD_ATTEMPT_ID
export T3_STEWARD_ASSIGNMENT_ID T3_STEWARD_THREAD_ID
export T3_STEWARD_ATTEMPT_REVISION

# When the two revisions differ, the unmodified path is run first and its
# refusal is kept. That refusal is the evidence for the defect; it cannot park
# anything, so running it costs the case nothing.
if [ -n "${CURRENT:-}" ] && [ "$CURRENT" != 0 ] && [ "$CURRENT" != "$T3_STEWARD_ATTEMPT_REVISION" ]; then
  "$STEWARD" wait add --config "$COORDINATOR_CONFIG" --task current --name "$QUAL_SIGNAL-unpatched" --every 30s --max-every 30s --timeout 10m -- test -f "$SIGNALS/$QUAL_SIGNAL" >"$EVIDENCE/$QUAL_SIGNAL-register-unpatched.txt" 2>&1
  T3_STEWARD_ATTEMPT_REVISION=$CURRENT
fi

if ! "$STEWARD" wait add --config "$COORDINATOR_CONFIG" --task current --name "$QUAL_SIGNAL" --every 30s --max-every 30s --timeout 10m -- test -f "$SIGNALS/$QUAL_SIGNAL" >"$EVIDENCE/$QUAL_SIGNAL-register.txt" 2>&1; then
  # Say so in the final message rather than ending quietly. A turn that failed
  # to park and then stopped would otherwise look like an ordinary empty turn,
  # and the attempt would be collected and verified against nothing.
  echo "backlog status: failed task-bound wait registration failed"
  exit 0
fi

# The registration printed the instruction to end the turn. Ending here is the
# whole point of the case: nothing is written, so a collection that happened
# anyway would have nothing to collect and verification would fail.
echo "parked on $QUAL_SIGNAL"
