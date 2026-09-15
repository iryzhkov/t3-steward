#!/bin/sh
# The scripted first turn of the replayed-request-id attack.
#
# It registers a task-bound wait under a fixed request id, and the second turn
# re-registers the same request id after that wait has settled. The second
# registration must be refused: the attempt is running again, so telling the
# agent to end its turn would end a turn nobody is going to resume, and the
# worker would collect and verify against outputs that were never written.
# That is the sequence that reconstructed the original failure.
set -u

# The state of the workspace is recorded whether or not the identity survived,
# because "the record survives for the turn that resumes after the wake" is a
# claim this attack has to be able to check.
{
  printf 'turn=%s pwd=%s\n' "${QUAL_TURN:-?}" "$(pwd)"
  ls -la . 2>&1 | head -20
  printf 'identity directory:\n'
  ls -la ./.t3-steward 2>&1 | head -10
} >"$EVIDENCE/$QUAL_SIGNAL-workspace.txt" 2>&1

if [ ! -f ./.t3-steward/task.env ]; then
  echo "backlog status: failed no execution identity in the workspace"
  exit 0
fi
. ./.t3-steward/task.env

# The identity is used exactly as the workspace record gives it. Nothing here
# corrects the revision: a task that cannot park itself with what it was given
# is the finding, not something for the harness to paper over.

if ! "$STEWARD" wait add --config "$COORDINATOR_CONFIG" --task current --name "$QUAL_SIGNAL" --request-id "$QUAL_REQUEST_ID" --every 30s --max-every 30s --timeout 10m -- test -f "$SIGNALS/$QUAL_SIGNAL" >"$EVIDENCE/$QUAL_SIGNAL-register.txt" 2>&1; then
  echo "backlog status: failed task-bound wait registration failed"
  exit 0
fi
echo "parked on $QUAL_SIGNAL"
