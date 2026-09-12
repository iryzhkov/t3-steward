#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 || $# -gt 2 ]]; then
  echo "usage: $0 WORKFLOW_RUN [STALL_SECONDS]" >&2
  exit 2
fi

run_id=$1
stall_seconds=${2:-180}
payload=$(t3-steward backlog show "$run_id" --json) || exit 1
progress=$(jq -r '.workflow.summary.run.progress' <<<"$payload")

case "$progress" in
  succeeded|failed|cancelled|skipped)
    echo "SETTLED run=$run_id progress=$progress"
    exit 0
    ;;
esac

latest=$(jq -r '[.workflow.tasks[].attempt.updatedAt // empty] | max // empty' <<<"$payload")
if [[ -z "$latest" ]]; then
  echo "WAITING run=$run_id progress=$progress reason=no-attempt-timestamp"
  exit 1
fi

latest_epoch=$(date --date="$latest" +%s) || exit 2
now_epoch=$(date +%s)
age=$((now_epoch - latest_epoch))
if (( age >= stall_seconds )); then
  states=$(jq -c '[.workflow.tasks[] | {task:.task.name,progress:.attempt.progress,control:.attempt.control,updatedAt:.attempt.updatedAt}]' <<<"$payload")
  echo "STALLED run=$run_id progress=$progress age_seconds=$age states=$states"
  exit 0
fi

echo "WAITING run=$run_id progress=$progress age_seconds=$age threshold_seconds=$stall_seconds"
exit 1
