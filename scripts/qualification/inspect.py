#!/usr/bin/env python3
"""Read one fact out of a coordinator JSON document, on standard input.

The cases need small, exact readings of admin output. Keeping them here rather
than inline in the shell means each reading is named, and a document the
coordinator did not produce is reported as unreadable instead of being silently
treated as an empty answer.

Usage: inspect.py <reading> [argument]
"""

import json
import sys


def load():
    try:
        return json.load(sys.stdin)
    except Exception:
        return None


def first_task(document):
    """Return the one non-sink task detail of a workflow-show document."""
    workflow = document.get("workflow") or {}
    for task in workflow.get("tasks") or []:
        if task.get("sink"):
            continue
        return task
    return None


def reading_task_state(document):
    task = first_task(document)
    if task is None:
        return "no-task no-task 0"
    attempt = task.get("attempt") or {}
    return "%s %s %s" % (
        attempt.get("progress") or "none",
        attempt.get("control") or "none",
        attempt.get("revision") or 0,
    )


def reading_task_field(document, field):
    task = first_task(document)
    if task is None:
        return ""
    if field == "taskId":
        return (task.get("task") or {}).get("id") or ""
    attempt = task.get("attempt") or {}
    if field in attempt:
        return attempt.get(field) or ""
    assignment = task.get("assignment") or {}
    return assignment.get(field) or ""


def reading_run_state(document):
    summary = (document.get("workflow") or {}).get("summary") or {}
    run = summary.get("run") or {}
    sink = run.get("sink") or {}
    return "%s/%s" % (run.get("progress") or "unknown", sink.get("progress") or "unknown")


def artifact_label(artifact):
    body = artifact.get("metadata") or artifact
    return "%s:%s" % (body.get("kind") or "?", body.get("name") or "?")


def reading_artifact_names(document):
    workflow = document.get("workflow") or {}
    names = [artifact_label(artifact) for artifact in workflow.get("artifacts") or []]
    task = first_task(document)
    if task:
        names.extend(artifact_label(artifact) for artifact in task.get("artifacts") or [])
    return " ".join(sorted(set(names)))


def reading_output_count(document):
    """Count the declared outputs collected for the task.

    This is the reading a parked attempt has to answer zero for: an output that
    exists means the worker collected while the task was supposed to be waiting.
    """
    task = first_task(document)
    if task is None:
        return "0"
    count = 0
    for artifact in task.get("artifacts") or []:
        body = artifact.get("metadata") or artifact
        if body.get("kind") == "output":
            count += 1
    return str(count)


def reading_verification(document):
    """Count verification report artifacts for the task."""
    task = first_task(document)
    if task is None:
        return "0"
    count = 0
    for artifact in task.get("artifacts") or []:
        body = artifact.get("metadata") or artifact
        name = (body.get("name") or "").lower()
        if body.get("kind") == "verification" or "verification" in name:
            count += 1
    return str(count)


def reading_assignment_state(document):
    task = first_task(document)
    if task is None:
        return "no-task"
    assignment = task.get("assignment") or {}
    return assignment.get("state") or "none"


def reading_events(document, needle):
    """Count events whose kind contains the needle."""
    count = 0
    for event in document.get("events") or []:
        if needle in (event.get("kind") or ""):
            count += 1
    return str(count)


def reading_quarantine(document):
    rows = document.get("quarantine") or document.get("quarantined") or []
    out = []
    for row in rows:
        out.append("%s|%s" % (row.get("key") or "?", (row.get("reason") or "?")[:120]))
    return "\n".join(out)


def reading_worker_catalog(document, worker_id):
    """Return the catalog revision the coordinator requires of one worker."""
    for worker in document.get("workers") or []:
        requirement = worker.get("requirement") or {}
        if requirement.get("workerId") == worker_id:
            return requirement.get("catalogRevision") or ""
    return ""


def reading_worker_states(document):
    rows = []
    for worker in document.get("workers") or []:
        snapshot = worker.get("snapshot") or {}
        rows.append(
            "%s=%s/%s"
            % (
                snapshot.get("workerId") or "?",
                worker.get("state") or "?",
                "enrolled" if worker.get("enrolled") else "unenrolled",
            )
        )
    return " ".join(sorted(rows))


def reading_workflow_count(document):
    if document.get("kind") == "error" or document.get("version") != "backlog.admin/v1":
        return "-1"
    return str(len(document.get("workflows") or []))


def main():
    if len(sys.argv) < 2:
        print(__doc__, file=sys.stderr)
        return 2
    document = load()
    if document is None:
        print("unreadable")
        return 0
    reading = sys.argv[1]
    argument = sys.argv[2] if len(sys.argv) > 2 else ""
    table = {
        "task-state": lambda: reading_task_state(document),
        "task-field": lambda: reading_task_field(document, argument),
        "run-state": lambda: reading_run_state(document),
        "artifacts": lambda: reading_artifact_names(document),
        "assignment-state": lambda: reading_assignment_state(document),
        "events": lambda: reading_events(document, argument),
        "quarantine": lambda: reading_quarantine(document),
        "workflow-count": lambda: reading_workflow_count(document),
        "worker-catalog": lambda: reading_worker_catalog(document, argument),
        "worker-states": lambda: reading_worker_states(document),
        "outputs": lambda: reading_output_count(document),
        "verification": lambda: reading_verification(document),
    }
    if reading not in table:
        print("unknown reading %s" % reading, file=sys.stderr)
        return 2
    print(table[reading]())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
