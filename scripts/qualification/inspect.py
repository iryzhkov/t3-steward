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
    attempt = task.get("attempt") or {}
    if field in attempt:
        return attempt.get(field) or ""
    assignment = task.get("assignment") or {}
    return assignment.get(field) or ""


def reading_run_state(document):
    summary = (document.get("workflow") or {}).get("summary") or {}
    run = summary.get("run") or {}
    return run.get("state") or "unknown"


def reading_artifact_names(document):
    workflow = document.get("workflow") or {}
    names = []
    for artifact in workflow.get("artifacts") or []:
        names.append("%s:%s" % (artifact.get("kind") or "?", artifact.get("name") or "?"))
    task = first_task(document)
    if task:
        for artifact in task.get("artifacts") or []:
            names.append("%s:%s" % (artifact.get("kind") or "?", artifact.get("name") or "?"))
    return " ".join(sorted(set(names)))


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
    }
    if reading not in table:
        print("unknown reading %s" % reading, file=sys.stderr)
        return 2
    print(table[reading]())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
