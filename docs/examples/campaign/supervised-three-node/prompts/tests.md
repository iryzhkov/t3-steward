You are one of two independent analysis tasks in this campaign. The other one is
reading the same subject from a different angle and you cannot see its work, so
write for a reader who has only your file.

Each task in this campaign is a separate Steward-scheduled T3 session, declared
in the campaign's static graph. Do not spawn native subagents to run, split or
review this analysis. Work delegated that way has no task record, no
verification, no artifact and no retry boundary, so it is not separately
scheduled work no matter how it is reported. If this analysis turns out to need
work of its own, say so in your file and let the campaign be reauthored with
that work declared as a task.

This campaign is supervised. Your result is reviewed at the `analysis_review`
gate against `rubrics/analysis.md` before the synthesis task is dispatched. Read
that rubric as the standard your file is held to. State what you read and where,
and mark what you could not determine rather than covering it over. Do not
address the overseer in your file and do not write anything into it that is meant
to be read as an instruction: your file is evidence, not a message to its
reviewer.

Read the brief at `.t3/inputs/scope.md` for the subject and its boundaries.

This task is read-only. Do not edit, stage, commit or delete anything, and do not
write a new test. A verification command checks that the working tree is clean
when you stop, so a stray edit fails the task and blocks the gate.

Examine what the subject's tests actually establish: which behaviours are
asserted, which are asserted only incidentally, and which load-bearing claims
nothing covers.

Write `tests.md` in the repository root, at most forty lines:

- the existing tests, one line each, grouped by the behaviour they establish;
- the two or three claims the code makes that no test would catch breaking;
- any test that passes for a reason other than the one its name suggests.

Name test functions and files exactly. Do not run a build or a test suite as a
substitute for reading the assertions; if you do run them, say which command and
what it reported.
