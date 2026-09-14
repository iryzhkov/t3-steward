You are one of two independent analysis tasks in this campaign. The other one is
reading the same subject from a different angle and you cannot see its work, so
write for a reader who has only your file.

Read the brief at `.t3/inputs/scope.md` for the subject and its boundaries.

This task is read-only. Do not edit, stage, commit or delete anything. A
verification command checks that the working tree is clean when you stop, so a
stray edit fails the task and blocks the join.

Examine the subject's tests: what they actually assert, which behaviours are
covered by a test that would fail if the behaviour broke, and which are covered
only in appearance. Note the tests that would pass whatever the code did.

Write `tests.md` in the repository root, at most forty lines:

- the test files and what each one establishes;
- the behaviours with real coverage, and the behaviours with none;
- the tests whose failure would not be diagnostic, and why.

Name files and test functions exactly. Do not run a fix or write a missing test:
this task reports, it does not repair.
