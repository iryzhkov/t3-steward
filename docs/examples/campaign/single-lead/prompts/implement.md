You are the only task in this campaign. You own the repository change from start
to finish, and nothing downstream will clean up after you.

Read the frozen plan at `.t3/inputs/plan.md` first. It states the objective, the
boundaries and what done means; treat it as authoritative and do not widen the
scope it sets. The preflight evidence you were given already reports the
repository's HEAD and the baseline test result, so start from those facts rather
than re-establishing them.

Work in the repository you were placed in:

1. Reproduce the defect the plan describes, with a test that fails for the
   stated reason rather than by accident.
2. Make the smallest change that fixes it.
3. Run the project's full test suite.
4. Commit the change and the test together, with a message that says what broke
   and why this fixes it.

Then write the three declared outputs, all relative to the repository root:

- `commit.txt` — the full SHA of the commit you made, and nothing else. A
  verification command resolves it, so a branch name or a short SHA with stray
  text will fail the task.
- `verification.txt` — the exact commands you ran to verify the change and their
  results, copied rather than paraphrased. Include the failing run from step 1
  and the passing run from step 3.
- `handoff.md` — at most twenty lines: what you changed, why, what you verified,
  and anything the next reader must know that the diff does not show. Name any
  boundary you had to approach, and say so plainly if you could not finish.

If the plan turns out to be wrong, or the fix cannot be made within its
boundaries, stop and say so in `handoff.md` rather than expanding the change.
An honest handoff that reports a blocker is a successful task; a large
unreviewable diff is not.
