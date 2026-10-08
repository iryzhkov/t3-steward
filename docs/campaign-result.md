# Campaign result and the PR-checks wait

After a run ends, a coordinating session used to run `campaign show`, one
`task result` per task, `backlog artifact get` and log tails, and then parse
`review.md` and gate logs, to learn one thing: was the unit accepted. Two
additions shorten that.

## `t3-steward campaign result <run> [--json] [--wait [--timeout D]]`

```
$ t3-steward campaign result run-4be1
run run-4be1: succeeded (2 tasks: 2 succeeded)
  implement  succeeded; commit 1a2b3c4d5e6f; verification passed 5/5
  review     succeeded; verdict ACCEPT blocking=0 on implement 1a2b3c4d5e6f
review ACCEPT and verification passed on the same commit: yes, 1a2b3c4d5e6f (review accepted, implement verification passed)
```

One line per task gives its state, the failure class and failure of a failed
task, the review verdict recorded for its latest attempt and the commits that
verdict is about, the review gate of a review-declared task, the declared
commit and the verification result. The last line is the acceptance fact.

### Why a new verb

`campaign show` forwards to `backlog show` and prints the full run document,
whose text and JSON contracts other tools already read; adding a summary to it
would change both. `task result` collects a task's files into a directory. The
result block is a third thing, the decision summary of the other two, and it
sits in the campaign family because it is about a run. The verb mirrors
`task result`: the same `--wait`/`--timeout` semantics and the same exit codes.

### The acceptance fact

"Review ACCEPT and verification passed on the same commit" is `yes` only when
both halves are recorded facts that name one commit:

- **A review task's verdict.** The coordinator records the verdict of a task
  that declares `review_output` on its attempt. The verdict is about the commit
  outputs the review task consumed, which the graph records as its dependency
  inputs. Each such commit is read from the producer's commit record of its
  latest attempt. That attempt must have succeeded, and its own verification
  must have passed: every declared verification command has a retained report
  with exit code 0, and a declared gate's report says passed for that attempt.
  A run's producer has one succeeded attempt, and its consumers start only
  after it, so this is the commit the review read.
- **A review-declared task's gate.** A task with `review:` requirements
  records a completion gate. A gate that passed as `accepted-head` binds the
  latest accepted review round, the clean workspace and the declared commit to
  one head. That head must be the commit the task's commit record names, and
  the task's own verification must have passed as above.

The fact is `no` when no verdict is recorded (a `review.md` that says ACCEPT is
not a recorded verdict), when verification is missing, failed, unreadable or not
declared, when the commit record is missing, unreadable or of a failed attempt,
when the review consumed no commit, and when any recorded verdict asks for
changes on the same commit. Its reason names which. It is never computed from
exit codes or from tasks having succeeded. When several commits qualify, the
last in manifest order is reported.

The failure class is read from the stable prefix of the recorded failure
(`verification`, `gate`, `review-gate`, `missing-output`, `declared-commit`,
`dependency`, `no-success`, `infrastructure`, `cancelled`, `other`). It is a
reading aid; nothing is decided from it.

### Exit codes and waiting

The exit code is the run's: 0 succeeded, 2 failed or cancelled, 1 not terminal
yet. Acceptance is in the output, not in the exit code. `--wait` blocks until
the run is terminal; `--timeout D` bounds the wait, prints the last state and
exits 1. Interrupting a wait never cancels work.

### JSON

`--json` prints one document, `schemaVersion`
`"t3-steward.campaign-result/v1"`:

```json
{
  "schemaVersion": "t3-steward.campaign-result/v1",
  "run": "run-4be1", "state": "succeeded", "terminal": true,
  "tasks": [
    {"task": "implement", "taskId": "...", "attemptId": "...", "state": "succeeded",
     "commits": [{"task": "implement", "name": "implementation", "commit": "1a2b..."}],
     "verification": {"state": "passed", "declared": 5, "passed": 5}},
    {"task": "review", "taskId": "...", "attemptId": "...", "state": "succeeded",
     "verdict": {"verdict": "ACCEPT", "blockingFindings": 0},
     "verification": {"state": "not-declared", "declared": 0, "passed": 0},
     "reviewed": [{"task": "implement", "name": "implementation", "commit": "1a2b..."}]}
  ],
  "acceptance": {"accepted": true, "commit": "1a2b...", "reviewTask": "review",
                 "verifiedTask": "implement", "reason": "review accepted, implement verification passed"}
}
```

A task may also carry `failureClass`, `failure` and `reviewGate`
(`passed`, `code`, `reviewedHead`, `roundVerdict`). Verification states are
`not-declared`, `pending`, `passed`, `failed`, `missing` and `unavailable`, with
`gate` set when the task declares a gate.

## `wait add --github-checks REF`

A github wait on the checks of one commit, or of a pull request's head:

```sh
t3-steward wait add --task current --github-checks owner/name@$(git rev-parse HEAD) --timeout 2h
t3-steward wait add --github-checks owner/name#45
```

`REF` is `owner/name@<sha>`, `https://github.com/owner/name/commit/<sha>`, a
bare `<sha>` of 7 to 40 hexadecimal digits with `--repo`, or a pull request in
the forms `--github pr` accepts (`owner/name#<n>`, its URL, or `<n>`). Digits
alone are always a pull request number. It is a `github` wait in state
`checks-completed` (also available as `--github pr <n> --state
checks-completed`), so it is registered, polled, given up on and woken exactly
like `--github`, from a session or from inside a task.

A commit's checks are read with one `gh api graphql` call for the commit's
status-check rollup: its check runs and commit statuses. The wait settles only
once no check is still running. Unlike `checks-passed`, it does not settle on
the first failure, so the summary covers every check. It is met when none
failed and failed when one did. The reason is one line counting the
conclusions, failures first:

```
checks failed: 6 checks, 1 failure (lint), 1 skipped, 4 success
```

The wake trailer carries `target=commit:<sha>` (or `pr:<n>`), `conclusion=`
and `checks=<conclusion>=<n>,...`. A commit or repository GitHub does not know
gives up at once. When more than 100 checks exist, the ones not listed are
judged by GitHub's own rollup state.
