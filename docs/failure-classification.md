# Failure classification and automatic retries

Every failed or cancelled attempt carries a typed failure class and a
machine-readable reason code. The coordinator records both on the attempt
(`failureClass`, `failureReason`) on the first boundary after the attempt
becomes terminal, and `task result`, `campaign show`, `task show` and the
wake summary print them as `class/code`, for example
`infrastructure/provider-turn-failed`. An attempt recorded before
classification existed is classified from its reason text when it is shown.

The class decides one thing: whether the coordinator may retry the task on
its own. Only infrastructure failures are retried automatically, within a
small declared budget and after a backoff. Code, protocol, policy, cancelled
and unknown failures are never retried automatically; they stop the task as
before and an operator decides.

## Classes

- **infrastructure**: the machinery around the agent failed: a worker, a
  workspace, a T3 thread, a provider session or a collection. Running the same
  task again can succeed without anyone changing anything.
- **protocol**: the result broke the task contract: a declared output is
  missing, the agent ended its turn unfinished, evidence is missing, or the
  result could not be imported.
- **code**: the work ran and was checked, and the check failed: a declared
  verification or gate command exited non-zero.
- **policy**: a rule or a declared limit refused the result: the result
  secret scanner, a transfer size limit, the task timeout, a declared memory
  reservation or the review completion gate.
- **cancelled**: an operator, a run cancel or the coordinator stopped the
  attempt.
- **unknown**: the reason matches no row of the table. Unknown is never
  retried, because it is not known to be safe to repeat.

A secret scanner refusal is policy even though it is sometimes a false
positive: only an operator can tell, and an automatic retry would publish the
same bytes to the same scanner.

## Classification table

The table is `domain.FailureClassificationTable`. It is searched in order and
the first row whose text occurs in the recorded reason decides. The order is
the safety order: a reason that joins several failures ("verification command
failed ...; missing declared output ...") takes the class least willing to
retry, so policy rows come first, then code, then protocol, and
infrastructure last. A test keeps this page and the code table in step.

| Reason contains | Class | Code | Meaning |
|---|---|---|---|
| `permanent collection secret failure:` | policy | secret-scan | the result secret scanner refused the collected result |
| `withheld by secret scan` | policy | secret-scan | the result secret scanner withheld a failed result |
| `permanent collection size failure:` | policy | result-size | a collected artifact exceeded the transfer size limit |
| `task timeout expired` | policy | task-timeout | the task's declared timeout expired |
| `MB memory reservation` | policy | memory-limit | a contained run exceeded its declared memory reservation |
| `review gate` | policy | review-gate | the review completion gate refused unreviewed or changed work |
| `stopped by the coordinator before dispatch` | cancelled | coordinator-stop | the coordinator stopped the attempt before it was dispatched |
| `verification command failed (` | code | verification-failed | a declared verification command exited non-zero |
| `gate command failed (` | code | gate-failed | a declared gate command exited non-zero |
| `review_output verification failed:` | protocol | review-output-invalid | the declared review output is missing or malformed |
| `verification failed` | code | verification-failed | verification failed without a recorded command |
| `missing declared output:` | protocol | missing-output | the turn ended without writing a declared output |
| `missing verification evidence:` | protocol | missing-evidence | the result lacks verification evidence |
| `missing gate evidence` | protocol | missing-evidence | the result lacks gate evidence |
| `agent reported unfinished work:` | protocol | unfinished-work | the agent ended its turn with continue or needs-input |
| `result import rejected` | protocol | result-rejected | the result violates the import contract |
| `thread still has pending input, approval, or background work` | protocol | pending-thread-work | the turn ended with input, an approval or background work pending |
| `missing explicit success` | protocol | missing-success | the result carries no explicit success |
| `preparation failed` | infrastructure | preparation-failed | the worker could not prepare the workspace |
| `preparation returned an empty workspace` | infrastructure | preparation-failed | the worker could not prepare the workspace |
| `T3 thread creation failed:` | infrastructure | thread-start-failed | the worker could not create the T3 thread |
| `T3 thread never started:` | infrastructure | thread-start-failed | the T3 thread never started |
| `T3 refused to start the provider turn` | infrastructure | turn-start-refused | T3 or the provider refused to start the turn |
| `provider turn did not complete successfully` | infrastructure | provider-turn-failed | the provider turn ended in an error |
| `paused by quota watchdog:` | infrastructure | quota-pause | a quota pause ended the turn |
| `provider session is not ready without an active turn or error` | infrastructure | session-not-ready | the provider session was left in an unusable state |
| `thread completion identity is missing or mismatched` | infrastructure | thread-identity | T3 reported a different thread than the attempt's |
| `provider completion timestamps are missing or invalid` | infrastructure | thread-identity | T3 reported an incomplete turn record |
| `workspace is missing; outputs cannot be collected` | infrastructure | workspace-missing | the workspace vanished before collection |
| `preserved result digest mismatch` | infrastructure | preserved-result-mismatch | the workspace changed between the end of the turn and a collection retry |
| `contained run was killed for exceeding available memory` | infrastructure | host-memory | the host ran out of memory |
| `unknown execution resolved as failed` | infrastructure | execution-unknown | an operator resolved an execution of unknown state as failed |
| `was parked on a task-bound wait; the execution cannot be resumed` | infrastructure | execution-abandoned | the assignment of a parked attempt settled |
| `no longer owns parked attempt` | infrastructure | execution-abandoned | the assignment of a parked attempt was replaced |

Any cancelled attempt is `cancelled/cancelled`. Any reason no row matches is
`unknown/unrecognized`. A new failure reason gets a new row and, where its
meaning is new, a new code; an existing code is never reused for a different
meaning.

## Automatic retries

On every boundary, before the workflow projection, the coordinator looks at
the latest attempt of every task. It submits an automatic retry when all of
these hold:

- the attempt failed with an infrastructure-class failure;
- the task has budget left: fewer earlier automatic retries in this run than
  its effective budget;
- the run is unsupervised and its sink is not final (a supervised run is left
  to its overseer's recovery, so two retry authorities never race);
- no automatic retry was submitted for that attempt before, and no operator
  retry of it is pending.

The retry is an ordinary admin `retry` command with the stable ID
`auto-retry-<attempt>` and the principal
`steward-coordinator/automatic-retry`, audited on submission like any other
command. It is applied by the same code an operator's retry is, so the
closed-run refusal and review retry admission still apply, and the new
attempt goes through quota admission, placement and verification like every
attempt. The new attempt carries an `automaticRetry` receipt naming the
failed attempt, its class and code, which retry of the budget it is, and the
time before which it is not admitted.

While a retry is due but not yet created, the projection leaves the run
alone: its sink does not settle and the failed task's dependents are not
skipped. A retry command that is rejected (for example because the run was
closed meanwhile) releases the run, which then settles as failed.

### Budget

A workflow declares a default budget for all its tasks and a task may
override it:

```yaml
retry:
  infrastructure: 2   # automatic retries of infrastructure failures, 0..5
  backoff: 2m         # delay before the first one; each later one doubles
tasks:
  flaky-integration:
    prompt_file: prompts/flaky.md
    retry:
      infrastructure: 3
  must-not-repeat:
    prompt_file: prompts/once.md
    retry:
      infrastructure: 0   # never retried automatically
```

Without a `retry` block the budget is 2 retries with a 2 minute backoff. The
backoff doubles for each later retry and never exceeds one hour. A task's
block overrides the fields it names; the rest come from the workflow block.
`campaign validate` refuses a budget outside 0..5, a backoff that is not
positive or longer than one hour, and an empty block.

The coordinator caps every budget with
`backlog_v2.coordinator.automatic_retries.max_infrastructure` (default 3,
range 0..5). Zero turns automatic retries off on that coordinator; failures
are still classified. The setting is read when the coordinator starts.

## Keeping completed work through a collection failure

When a collection captures a finished turn's result, the worker records a
preserved result beside the attempt's workspace
(`preserved-result.json`): the SHA-256 of every declared output file and the
commit every declared commit resolves to, bound to the execution and the
turn. The workspace and its commits stay on the worker for the attempt's
retention.

If publishing that result fails (an upload or verification transport error,
a worker restart), the worker retries the collection on a later pass instead
of running the agent again. Before the retry captures anything it recomputes
the digest:

- **match**: the retry reuses the workspace and publishes the turn's work;
- **mismatch** (a declared output or commit changed since the capture): the
  attempt fails with `preserved result digest mismatch: ...`, an
  infrastructure failure, because work that cannot be proven to be the turn's
  is never collected;
- **missing workspace**: the attempt fails with `workspace is missing;
  outputs cannot be collected: ...`, also infrastructure.

A retry whose digest matches runs verification again, and a verification
command may write a declared output (a report with a timestamp, an appended
log). The retry therefore records the digest of what it captured after
verification, replacing the earlier one, so a further retry after another
publication failure is compared with the result the latest collection
sealed. Only verification run by a digest-verified collection can move the
record; any other change is still a mismatch.

Both failures are then eligible for an automatic retry, which runs the agent
afresh. A record of an earlier turn of the same attempt is replaced rather
than compared, because a later turn was entitled to change the work. A
workspace the worker cannot read (some contained deployments) records
nothing, and its collection retries behave as before.

The preserved result covers retries of collection within one attempt. A new
attempt, including an automatic retry, always gets a fresh workspace and a
fresh turn; reusing a previous attempt's workspace in a new attempt is not
implemented.
