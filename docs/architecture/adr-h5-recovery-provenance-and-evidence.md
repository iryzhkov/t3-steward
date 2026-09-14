# H5 ADR: keep the first cause, stop repeating yourself, and make a commit reachable

Status: accepted. Freezes the contract before implementation.
Date: 2026-09-14
Authority: the diagnostic and recovery failures observed alongside the UpKeeper Go-migration
campaign.

## Four small defects with one shape

Each of these turned a recoverable situation into an unexplainable one.

**Preparation retries destroy their own evidence.** The retained preparation log is named from the
attempt ID alone, and the attempt ID does not change across the three prepare attempts. The second
attempt cannot create the file, so the real Git error is wrapped in a "file exists" complaint and
the log pointer is dropped; the terminal reason reports only the last error. The first causal
failure — the one that says why the repository could not be cloned — is gone.

The fix is that each preparation attempt gets its own immutable evidence path, named from the
attempt and the retry ordinal, and the first causal failure is preserved in a durable field that
the final diagnostics quote alongside the retry count.

**A deterministic legacy-intake conflict is reported forever.** The legacy submission source
re-reads its drop directory every coordinator cycle and never drains it, so a file that can never
be accepted produces the same conflict on every tick, for as long as the coordinator runs.

The fix is quarantine after one durable report. The conflict is recorded in the existing
submission table — the drop directory is read-only to the coordinator, so the marker cannot live
in the filesystem — and a quarantined key is skipped on later ticks until its content digest
changes. One report, with the reason, and then silence.

**Catalog drift is reported as "no route".** When a worker's requirement no longer matches its
enrollment, the worker is removed from the inventory before routing is evaluated, so the operator
sees a generic "no eligible worker" for what is a digest mismatch.

The fix is that drift is named as drift, with the desired digest, the observed digest and the
expected revision, and an enrollment-planning command renders the authorization-relevant diff.
Planning is not applying: applying enrollment stays a deliberate, revision-fenced Steward
operation, for the reason the worker-enrollment ADR already gives — re-enrollment invalidates
in-flight offers.

**A terminal campaign has no honest way forward.** The only options were to leave it failed or to
mutate the historical run, and mutating it destroys the evidence of what happened.

The fix is `t3-steward campaign rerun <run> --from <task> --idempotency-key KEY`.

## Rerun

A rerun creates a new workflow run linked to the source run. It never mutates the historical run,
which stays failed and stays readable, because a run that pretends it did not fail is a run that
cannot be learned from.

The new run retains the original task definitions, the relevant input artifacts, the source commit
and the reason, and records its provenance: source run, source task, source attempt and the
idempotency key.

Scope is explicit rather than clever. The named task and every descendant of it are rerun.
Successful ancestors are reused, and their output artifacts are carried over by reference, not
copied. Artifacts produced by the failed subtree are not carried over; they are retained under the
source run as evidence and are not visible as inputs in the new run. If an ancestor's artifact is
no longer retrievable, the rerun refuses rather than silently starting a task with a missing input.

The idempotency key behaves as it does everywhere else: the same key with the same content returns
the same run, the same key with different content is refused.

## A commit a downstream task can actually reach

The implementation task in the failed campaign pushed its branch only into the worker's repository
cache. Another task later ran `remote update --prune` in that shared cache, the temporary branch
ref disappeared, and the commit survived only because nothing had garbage-collected it yet. The
next task had to fetch it by object identity.

That is not a handoff, it is a coincidence. So: a campaign-scoped Git commit or branch that a
downstream task needs is represented explicitly, either as a declared artifact or as a durable
remote ref in a campaign-scoped namespace that pruning does not touch. When a task declares that it
produces a commit, the coordinator keeps that commit reachable for the declared campaign lifetime
and reports its provenance — which task produced it, from which base, in which repository. A
downstream task resolves it by that reference, never by scanning a cache.

The repository cache stays what it is: a cache, safe to prune, holding nothing anyone depends on.

## Notification without a hand-written helper

Terminal notification is `campaign submit --notify-thread <current|id>`, which registers a node
wait on the run's sink bound to the caller's canonical T3 thread. `current` resolves the same way
it resolves for waits, through the canonical thread identity rather than a provider-local session
ID. No SSH helper, no direct SQLite reading, and no requirement that the submitting agent already
know its own thread ID.

## Known limitation: the commit's lifetime is the record's, and nothing prunes yet

Amendment, 2026-09-14, after implementation. "For the declared campaign
lifetime" is implemented as the lifetime of the provenance record that names the
commit: while the record is retained the commit must resolve, and when retention
removes the record the ref is released, on the coordinator and on every worker.
Settlement is deliberately not the boundary. A rerun may only be created from a
run that has already finished, so releasing at settlement released exactly the
commits a rerun was about to carry; tying the release to the record instead
makes a rerun hold its source with the retention pin it already takes, and needs
no republication and no special case.

The limitation this leaves is that no production path prunes coordinator
artifacts. The function exists and is the boundary, but nothing schedules it and
no retention window is configured, so in a deployed fleet the records — and the
commits — persist indefinitely. This ADR should not be read as delivering a
bound today. What it delivers is that campaign commits are not a separate
unbounded store with a lifetime of their own: they follow artifact retention
automatically, so the bound arrives with a retention pass and requires no
further work here.

A retention window is not chosen here on purpose. It is a policy decision about
an operator's data, and artifact retention policy is out of scope below;
shipping an invented default under cover of this work would be setting that
policy without saying so. Whoever configures a pass must account for pinned
runs: a pass reports a pinned run as skipped, with the owners holding it, and
prunes the rest rather than failing whole.

## Explicitly not in this work

No retry-policy redesign, no artifact retention policy change beyond keeping a declared campaign
commit reachable for the campaign's lifetime, no garbage-collection scheme for the repository
cache, and no change to how legacy submissions are authored — only to how often their permanent
failures are reported.
