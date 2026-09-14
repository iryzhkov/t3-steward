# H3 ADR: live campaign readiness, and a probe that may talk to a repository

Status: accepted. Freezes the contract before implementation.
Date: 2026-09-14
Authority: the UpKeeper Go-migration campaign, which was accepted against a project whose
repository was wrong and failed only when a worker tried to prepare a workspace.

## Scenario

`campaign validate` and `campaign plan` are offline and pure, and the CM0 ADR is explicit that a
static plan may not claim a worker, a route or capacity will be available. That is right, and it
leaves a gap: submission performs no project, worker, route or repository check at all.
`SubmissionService.SubmitDirectory` reserves an idempotency key and ingests the bundle; the first
time anything asks whether the work is possible is when the planner looks for a worker, and the
first time anything asks whether the repository exists is when a worker runs `git clone`.

So a permanently impossible campaign still creates a workflow run, consumes a run ID, occupies
the graph, and fails hours later with a message about workspace preparation. Four of the observed
dogfood failures are this one gap: a repository URL that was wrong, anonymous HTTPS against a
private repository, a missing ref, and a project whose eligible workers could not serve it.

## Decision

Add a third verb, `t3-steward campaign check <dir> [--json]`, which is read-only, live, and
separate from `validate` and `plan`.

It is a new verb rather than a flag, because the campaign test suite deliberately disarms the
coordinator seams and fails if an authoring command touches them. That guard states the property
that `validate` and `plan` are offline; a flag that reached the coordinator would erase it.

`check` asks the coordinator for a read-only per-task, per-worker viability matrix. `submit` runs
the same check by default before packing, and the coordinator repeats the permanent part of it
transactionally during acceptance, so a check that passes and a fleet that changes underneath
still cannot create an impossible run.

## The matrix

For every task in the projected plan and every worker eligible for it, the coordinator reports:
worker freshness; enrollment and catalog-digest match; project policy; eligible-worker policy;
CPU class; resource requirements; capabilities; required directories; setup or environment
profile; provider instance; model; quota pool; observed T3 authentication; credential-reference
availability; repository reachability; ref reachability; timing constraints; resource locks;
artifact and message limits; and quota-snapshot freshness.

The evaluation reuses what already exists and is already pure: `MatchWorkers` for placement,
`ResolveProviderRoutePools` for routes, and the explanation composer's worker, route and quota
blockers. `check` adds no second copy of the eligibility rules, for the same reason the campaign
façade refused a second validator.

One behaviour is deliberately corrected rather than reused. Today a worker whose requirement no
longer matches its enrollment is dropped from the inventory before routing is evaluated, so the
operator is told "no eligible worker" when the truth is a catalog-digest mismatch. `check` reads
the worker requirement's catalog revision and the enrollment's accepted revision directly and
reports the drift as drift, with both digests and the expected revision.

## The repository probe

Repository and ref reachability is a bounded, built-in worker-protocol probe equivalent to
`git ls-remote <repository> <ref>`, run under the same execution identity and credential
references the real task would use.

- No shell. It runs through the existing direct-argument process path, argv built as
  `git ls-remote --exit-code -- <repository> <ref>`, with the `--` separator the probe runner
  already emits.
- No arbitrary manifest command. It is a built-in probe name in the probe registry; a campaign
  cannot supply the argv.
- Syntax and scheme are validated before the network is touched, reusing the repository and ref
  validators the catalog already applies: https and ssh only, no embedded credentials, no query
  or fragment, and refs rejected for leading `-` or `/`, `..`, `@{`, `//`, `.lock` and control
  characters. A repository or ref beginning with `-` is refused as a value, never passed as a
  flag.
- Runtime and output are bounded before accumulation, not after. The existing probe path buffers
  the whole output and truncates afterwards; a repository with very many refs makes that a memory
  hazard, so the probe reads bounded output and stops.
- Results are classified, not collapsed: `authenticated-ok`, `authentication-failed`,
  `repository-not-found`, `ref-not-found`, `timeout`, `dns-failure`, `network-unavailable`.
- Evidence is retained with a short TTL and is bound to worker, catalog digest, repository, ref
  and credential references. A change to any of those invalidates it.
- Credentials never appear in the result, the evidence, the logs or the diagnostics. The probe
  reports that a credential reference resolved, never what it resolved to.

This is a deliberate, documented exception to the comment that probes grant no network authority.
The exception is narrow: one built-in probe, one command shape, arguments the caller cannot choose.

## Result taxonomy

| Outcome | Meaning | Submission |
| --- | --- | --- |
| `ready` | every task has at least one viable candidate now | proceeds |
| `accepted_waiting` | no candidate now, but the obstruction is temporary | proceeds, reasons reported |
| `impossible` | no candidate can ever satisfy the request as written | refused, zero workflows created |

Permanent, and therefore refused before a workflow exists: unknown catalog entries (project,
setup profile, provider instance, model, quota pool); no configured route on any eligible worker;
invalid repository syntax; confirmed authentication failure, repository-not-found or
ref-not-found on every candidate; impossible CPU, resource, directory or capability requirements;
and unavailable required credential references.

Temporary, and therefore compatible with asynchronous execution: quota currently closed; every
eligible worker at capacity; an otherwise eligible worker briefly offline; temporary network
failure; and an observation that is stale but recoverable.

The distinction is about the request, not about the moment. "No worker is free" is temporary
because waiting fixes it. "No worker has this capability" is permanent because waiting does not.

## `--allow-unverified`

The escape hatch exists, is operator-oriented, and is loud. It skips the live check on the client
side only; the coordinator still applies its transactional permanent validation at acceptance,
because an escape hatch that could create a structurally impossible run would defeat the point.
Every use is recorded in the submission audit record with the principal and the reason. It is not
implied by `--json`, not implied by any quiet flag, and the help text says plainly that agents
should not use it.

## Help is part of the contract

`campaign --help` distinguishes the three verbs in one line each: `validate` is offline and
structural, `plan` is offline and shows the graph that would be created, `check` asks the live
coordinator whether it could run. `submit` documents that it checks first, and what
`accepted_waiting` means for an agent that is about to end its turn. Each carries a complete
copyable example, its exit codes, its JSON availability, and the recovery command for the common
failures.

## Alternatives rejected

Making `plan` live was rejected: CM0 froze it as a static projection, and an agent needs one
answer that cannot lie because it never asked anyone.

A generic "run this command on a worker to check something" protocol message was rejected. It is
the same authority hole as forwarding a remote shell command, and the manifest would become a
place where arbitrary argv is executed under coordinator authority.

Probing from the coordinator instead of the worker was rejected. The coordinator's credentials
and network position are not the worker's, so a coordinator-side probe would answer a question
nobody asked, and would report success for a repository the worker cannot reach.

Caching probe results for a long time was rejected. Reachability and authentication change
exactly when a credential is rotated or a repository is renamed, which is when a stale positive
is most expensive.
