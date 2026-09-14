# Campaign manager implementation prompt

Copy the block below into a new lead-agent T3 session.

~~~text
Implement and publish the bounded t3-steward campaign manager described in
docs/plans/campaign-manager.md.

Starting authority
------------------
Repository: /home/igor/Work/t3-steward
Required base: origin/main at or descended from
f86c420 (v0.11.0-rc.38).

Verified starting receipt
-------------------------
Observed on homelab (linux/amd64, Go 1.27.1) against exact source
f86c4200e618a40593d4e475e7f4364c7de86a64 on 2026-09-14:
- gofmt gate: PASS
- go build ./...: PASS
- go test ./...: PASS
- go test -race ./...: PASS
- go vet ./...: PASS
- make lint with the pinned Go 1.25.0/staticcheck v0.7.0 pair: PASS
- exact-commit GitHub CI: PASS, run 34813636526

First fetch and compare current origin/main with this receipt. If it is unchanged, adopt the receipt
and run focused tests for your first change rather than repeating the full baseline. If main
advanced, inspect the delta and run checks proportional to it. Continue only when new commits do not
invalidate the plan's fixed boundaries.

Product boundary
----------------
Campaign is an agent-facing façade over the existing version-2 workflow bundle and WorkflowRun.
Do not add a campaign database, a second DAG engine, a new manifest schema or another scheduler.

Deliver:
1. campaign validate <directory|workflow.yaml> [--json]
2. campaign plan <directory|workflow.yaml> [--json|--dot]
3. campaign submit <directory|workflow.yaml> --idempotency-key KEY [--json]
4. the bounded lifecycle aliases named in the plan;
5. deterministic internal packaging using the existing parser, safety limits and submission
   transport;
6. stable static DAG projection with roots, leaves, waves, edges, inherited execution settings and
   artifact bindings;
7. excellent concise help and checked-in single-lead and three-node examples;
8. focused, full, race, vet and pinned-lint validation;
9. exact-commit CI, the next release candidate, UpKeeper publication and two small dogfood
   projects: a single-lead coding campaign and a parallel three-node artifact-join campaign.

Execution discipline
--------------------
You are the lead. You own architecture coherence, shared interfaces, integration, commits, CI,
release, UpKeeper publication, the dogfood run and final report.

Use at most two concurrent leaf subagents with disjoint ownership:
- CLI/packaging lane: shared loader/packer and validate/submit.
- DAG/view lane: static plan projection, renderers, examples and relevant help.

Subagents do not commit or push. Keep their assignments bounded and integrate their work yourself.
Do not create more orchestration layers merely to coordinate the implementation.

Every agent and subagent must use Huyang for all repository reading, searching and editing:
workspace_open first, then Huyang read/search/edit operations. Shell is only for Git, builds, tests
and commands whose output is the point.

Verification strategy
---------------------
Run focused package tests during CM1-CM3. Defer the disposable coordinator integration and broad
end-to-end fix loop to CM4-CM5. Do not repeatedly deploy the fleet during implementation.

Before publication require:
- gofmt clean;
- go build ./...;
- go test ./...;
- go test -race ./...;
- go vet ./...;
- make lint;
- exact-commit GitHub CI.

Publish validated agent-tooling changes through UpKeeper. Review the complete manifest diff,
preserve unrelated pins/configuration/secret references, confirm UpKeeper CI, and verify intended
fleet convergence. Do not capture hand-built or unvalidated binaries.

Dogfood
-------
After deployment, run two small projects through the new campaign command, each from a preserved
campaign directory and with a distinct idempotency key:

1. Single-lead coding project: a disposable small Git repository with one bounded defect or feature.
   The one lead task must return a verified commit, test receipt and concise handoff.
2. Parallel DAG artifact project: two independent non-mutating analysis nodes produce different
   bounded artifacts; one join/qualification node declares both with inputs_from, verifies their
   materialization and emits a combined result.

Prefer gpt-5.6-sol for one project and claude-opus-5 for the other when both configured routes are
healthy. Either approved route may substitute when quota or availability requires it. Record the
actual route; do not treat this as a model-quality comparison.

Let ordinary placement choose workers and do not pin hosts merely to manufacture distribution. Each
submission must create exactly one workflow run and settle without duplicate work. Rerun each
project at most once after a campaign-blocking steward fix, using a new idempotency key and preserving
the first result.

Record campaign directories/digests, operator time, interventions, run/task/thread IDs, routes,
placement, artifacts, terminal states and provider usage separately for both projects. Stop after
this bounded evidence gate. Do not add cron, webhook, OV/Pensieve, dynamic-DAG, checkpoint,
model-routing, credential, retention or UI work.

User decisions
--------------
Use T3 structured user input only when a missing authority or material choice blocks all remaining
independent work. Ordinary implementation choices and recoverable failures belong to you.

Completion
----------
Finish only when CM0-CM5 in docs/plans/campaign-manager.md pass, or a genuine user decision blocks
the campaign. The final response must distinguish implemented behavior, validated evidence,
publication/convergence and explicit deferrals.
~~~
