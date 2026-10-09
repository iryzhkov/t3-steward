# CLI route policy

The steward reads `$XDG_CONFIG_HOME/t3-steward/route-policy.yaml` (normally
`~/.config/t3-steward/route-policy.yaml`), beside `coordinator-client.json`.
It never installs or replaces it. Use `policy show --file PATH` to inspect a
candidate file and `policy validate --file PATH` to check it against the
coordinator's configured worker model authorizations. Both accept `--json`.
Validation is read-only and does not depend on workers being online. A coordinator
that cannot report configured model authorizations refuses validation; inventory
alone cannot establish validity. A sole configured models list `["*"]` authorizes
any concrete model on that provider instance, using the same authorization rule
as worker routing. Explicit lists authorize only their named models; mixed lists
and partial patterns do not grant wildcard authorization. Dropped providers do
not authorize candidates. Catalog validity does not grant runtime readiness,
project advertisement or model observation, and `*` is never a runnable model.

```yaml
schema: route-policy/v1
roles:
  - name: execute
    candidates:
      - {route: claudeAgent/claude-opus-5-5, effort: high, tier: standard}
      - {route: codex/gpt-6.1-sol, effort: high, tier: standard}
  - name: critical-review
    constraints:
      tiers: [premium]
      exclude_provider_families: [example-executor-family]
    candidates:
      - {route: codex/gpt-6-astra, effort: medium, tier: premium}
  - name: read
    candidates:
      - {route: codex/gpt-6-luna, effort: medium, tier: economy}
```

Roles are an ordered list of unique names: plan, execute, review, critical-review,
read, and safe specialty names such as 3d. Candidates are ordered unique
INSTANCE/MODEL routes; MODEL may contain slashes. Every candidate requires effort
(low, medium, high) and cost tier (economy, standard, premium). Max and higher
efforts are refused. Unknown fields, duplicate roles/routes, malformed documents
and unknown schema versions are refused. Constraints accept provider_families,
exclude_provider_families and tiers, all lists. Provider constraints require
catalog family metadata; missing metadata cannot satisfy an exclusion.
Cost tiers correspond to review metadata economy, executor and critical.
A candidate's cost tier never grants review authority.

`task run --role execute --no-notify -- "prompt"` ranks eligible candidates
advertised by ready workers of the project using `route-ranking/v1`. `--worker` narrows that
set. `--policy-file PATH` chooses an explicit policy file. `--dry-run --json`
shows the selection without submitting. Roles do not expand a route into multiple
routes; specialty roles remain one candidate. Explicit prompt fan-out remains
the caller's request and uses the same single selected route.

An explicit `--model` overrides role candidate order and remains pinned; role
constraints still apply. Without explicit effort, the model receives its
unambiguous policy effort, across roles. Conflicting effort definitions require
`--effort` or a corrected policy. Explicit effort always wins. A missing default
policy preserves existing explicit callers; `--role` or a missing explicit
file refuses. Installed malformed policies refuse before submission, including for explicit callers.
Without a role, explicit models and defaults.model keep the existing resolution
path: fully qualified routes with a named project do not require a successful
projects query or configured authorization response. The optional projects query
still supplies the quota pool when available. Policy effort and frozen provenance
are added without requiring worker readiness.
With a role and explicit model, project route metadata resolves the pin and checks
role constraints without requiring readiness or project advertisement. Missing
family/tier metadata cannot satisfy the corresponding constraints. No pin widens
to a fallback. Automatic role choice additionally requires configured catalog
validity and a ready worker advertising project availability; Configured alone
does not mean the worker can prepare the project. No readiness is inferred from
validity.

For standalone `review --role R`, the role chooses exactly one independent
reviewer if no `--independent`, `--reviewer` or `--model` is supplied.
Those three aliases name explicit independent routes and override automatic
choice; each must satisfy role constraints. Judge and swarm models remain
explicit and inherit only their unambiguous policy effort. The role never
fills a judge or expands a swarm. Candidate selection checks the complete
independent/judge provider-diversity and tier requirements. A single candidate
unable to satisfy a two-family round refuses. The refusal offers a complete
explicit provider-diverse example when the catalog has suitable routes:
`t3-steward review --independent claudeAgent/claude-opus-5-5 --independent codex/gpt-6-astra --project P --diff-file DIFF --no-notify`.
Use catalog-authorized executor/critical routes from distinct families and omit
`--role` for this explicit round. Alternatively specify a provider-diverse judge
of executor tier together with an economy swarm where supported. Roles select
one ranked eligible candidate, including specialty and single-candidate roles;
they never automatically fan out reviewers or select judges. Every reviewer remains a new isolated task;
independent reviewers have no dependencies on other reviewers. Roles do not
authorize in-task review or self-use of the caller's session. Existing signed
verdict validation and gate authority remain unchanged.

## Ranking

Automatic task and review role selection uses `route-ranking/v1`. Eligibility
still checks project readiness, catalog tiers, role constraints and review
diversity. Explicit model/reviewer pins use policy order and never rank.
The quota view merges snapshots from all coordinator-reported workers, then reads
each serving pool with the configured `quota_stale_after`. The best pool among
the candidate's ready, advertising project workers determines its band.

Bands, in order, are reset-soon, healthy, unknown, saturated, gated. A pool at
its concurrency limit, counting active and planned assignments, is saturated
unless its quota already makes it gated: saturation only ever makes a band
worse. A caller that supplies no concurrency evidence leaves the band to quota
alone, so receipts written before saturation existed mean what they meant.
A fresh pool is gated
when any window is exhausted, the short window (Claude five_hour or Codex
primary) is at least 90% used, or admission is draining/closed. A fresh,
ungated long window (Claude seven_day or Codex secondary) resetting within
24 hours, including exactly 24 hours, with at most 95% used is reset-soon.
Codex with only primary can be healthy but cannot receive reset-soon preference.
Missing, stale or unknown telemetry fails closed for preference: unknown never
outranks known healthy headroom. An unsupported or failed quota query makes all
candidates unknown and falls back to policy order with an explicit reason.
Ranking itself never refuses admission; even a gated last remaining candidate
stays eligible for the coordinator's admission decision.

The lexicographic keys are band, policy ordinal, smaller maximum used percentage
across both windows, higher optional worker score, then ascending route string.
The CLI supplies no worker score; absent scores contribute zero. Two reset-soon candidates therefore retain
policy order; an earlier reset alone never wins. V1 uses raw percentages, not
spendable budget after forecasts and reservations, burn-rate projections or
budget pacing. These constants and key order are versioned; changes require v2.
Planner adoption for explicit campaign routes remains M17-2b.

Text, JSON and task dry-run receipts show the ranking version, chosen route and
candidate eligibility, band, pool and reason. The pinned `route-selection.json`
keeps stable provenance plus the ranking version and chosen route; live reasons
and candidate telemetry are excluded. Changed readings selecting the same route
therefore retain the same run key. Changing the ranking version changes run keys
once. Unknown quota falls back to policy order while still recording v1 as the
ranking used.

Provenance is `route-selection/v1`: role, actual route, raw-policy SHA-256 digest,
reason and effective effort. The reason distinguishes an explicit model override
from a configured default model and from the ranked policy candidate. Task receipts/dry-run add optional `selection`;
review-submit/v1 adds optional `selections`. Existing fields and versions
remain compatible. Exact policy bytes and selection records are also retained
as bounded pinned inputs named `route-policy.yaml` and `route-selection.json`.
User inputs using those basenames are refused as collisions. The input digest
binds these bytes to the submitted archive, run and review verdicts; later
policy edits cannot rewrite them. Effective effort is in route.options and
survives workflow/result queries. Replay uses the existing archive-content
conflict check: a reused explicit key with changed policy bytes is refused.
Result contracts retain the input digest and workflow routes; provenance can
be inspected in retained pinned evidence rather than recomputed from disk.

No wire protocol extension is required: existing workers/projects queries,
v2 pinned input archives and route options carry the information. Old clients
continue to use explicit routes. New explicit callers keep the old mixed-coordinator
path even with an installed policy. Explicit reviews retain their existing
projects/review-metadata requirements for tier and provider diversity, without
adding readiness or configured worker-authorization requirements.
Automatic policy choices need configured worker authorizations and ready project
advertisement responses; incomplete older catalogs refuse before submission,
with an upgrade/configuration remedy.

`models` text groups routes by quota pool and displays each governing bucket's
used percentage, headroom, window, reset and freshness from coordinator-merged
observations. Weekly percentages stay paired with their own weekly resets.
Unknown selection/observations print unknown. JSON schemaVersion 1 preserves
the existing aggregate percent/phase/earliest-reset fields and adds optional
per-instance `windows`; aggregates must not be treated as a single quota window.
Consumers comparing windows should use windows[]. The dispatcher/admission,
failover, fleet policy installation and generated instructions
remain later milestones.

## Campaigns and schedules

```yaml
role: execute
options: {effort: low}
tasks:
  implement:
    prompt_file: implement.md
    outputs: [unit.bundle]
  review:
    role: review
    prompt_file: review.md
    needs: [implement]
    inputs_from: {implement: [unit.bundle]}
```

Roles for campaigns and schedules
  role: execute
  options: {effort: low}

A workflow role is inherited by tasks that declare neither role nor routes.
A task's own role replaces workflow routes and a task's own routes replaces
the workflow role. role and routes are mutually exclusive on the same object.
options without role is refused, even when the task inherits a workflow role.
Role options accept only effort: low, medium or high; max and higher are refused.
An override may lower the selected candidate's policy effort, never raise it.
Unknown option keys and malformed role names are refused offline; an unknown
coordinator role or unavailable policy is refused before submission. Manifest
review: round members must use explicit routes.

The coordinator reads its route policy at resolution and chooses one eligible
concrete route per role task. validate and plan remain offline; a local policy
is advisory, while check reports the coordinator's selection and provenance.
Submission freezes that selection, and rerun reuses it. A registered schedule
resolves roles afresh for each occurrence; replay keeps the original selection.
Manifests without role keep their existing explicit-route behavior.

Review-type role tasks (review or critical-review, or tasks with review_output:)
prefer the first usable candidate in the winning quota rank band outside every known local producer family.
Producers come from inputs_from, or local needs when inputs_from is empty.
If none qualifies, the highest ranked eligible candidate is used with a diversity
fallback reason. Unknown producer families do not apply the preference.
Explicit routes always stay as written. This preference is also applied to
schedule occurrences and never grants eligibility or review authority.

Readiness counts ready eligible workers first. If none is ready, an enrolled
project worker advertising a policy route permits acceptance with waiting for
capacity. If no worker advertises any candidate, the coordinator refuses with
candidate reasons. Worker placement and resources constrain eligibility.

Selections retain the role, route, effective effort, raw-policy digest, candidate
verdicts, provider-diversity outcome and resolution time. They appear in check
and show, including JSON. Each schedule run retains its own selection. An
unresolved occurrence is suppressed as `role-unresolved` and tries again at the
next occurrence. Older coordinators must be upgraded to support `role:`;
workers still receive ordinary concrete routes.

Campaign and schedule roles use `route-ranking/v1` after policy, catalog and
worker eligibility checks, from the coordinator's request-local quota snapshot.
A healthy authorized candidate wins over an exhausted, gated or unknown pool;
equal bands retain policy order before the soft review diversity preference.
Selections retain the ranking version and candidate bands, pools and reasons.
Missing, stale or malformed quota cannot supply healthy headroom. Existing
class admission gates apply before ranking: surplus tasks cannot prefer a
constrained or recovering pool over a usable alternative. Explicitly
disabled quota checks retain their operator-defined behavior. Ranking is a
preference: submission and schedule admission still enforce quota atomically,
including changes after resolution. If every candidate is gated or unknown,
the ranked receipt explains the preference and admission can refuse or suppress
the occurrence; diversity cannot promote an unusable pool. Explicit pins stay
literal, including quota refusal when their pool is closed. New schedule
occurrences read new quota; persisted selections and templates remain immutable.

A selection is frozen at submission, but the pool it chose can fill up before
the task starts (feedback 140: every role task resolved to an idle-quota pool
and queued at its concurrency cap while another pool sat empty). So at planning
time, a role task whose first attempt has never been assigned or started, and
whose selected route's pool is saturated, is moved to the best other candidate
of its own receipt that is eligible, not gated, has a recorded effort (or a task
effort override) and whose pool has room and is not closed or draining. The
receipt itself is not rewritten: the assignment's placement records the move as
`routeReresolution` with role, from and to route and pool, effort, ranking and
reason, and `campaign explain` prints it. Retries, started attempts, tasks with
a review-independence constraint (producer families or a cross-provider
result) and explicit pins are never moved. A `task run --role` route is resolved
by the client and submitted as a pin, so it is not moved either.
