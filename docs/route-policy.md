# CLI route policy (Phase A)

The steward reads `$XDG_CONFIG_HOME/t3-steward/route-policy.yaml` (normally
`~/.config/t3-steward/route-policy.yaml`), beside `coordinator-client.json`.
It never installs or replaces it. Use `policy show --file PATH` to inspect a
candidate file and `policy validate --file PATH` to check it against the
coordinator's configured worker model authorizations. Both accept `--json`.
Validation is read-only and does not depend on workers being online. A coordinator
that cannot report configured model authorizations refuses validation; inventory
alone cannot establish validity.

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

`task run --role execute --no-notify -- "prompt"` chooses the first candidate
advertised by a ready eligible worker of the project. `--worker` narrows that
set. `--policy-file PATH` chooses an explicit policy file. `--dry-run --json`
shows the selection without submitting. Roles do not expand a route into multiple
routes; specialty roles remain one candidate. Explicit prompt fan-out remains
the caller's request and uses the same single selected route.

An explicit `--model` overrides role candidate order and remains pinned; role
constraints still apply. Without explicit effort, the model receives its
unambiguous policy effort, across roles. Conflicting effort definitions require
`--effort` or a corrected policy. Explicit effort always wins. A missing default
policy preserves existing explicit callers; `--role` or a missing explicit
file refuses. Installed invalid policies and unavailable catalogs refuse before
submission. No readiness is inferred from validity.

For standalone `review --role R`, the role chooses exactly one independent
reviewer if no `--independent`, `--reviewer` or `--model` is supplied.
Those three aliases name explicit independent routes and override automatic
choice; each must satisfy role constraints. Judge and swarm models remain
explicit and inherit only their unambiguous policy effort. The role never
fills a judge or expands a swarm. Candidate selection checks the complete
independent/judge provider-diversity and tier requirements. A single candidate
unable to satisfy a two-family round refuses; pass explicit independent routes
or an explicit judge plus swarm. Every reviewer remains a new isolated task;
independent reviewers have no dependencies on other reviewers. Roles do not
authorize in-task review or self-use of the caller's session. Existing signed
verdict validation and gate authority remain unchanged.

Provenance is `route-selection/v1`: role, actual route, raw-policy SHA-256 digest,
reason and effective effort. Task receipts/dry-run add optional `selection`;
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
continue to use explicit routes. New explicit callers without a default policy
keep the old mixed-coordinator path. Policy callers need configured worker
authorizations and project readiness responses; incomplete older catalogs
refuse before submission, with an upgrade/configuration remedy.

`models` text groups routes by quota pool and displays each governing bucket's
used percentage, headroom, window, reset and freshness from coordinator-merged
observations. Weekly percentages stay paired with their own weekly resets.
Unknown selection/observations print unknown. JSON schemaVersion 1 preserves
the existing aggregate percent/phase/earliest-reset fields and adds optional
per-instance `windows`; aggregates must not be treated as a single quota window.
Consumers comparing windows should use windows[]. The dispatcher/admission,
quota ranking, failover, fleet policy installation and generated instructions
are unchanged and remain later milestones.
