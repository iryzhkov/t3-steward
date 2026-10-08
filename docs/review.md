# Standalone review rounds

Review a plan from a plain CLI session:

```sh
t3-steward review --plan /tmp/plan.md --project scratch \
  --reviewer claudeAgent/claude-opus-5-5 --independent codex/gpt-6.1-sol \
  --swarm security,errors,tests \
  --swarm-model claudeAgent/claude-sonnet-5-5 --swarm-model codex/gpt-6-luna \
  --judge codex/gpt-6.1-sol --deadline 4h --wait
```

Review committed code from a checkout whose origin matches the catalog repository:

```sh
t3-steward review --project t3-steward-github --diff origin/main..HEAD \
  --criteria /tmp/criteria.md --reviewer claudeAgent/claude-opus-5-5 \
  --independent codex/gpt-6.1-sol --wait --gate --json
```

Repeat `--plan`, `--reviewer` (alias `--model`) or `--swarm-model`.
`--independent` adds an independent reviewer. `--swarm default` (also bare
`--swarm`) selects security, errors and tests for routine risk; risky risk adds
concurrency, rollout, resources and docs. Lenses alternate provider families
when the supplied economy routes offer more than one family. Swarm requires a
judge, and a judge requires a swarm. At least one independent reviewer is
required. CLI Phase A role selection is described in [route-policy.md](route-policy.md).
Quota-aware selection remains deferred. Inside a task, `review --task current`
opens the review declared in its manifest; see [m16-review-checkpoint.md](m16-review-checkpoint.md).

The coordinator catalog must classify routes explicitly; instance aliases are
not evidence of different providers. Configure the following on the coordinator:

```yaml
backlog_v2:
  review_routes:
    claudeAgent/claude-opus-5-5: {provider_family: claude, tier: executor}
    claudeAgent/claude-sonnet-5-5: {provider_family: claude, tier: economy}
    codex/gpt-6.1-sol: {provider_family: openai, tier: executor}
    codex/gpt-6-luna: {provider_family: openai, tier: economy}
```

Route keys split at the first `/`: the instance comes first and the remaining
text is the model ID. Model IDs may contain more slashes or colons, such as
`opencode/deepseek/deepseek-flash` or `opencode/ollama/qwen3-coder:30b`.

These are metadata, not route authorization or role policy. The existing worker
catalog still authorizes routes. The projects query attaches metadata to its
advertised routes. Classify every advertised route's provider family so diversity
can be verified; selected routes also require a tier. Allowed tiers are economy,
executor and critical. Independent reviewers require executor or critical;
the judge requires executor; swarm lenses require economy. The coordinator
checks the manifest against its own catalog before accepting it. When the
project offers two provider families, independent reviewers plus judge must
include at least two. Missing metadata is a refusal with a configuration remedy.

Every reviewer runs as a new campaign task with a new session identity. The
caller is never reused as a reviewer; sharing its provider is allowed.
Independent tasks receive original inputs only. The judge depends only on swarm
tasks and receives their verdict.json files under
`.t3/dependencies/<swarm-task>/verdict.json`. It checks findings against code,
deduplicates them and explains discarded false positives in review.md.
Only independent reviewers and the judge determine the combined verdict.
Every required reviewer must produce valid evidence for collection to succeed.
All review manifests require environment.scope: task. The judge becomes ready
once every swarm task is terminal, including failed or cancelled lenses. It reads
the available verdicts and the coordinator list of missing lenses from
`.t3/context/index.json`. Optional failures remain visible but do not decide the
combined verdict or fail collection.

Local files use the shared pinned-input ingestion: bounded, snapshotted, hashed,
read-only files under `.t3/inputs/`. Inputs are never pasted into prompts.
`--diff` resolves both refs to full commit IDs once, generates a bounded diff,
and checks out the pinned head for all reviewers. Dirty and untracked files are
excluded. `--diff-file` accepts a previously generated diff.
`--commit REF` resolves and checks out one commit, after checking its reachability
on the catalog project remote. Add `--base REF` to snapshot its bounded diff;
without it no diff is generated. An unpushed candidate is refused with
"push the commit or use --bundle".
`--bundle FILE` verifies and snapshots a single-head Git bundle whose head is a
commit object (annotated tag objects are refused), including bundles
from `campaign commit export`. Its prerequisites must resolve in the current
checkout and be reachable on the catalog project remote, so every worker can
prepare them; unpublished prerequisites are refused with a push or self-contained
bundle remedy. Ancestry is checked against the project's remote default ref;
unrelated bundles are refused. Reviewers start from its prerequisite
base (or the project default ref for a self-contained bundle), then receive exact
fetch and detached-checkout commands for the pinned candidate. Bundle bytes keep
the same 1 MiB per-file limit as other inputs. These candidate modes require a
catalog git project whose repository matches the current checkout's origin and a
coordinator at 0.11.0-rc.118 or later.
`--commit`, `--bundle`, `--diff` and `--diff-file` are mutually exclusive;
`--base` requires `--commit`. Task mode refuses `--commit`, `--bundle` and
`--base`, like all other submission flags. Each input is at
most 1 MiB, the total is at most 3 MiB, and basename collisions are refused.
The composed first turn is checked against the M7b 120,000 UTF-16-unit limit.
Fixed binary instructions are versioned as review-instructions/v1, require
file:line evidence, review.md and review-verdict/v1 verdict.json, and permit
same-provider reader subagents for inputs over 1,500 changed lines or ten files.

Inside T3, the caller's current thread is notified by default using the existing
node-wake mechanism, with the short review reply. Outside T3, explicitly pass
`--wait`, `--no-notify` or `--notify-thread ID`. Notification registration
failure is reported after submission with the round ID so it can be collected.
`--wait` uses `review result --wait` with the round deadline plus a two-minute
collection margin; interrupting the client does not cancel
the round. Reattach with:

```sh
t3-steward review result ROUND --wait --json
```

The default round deadline is four hours; the maximum is seven days. Each task
carries the deadline and collection marks unfinished reviewers timed-out.
A late completed review does not become acceptance. A client wait timeout is
separate from the durable reviewer states. Failed, missing, malformed or oversized
evidence is never acceptance. Collection runs on coordinator ticks, survives
restarts, and uses retained artifacts from each reviewer's latest attempt.
Invalid swarm evidence remains visible as an optional lens failure even if the
judge accepts. Scheduled registration refuses review rounds explicitly.

Results live under the steward state directory, outside checkouts:
`results/reviews/ROUND/<reviewer>/review.md`, `verdict.json`, and
`results/reviews/ROUND/summary.json`. The short reply contains routes, verdicts,
counts, blocking titles and paths. Very long wake replies are bounded; the full
finding list remains in summary.json. Merge and severity sorting are mechanical.

Exit codes:

| Code | Meaning |
| --- | --- |
| 0 | Every required review was collected and valid, including reject or accept-with-changes. Async submission also exits 0 when accepted. |
| 1 | Pending result, client wait timeout, argument or submission failure. |
| 2 | A required reviewer failed, timed out or produced invalid evidence. |
| 3 | `--gate` and the combined verdict is not accept. Collection failure takes precedence. |
| 130 | Interrupted blocking wait. |

Transport failures retain the existing transport codes. Submission `--gate`
requires `--wait`. `review result --gate` checks an already retained round.

Mixed versions: ordinary task callers need no new metadata. Old coordinators
are refused before catalog lookup or submission with "coordinator does not support
review rounds (needs 0.11.0-rc.104 or later)". Unknown coordinator releases also
refuse; review has no fallback that weakens the round. Upgrade the coordinator
and configure review_routes before use. For slash-containing model IDs, upgrade
the coordinator to a build with the first-slash validation fix before adding
those route keys to its configuration: older binaries reject the configuration
at startup. Upgrade review clients too, including clients collecting results;
older clients reject these routes in campaigns and verdicts even though the
rc.104 capability check passes. Workers use existing campaign tasks,
pinned inputs and dependency artifacts. Judge packages use the existing
project-context-v1 capability to carry missing-lens observations, with no new
worker protocol fields. A worker without that capability is refused by the
existing package capability gate. A receiving
host's existing node-wake runner prints the short reply carried in the observation.
No new execution or approval authority is granted.
