# Review evidence and round foundation

The foundation supports `task run --input FILE` (repeatable) and
`review result ROUND [--json] [--wait [--timeout D]]`. It does not submit
review rounds. The later review caller uses the same
`internal/pinnedinput.SnapshotFiles` and `Snapshot.Write` implementation.

## Pinned inputs

Each local file is read once at submission composition. Its basename becomes
its campaign `inputs:` name and its workspace path
`.t3/inputs/<basename>`, in Git and fresh environments. Basename collisions
are refused rather than silently renamed. Absolute source paths are allowed;
absolute workspace paths, any `..` component, glob metacharacters and
symlinks in any source component are refused. Only regular files are accepted.

Limits are **1 MiB per file, 3 MiB in total, 100 files**. The campaign's existing
archive limits also apply (normally 4 MiB including prompts and manifest).
Workspace preparation checks retained size and SHA-256 before publishing files
with read-only modes. This is filesystem immutability for cooperative host
execution, not a container security boundary: the task runs as the same user.
No dirty or untracked file is included implicitly.

The submission receipt and durable workflow definition record the input
manifest: `entries` containing `name`, `size` in bytes, and lowercase
`sha256`, plus `digest`. The digest is SHA-256 of the compact JSON array
of entries sorted by name, with fields in `name,size,sha256` order, UTF-8
encoding and Go encoding/json string escaping. Empty inputs use `[]`.
Source locations do not enter the digest. Changed bytes change the default
task-run idempotency key; explicit keys still refuse different content.
Ordinary campaign input paths keep their relative names and are recorded by
the same manifest builder on coordinator ingestion.

## review-verdict/v1

Each reviewer produces `review.md` and `verdict.json`. The latter contains
one JSON object, at most 1 MiB, with no duplicate or unknown keys. Markdown is
also limited to 1 MiB. All fields below are required.

```json
{
  "schema": "review-verdict/v1",
  "verdict": "accept-with-changes",
  "inputManifestDigest": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
  "reviewerRoute": "codex/gpt-6.1-sol",
  "findings": [{
    "id": "F1",
    "severity": "low",
    "blocking": false,
    "title": "Explain the flag",
    "evidence": ["cmd/t3-steward/main.go:42"],
    "recommendation": "Add the missing help sentence."
  }]
}
```

The JSON Schema (draft 2020-12) is:

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "review-verdict/v1",
  "type": "object",
  "additionalProperties": false,
  "required": ["schema", "verdict", "inputManifestDigest", "reviewerRoute", "findings"],
  "properties": {
    "schema": {"const": "review-verdict/v1"},
    "verdict": {"enum": ["accept", "accept-with-changes", "reject"]},
    "inputManifestDigest": {"type": "string", "pattern": "^[0-9a-f]{64}$"},
    "reviewerRoute": {"type": "string", "pattern": "^[^/\\s]+/[^/\\s]+$"},
    "findings": {
      "type": "array",
      "items": {
        "type": "object",
        "additionalProperties": false,
        "required": ["id", "severity", "blocking", "title", "evidence", "recommendation"],
        "properties": {
          "id": {"type": "string", "pattern": "^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$"},
          "severity": {"enum": ["high", "medium", "low"]},
          "blocking": {"type": "boolean"},
          "title": {"type": "string", "minLength": 1},
          "evidence": {
            "type": "array",
            "minItems": 1,
            "items": {
              "type": "string",
              "anyOf": [
                {"pattern": "^artifact:[A-Za-z0-9][A-Za-z0-9._-]{0,127}$"},
                {"pattern": "^.+:[1-9][0-9]*$"}
              ]
            }
          },
          "recommendation": {"type": "string", "minLength": 1}
        }
      }
    }
  },
  "allOf": [
    {
      "if": {"properties": {"verdict": {"const": "accept"}}},
      "then": {"properties": {"findings": {"maxItems": 0}}}
    },
    {
      "if": {"properties": {"verdict": {"const": "accept-with-changes"}}},
      "then": {"properties": {"findings": {
        "minItems": 1,
        "items": {"properties": {"blocking": {"const": false}}}
      }}}
    },
    {
      "if": {"properties": {"verdict": {"const": "reject"}}},
      "then": {"properties": {"findings": {
        "contains": {"properties": {"blocking": {"const": true}}},
        "minContains": 1
      }}}
    }
  ]
}
```

The Go validator additionally requires unique finding IDs within a reviewer,
non-whitespace titles and recommendations, normalized relative file paths
without parent traversal in file:line citations, and exact agreement with the
round's manifest digest and reviewer route. Artifact citations use
`artifact:<artifact-id>`; file citations use `relative/path:positive-line`.
Evidence is a required citation, not a claim that the validator has proved the
finding. IDs are reviewer-assigned and should remain stable across rounds;
the coordinator does not rename or deduplicate them.

Contradictions fail validation: accept has no findings; accept-with-changes
has at least one finding and none blocking; reject has a blocking finding.
An accept-with-changes document with blocking evidence becomes an invalid
reviewer result, which mechanically rejects a required reviewer's round.
Missing/empty review.md also makes an otherwise successful result invalid.

## Durable rounds

Coordinator-owned `CreateReviewRound` freezes the round ID, optional workflow
run and full base/head commit IDs, input digest, reviewer IDs, roles, required
flags and explicit INSTANCE/MODEL routes. No role policy, access profile or
authority kind is added. The SQLite migration adds one record table.
`RecordReviewResult` validates terminal results inside a revision-fenced
transaction. It refuses stale updates, unknown reviewers and replacement of a
terminal review. `GetReviewRound` survives coordinator restarts.

A round is limited to 32 reviewers; roles are at most 64 bytes, routes 256
bytes, and run/task IDs 128 bytes. Failure summaries are capped at 4096 bytes.
Round read responses omit bulk documents and findings; the CLI fetches each
1 MiB document with a separate authenticated read query, base64-encoded on
the wire to bound JSON expansion, and revalidates each successful verdict.

States are pending, succeeded, failed, timed-out and invalid. Combined verdict:
any required failed/timed-out/invalid reviewer or blocking validated verdict
means reject; otherwise unfinished required reviews mean pending; otherwise
any required accept-with-changes means accept-with-changes; otherwise accept.
At least one required reviewer is mandatory. Optional lens results do not
gate the round; part B must mark its judge and independent reviewer required.
Roles are labels for the caller to record; this foundation does not select
models or enforce provider diversity.

All reviewers must be terminal before collection is complete. The existing
admin read authorization governs the `review-round` query; there is no new
approval capability. Submission and task completion orchestration will call
the record APIs in part B; this foundation does not create rounds from tasks,
infer reviewers from outputs, or add the in-task executor loop.

## Reading and exporting

`review result` queries the durable record and writes under
`<state>/results/reviews/<round>/`: reviewer directories containing any
produced review.md/verdict.json, and summary.json with every validated finding,
reviewer, route, severity, blocking flag and evidence. Findings are sorted high,
medium, low, then reviewer and finding ID. Invalid verdict bytes are retained
for diagnosis but never included as validated findings. Missing outputs remain
missing; failed reviewer state and reason explain them.

The reply contains combined verdict, per-reviewer state/verdict/route, blocking
and non-blocking counts, blocking titles and paths, never full review text.
JSON replies use `review-result/v1`. There is no output option that targets the
checkout. Existing output links and unsafe round/reviewer IDs are refused. A configured
state/results path inside a Git checkout (including through a symlink) is
refused before any result directory is created.

Exit 0 means collection succeeded even when reviewers reject the change;
exit 2 means a reviewer failed, timed out or returned invalid evidence;
exit 1 means pending or local wait timeout; SIGINT returns 130.
Transport exit codes are unchanged. Waiting uses `internal/blockingwait`,
including its retry/backoff and reattachment rules; it never cancels a round.

## Mixed-version behavior

New task clients send ordinary version-2 campaigns with `inputs:`, already
understood by existing coordinators and workers. Their receipts show the
manifest; older coordinators retain input artifacts but omit the new explicit
workflow manifest projection. Existing workspace input mounting needs no
protocol change. New coordinators impose the documented input limits, including
on ordinary campaigns. Old clients ignore the extra workflow JSON field.

Review result requires a coordinator supporting the new read query and round
table. An old coordinator refuses the unknown query; it cannot supply an
acceptance. The additive database migration is coordinator-owned. Do not
downgrade a coordinator against a migrated database without the repository's
normal database rollback procedure. No source PR publishes a release or
converges a fleet host.
