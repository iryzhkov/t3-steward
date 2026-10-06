# Handoff: native Claude rate-limit events

Status: complete. Tests were written and observed failing before implementation; the final required race command and full make test gate passed. Local branch: fix/claude-native-rate-limit-events. No push, publication or deployment performed.

## Commits

- Base: 5d5d853e37a3bacee853e68696c8996ba5edf7ee (.t3/base-commit, matching initial HEAD).
- Regression tests first: d06d6f60a250732309db8b7f61e0abdb7b2ed72b.
- Implementation: e5219a50ea76bf7de26deaab238896f513cdb53f.
- The subsequent evidence commit contains finalized continuation.md, handoff.md and complete test.log; its SHA is repository HEAD.

## Files changed

- internal/source/providerlog/parse.go: explicit native Claude method/provider allowlist, createdAt/id metadata, existing normalizeClaude window parsing, shared SDK UUID identity for legacy and normalized CANON copies.
- internal/source/providerlog/tailer.go: malformed live/bootstrap quota records logged at Debug.
- internal/source/providerlog/native_test.go: native and CANON golden readings, unrelated native method/provider rejection, malformed input, legacy/normalized SDK identity, live/bootstrap delivery.
- internal/source/providerlog/testdata/canon-warning-sample.log and native-allowed-sample.log: byte-exact golden inputs, including both native lines.
- internal/daemon/resume_policy.go: stale draining exclusion with configured quota_stale_after and one-hour default; stopped behavior unchanged.
- internal/daemon/daemon.go: skip stale drain notifications and log once per bucket/observation at Info, with bucket, age and threshold; bookkeeping belongs to each daemon and uses its mutex.
- internal/daemon/native_quota_test.go: daemon persistent dedup, 97% draining to native 1% recovery, stale/fresh/boundary draining vs stale stopped, once-only logging.
- docs/t3-protocol.md and CHANGELOG.md: source and stale-draining behavior.
- continuation.md, handoff.md, test.log: execution and verification evidence.

## Approach and alternatives

Reuse normalizeClaude and existing persistent MarkEventSeen(event identity + window); preserve native event.id and align CANON identity to the SDK UUID, including raw.payload.uuid for normalized CANON windows. No extra dedup cache or database migration is needed. GoverningPause stays pure; observability lives in the daemon instance that owns bucket state. Only draining observations older than the threshold are disregarded; equality remains fresh.

Rejected: accepting every native method (would interpret unrelated provider traffic as quota), a second Claude normalizer (would duplicate window semantics), process-global logging state (would couple independent runtimes), resetting bucket state based on the clock, and changing stopped recovery/probe or admission percentages (outside the plan).

The supplied warning fixture reports 96%, not the plan prose's 97%; golden verification preserves 96%. The separate recovery test starts with the requested 97% draining state. Cited parser, resume policy, admission and probe source matched the base (git diff was empty).

## Commands and results

- git switch -c fix/claude-native-rate-limit-events: exit 0.
- git diff 5d5d853e37a3bacee853e68696c8996ba5edf7ee -- internal/source/providerlog internal/daemon/resume_policy.go internal/workerruntime/quota_guard.go internal/backlog/quota_admission.go: exit 0, no changes before implementation.
- go test ./internal/source/providerlog ./internal/daemon -run 'TestClaude(Native|Canon)|TestGoverningPauseStaleDraining' -count=1: RED, exit 1 before production edits. Native parsing/identity failed with "not a rate-limit event"; live tailing emitted zero snapshots; stale drain cases returned pause=true; daemon CANON/native dedup accepted a duplicate.
- go test ./internal/daemon -run TestStaleDrainingLoggedOnce -count=1: RED, exit 1 before production edits, no log emitted.
- go test ./internal/source/providerlog ./internal/daemon -run 'TestClaude(Native|Canon)|TestGoverningPauseStaleDraining|TestStaleDrainingLoggedOnce' -count=1: initial implementation exited 1 because rewrapping native data serialized limits:null, making the existing parser reject an ambiguous legacy/normalized payload. Replaced rewrapping with a direct normalizeClaude call; rerun GREEN, exit 0.
- go test ./internal/source/providerlog -run TestClaudeNativeNormalizedCanonIdentity -count=1: RED exit 1 (T3 ID retained); then GREEN exit 0 after normalized identity fix.
- go test -race ./internal/source/... ./internal/daemon/... ./internal/policy/... ./internal/workerruntime/...: exit 0. Repeated after final normalized identity change: exit 0 for all four packages.
- Foreground command (actual newlines between statements):
  ```sh
  umask 022
  set -o pipefail
  make test 2>&1 | tee test.log
  ```
  Ran exactly once; exit 0. All go test -timeout 10m ./..., go test -race -timeout 25m ./..., and go vet ./... passed. Complete combined output is saved in test.log. No unrelated failures appeared, so a base failure reproduction was unnecessary.
- git diff --check: exit 0.
- huyang trust "$PWD": exit 127 (CLI not on PATH). Huyang edits still provided gofmt; requested builds/tests were run directly in the foreground with the exact user commands. No claim of Huyang verification or LSP diagnostics is made.

Fixture copy receipts verified SHA256:
- canon-warning-sample.log: 852845cd77419a10f2a03e705bc344bf63a68d9b56376a623bc602cbe657cfe8
- native-allowed-sample.log: 1dc6181076d4e57b26ea38c623fa689422495eafc164ea23e964899736b690b4

## Open risks

- Native SDK IDs are expected to equal SDK UUIDs, as both sample events do. A CANON event without any source UUID keeps its T3 event ID.
- A daemon restart may log the same stale observation once again; logging suppression is instance-scoped, not process-global or persisted.
- Existing stopped buckets still require the existing recovery/probe path; no admission/display, policy threshold, packaging/systemd or concurrent campaign files were changed.
- No release CI or fleet verification is claimed; this task is explicitly local-only.
